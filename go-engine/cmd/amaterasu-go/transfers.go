package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"golang.org/x/sync/errgroup"
)

type transferEvents struct {
	writer http.ResponseWriter
	mu     sync.Mutex
	last   time.Time
	done   bool
}

func events(w http.ResponseWriter) *transferEvents {
	w.Header().Set("Content-Type", "text/event-stream")
	return &transferEvents{writer: w}
}

func (e *transferEvents) emit(data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(e.writer, "data: %s\n\n", payload); err != nil {
		return err
	}
	return http.NewResponseController(e.writer).Flush()
}

func (e *transferEvents) progress(current, total int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done {
		return context.Canceled
	}
	if time.Since(e.last) < 250*time.Millisecond {
		return nil
	}
	e.last = time.Now()
	return e.emit(map[string]any{"status": "running", "processed_bytes": current, "total_bytes": total})
}

func (e *transferEvents) Chunk(_ context.Context, state uploader.ProgressState) error {
	return e.progress(state.Uploaded, state.Total)
}

func (e *transferEvents) finish(result any, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.done = true
	if err != nil {
		var wait *floodError
		if errors.As(err, &wait) {
			_ = e.emit(map[string]any{"status": "cooldown", "retry_after": wait.seconds(), "source": wait.source})
		} else {
			_ = e.emit(map[string]string{"status": "failed"})
		}
		return
	}
	_ = e.emit(map[string]any{"status": "complete", "result": result})
}

func (e *engine) download(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path    string
		Sources []mediaRange
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256*1024)).Decode(&req); err != nil ||
		!filepath.IsAbs(req.Path) || filepath.Base(req.Path) != "payload" ||
		len(req.Sources) == 0 || len(req.Sources) > 32 {
		http.Error(w, "invalid download", http.StatusBadRequest)
		return
	}
	first := req.Sources[0]
	for _, source := range req.Sources {
		if !source.valid() || source.ID != first.ID || source.Kind != first.Kind || source.Size != first.Size ||
			source.Start != 0 || source.End != first.Size-1 {
			http.Error(w, "invalid download source", http.StatusBadRequest)
			return
		}
	}
	updates := events(w)
	err := e.downloadFile(r.Context(), req.Path, req.Sources, updates)
	updates.finish(first.Size, err)
}

func (e *engine) downloadFile(ctx context.Context, path string, sources []mediaRange, updates *transferEvents) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	var next, written atomic.Int64
	size := sources[0].Size
	group, ctx := errgroup.WithContext(ctx)
	for index, source := range sources {
		group.Go(func() error {
			setup, stop := context.WithTimeout(ctx, 12*time.Second)
			defer stop()
			w, err := e.getWorker(setup, source.credentials)
			if err != nil {
				return err
			}
			defer w.release()
			if wait := w.cooldown(); wait != nil {
				wait.source = index
				return wait
			}
			rpc, err := w.mediaClient(setup, source.DC)
			if err != nil {
				return sourceError(err, index)
			}
			location := source.location()
			for offset := next.Add(chunkSize) - chunkSize; offset < size; offset = next.Add(chunkSize) - chunkSize {
				bytes, err := w.fetch(ctx, rpc, location, offset)
				if err != nil {
					return sourceError(err, index)
				}
				length := min(chunkSize, size-offset)
				if int64(len(bytes)) < length {
					return io.ErrUnexpectedEOF
				}
				if _, err := file.WriteAt(bytes[:length], offset); err != nil {
					return err
				}
				if err := updates.progress(written.Add(length), size); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	if written.Load() != size {
		return io.ErrUnexpectedEOF
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

func sourceError(err error, index int) error {
	var wait *floodError
	if errors.As(err, &wait) {
		return &floodError{until: wait.until, source: index}
	}
	return err
}

func (e *engine) upload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		credentials
		Path string
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil ||
		!filepath.IsAbs(req.Path) || req.AppID <= 0 || req.AppHash == "" || req.Token == "" {
		http.Error(w, "invalid upload", http.StatusBadRequest)
		return
	}
	updates := events(w)
	worker, err := e.getWorker(r.Context(), req.credentials)
	if err != nil {
		updates.finish(nil, err)
		return
	}
	defer worker.release()
	info, err := os.Stat(req.Path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 10*1024*1024 || info.Size() > 2*1024*1024*1024 {
		updates.finish(nil, errors.New("unsupported upload file"))
		return
	}
	ctx := r.Context()
	select {
	case worker.slots <- struct{}{}:
		defer func() { <-worker.slots }()
	case <-ctx.Done():
		return
	}
	base := worker.client.API().Invoker()
	rpc := tg.NewClient(telegram.InvokeFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		if wait := worker.cooldown(); wait != nil {
			return wait
		}
		// Report waits to Python instead of sleeping until its progress reader times out.
		return worker.recordFlood(base.Invoke(ctx, input, output))
	}))
	file, err := uploader.NewUploader(rpc).WithPartSize(512*1024).
		WithThreads(4).WithProgress(updates).FromPath(ctx, req.Path)
	if err != nil {
		updates.finish(nil, err)
		return
	}
	big, ok := file.(*tg.InputFileBig)
	if !ok {
		updates.finish(nil, errors.New("unsupported upload result"))
		return
	}
	updates.finish(map[string]any{"id": big.ID, "parts": big.Parts, "name": big.Name}, nil)
}
