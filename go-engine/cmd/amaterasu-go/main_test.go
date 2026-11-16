package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
)

func TestSeekReturnsExactRangeAcrossChunks(t *testing.T) {
	source := bytes.Repeat([]byte("0123456789abcdefXYZ"), 120000)
	media := mediaRange{Start: chunkSize - 3, End: chunkSize + 16, Size: int64(len(source))}
	result := httptest.NewRecorder()
	serveRange(result, context.Background(), media, func(_ context.Context, offset int64) ([]byte, error) {
		return source[offset:min(offset+chunkSize, int64(len(source)))], nil
	})
	if result.Code != http.StatusPartialContent || result.Header().Get("Content-Length") != "20" ||
		result.Header().Get("Content-Range") != "bytes 1048573-1048592/2280000" ||
		!bytes.Equal(result.Body.Bytes(), source[media.Start:media.End+1]) {
		t.Fatalf("wrong range response: %d, %v, %q", result.Code, result.Header(), result.Body.Bytes())
	}
}

func TestShortFirstChunkProducesBadGateway(t *testing.T) {
	result := httptest.NewRecorder()
	serveRange(result, context.Background(), mediaRange{Size: 10, End: 5}, func(context.Context, int64) ([]byte, error) {
		return []byte("short"), nil
	})
	if result.Code != http.StatusBadGateway {
		t.Fatalf("expected a failure before sending media, got %d", result.Code)
	}
}

func TestRangeCancellationStopsProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	chunks := rangeChunks(ctx, 0, 10, func(ctx context.Context, _ int64) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	cancel()
	timer := time.After(time.Second)
	for {
		select {
		case _, ok := <-chunks:
			if !ok {
				return
			}
		case <-timer:
			t.Fatal("range producer survived cancellation")
		}
	}
}

func TestRangePrefetchesConcurrentlyAndSendsInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := make(chan struct{})
	started := make(chan int64, 4)
	chunks := rangeChunks(ctx, chunkSize-1, 2*chunkSize, func(ctx context.Context, offset int64) ([]byte, error) {
		started <- offset
		if offset == 0 {
			select {
			case <-first:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return bytes.Repeat([]byte{byte(offset / chunkSize)}, chunkSize), nil
	})
	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("later chunks did not start while the first was blocked")
		}
	}
	close(first)
	var output []byte
	for chunk := range chunks {
		if chunk.err != nil {
			t.Fatal(chunk.err)
		}
		output = append(output, chunk.data...)
	}
	if len(output) != chunkSize+2 || output[0] != 0 ||
		!bytes.Equal(output[1:len(output)-1], bytes.Repeat([]byte{1}, chunkSize)) ||
		output[len(output)-1] != 2 {
		t.Fatal("prefetch emitted the wrong bytes or order")
	}
}

type testPool struct{}

func (*testPool) Close() error                                           { return nil }
func (*testPool) Invoke(context.Context, bin.Encoder, bin.Decoder) error { return nil }

func TestMediaClientReusesWorkerPool(t *testing.T) {
	pool := &testPool{}
	w := &worker{done: make(chan struct{}), pools: map[int]telegram.CloseInvoker{2: pool}}
	for i := 0; i < 2; i++ {
		client, err := w.mediaClient(context.Background(), 2)
		if err != nil || client.Invoker() != pool {
			t.Fatalf("pool reuse failed: %v", err)
		}
	}
}

func TestFirstChunkFloodWaitIsReported(t *testing.T) {
	result := httptest.NewRecorder()
	serveRange(result, context.Background(), mediaRange{Size: 10, End: 5}, func(context.Context, int64) ([]byte, error) {
		return nil, &floodError{until: time.Now().Add(60 * time.Second)}
	})
	if result.Code != http.StatusTooManyRequests || result.Header().Get("Retry-After") != "60" {
		t.Fatalf("missing flood wait response: %d %v", result.Code, result.Header())
	}
}

func TestHosterMultipartAndTargetValidation(t *testing.T) {
	input := hostUpload{
		Provider: "gofile", URL: "https://upload.gofile.io/uploadfile",
		Path: filepath.Join(t.TempDir(), "video"), Field: "file", Filename: "video.bin",
		Fields: map[string]string{"folderId": "folder"},
	}
	target, ok := input.target()
	if !ok {
		t.Fatal("valid upload host rejected")
	}
	payload := "test payload"
	request, err := input.request(context.Background(), target, strings.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := io.ReadAll(request.Body)
	if err != nil || request.ContentLength != int64(len(wire)) || request.Method != "POST" {
		t.Fatalf("invalid HTTP body framing: %v", err)
	}
	request.Body = io.NopCloser(bytes.NewReader(wire))
	reader, err := request.MultipartReader()
	if err != nil {
		t.Fatal(err)
	}
	for _, expect := range []struct{ field, value string }{{"folderId", "folder"}, {"file", payload}} {
		part, err := reader.NextPart()
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(part)
		if err != nil || part.FormName() != expect.field || string(got) != expect.value {
			t.Fatalf("wrong multipart field or value: %v", err)
		}
	}
	input.URL = "https://gofile.io.example.com"
	if _, ok := input.target(); ok {
		t.Fatal("unrelated upload host accepted")
	}
}
