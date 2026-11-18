<script lang="ts">
  // The system architecture drawn as a stack of layers: the same layers the
  // old ASCII diagram listed, top to bottom.
  let { version = "" }: { version?: string } = $props();

  const layers: { name: string; cells: string[] }[] = [
    { name: "Engines", cells: ["Aria2c", "qBittorrent", "JDownloader", "Sabnzbd", "Rclone"] },
    { name: "Web", cells: ["FastAPI web interface · 8080"] },
    { name: "Tasks", cells: ["Mirror", "Leech", "Encode", "FileToLink", "RSS"] },
    { name: "State", cells: ["MongoDB", "Redis", "Pyrogram"] },
    { name: "Gateway", cells: ["Telegram Bot API"] },
  ];
</script>

<div class="stack" role="img" aria-label="Amaterasu architecture: {layers
    .map((l) => `${l.name}: ${l.cells.join(', ')}`)
    .join('; ')}">
  <div class="stack-cap">
    <span class="cap-name">Amaterasu</span>
    {#if version}<span class="cap-version">{version}</span>{/if}
  </div>
  {#each layers as layer, i (layer.name)}
    <div class="layer" style:--i={i}>
      <span class="layer-name">{layer.name}</span>
      <div class="cells" style:--n={layer.cells.length}>
        {#each layer.cells as cell (cell)}
          <span class="cell">{cell}</span>
        {/each}
      </div>
    </div>
  {/each}
</div>

<style>
  .stack {
    position: relative;
    display: grid;
    gap: var(--space-2);
  }

  .stack-cap {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    padding: var(--space-3) var(--space-4);
    border-radius: var(--radius);
    background: var(--primary-fill);
    color: var(--button-text);
    box-shadow:
      inset 0 1px 0 oklch(1 0 0 / 0.3),
      0 8px 22px -10px var(--glow-accent);
  }

  .cap-name {
    font-weight: 600;
    letter-spacing: -0.01em;
  }

  .cap-version {
    font-size: var(--text-xs);
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    opacity: 0.75;
  }

  .layer {
    display: grid;
    grid-template-columns: 4.5rem minmax(0, 1fr);
    align-items: center;
    gap: var(--space-3);
    animation: layer-in 520ms var(--ease-spring) both;
    animation-delay: calc(var(--i) * 70ms + 120ms);
  }

  .layer-name {
    font-size: var(--text-xs);
    font-weight: 600;
    color: var(--text-muted);
    text-align: right;
  }

  .cells {
    display: grid;
    grid-template-columns: repeat(var(--n), minmax(0, 1fr));
    gap: var(--space-1);
  }

  .cell {
    display: grid;
    place-items: center;
    min-height: 2.5rem;
    padding: var(--space-1) var(--space-2);
    border: var(--border-width) solid transparent;
    border-radius: var(--radius-sm);
    background:
      var(--inset-fill) padding-box,
      var(--inset-edge) border-box;
    color: var(--text);
    font-size: var(--text-xs);
    font-weight: 560;
    text-align: center;
    line-height: 1.2;
    overflow-wrap: anywhere;
  }

  @keyframes layer-in {
    from {
      opacity: 0;
      transform: translateY(6px);
    }
  }

  @media (max-width: 40rem) {
    .layer {
      grid-template-columns: minmax(0, 1fr);
      gap: var(--space-1);
    }
    .layer-name {
      text-align: left;
    }
    .cells {
      grid-template-columns: repeat(auto-fit, minmax(5.5rem, 1fr));
    }
  }
</style>
