<p>
  <a href="https://github.com/thadeu/anway">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/thadeu/anway/main/brand/png/lockup-on-dark.png">
      <img src="https://raw.githubusercontent.com/thadeu/anway/main/brand/png/lockup-on-light.png" alt="Anway" width="320">
    </picture>
  </a>
</p>

# anway-js

JavaScript packages for [Anway](https://github.com/thadeu/anway).

| Package | Purpose |
|---|---|
| [`@anway/client`](packages/client) | Client and worker for Node and Bun |

## Development

```bash
pnpm install
SERVER_BIN=/path/to/anway pnpm test
SERVER_BIN=/path/to/anway pnpm test:bun
SERVER_BIN=/path/to/anway pnpm test:package
```

## License

MIT
