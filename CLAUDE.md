# Project Instructions

JavaScript packages for the Anway job server, in a pnpm workspace. `packages/client` is `@anway/client`. Read its `README.md` first.

The contract comes from other repositories:

- Client rules and shutdown: `docs/dev/clients.md` in `thadeu/anway`.
- Scenarios: `conformance/scenarios.md` in `thadeu/anway`.
- Operations: `README.md` in `thadeu/anway-go`, which sets the contract.

## Rules

- **Contract:** `Anway.Client` has the same operations, argument order, defaults, and error kinds as anway-go, in camelCase.
- **API:** The docs use the default export: `new Anway.Client()`, `new Anway.Worker()`, `new Anway.Job()`. Named exports stay available.
- **Versions:** Support Node 24 and later, and Bun 1.2 and later. Do not add code for old versions.
- **Runtime:** Use only `node:` modules, so that Node and Bun share one code path. There are no runtime dependencies. Discuss a new one before you add it.
- **Build:** tsup makes ESM, CommonJS, and type declarations. A new package goes in `packages/`.
- **Tooling:** Use pnpm. ESLint and Prettier are local only. CI runs tests only.
- **Args:** Job args are JSON types, because other languages read them.
- **Shutdown:** A fetch in progress is not canceled, because its reply can hold a job. A promise cannot be killed, so a late result is ignored.

## Tests

```bash
SERVER_BIN=/path/to/anway pnpm test
SERVER_BIN=/path/to/anway pnpm test:bun
SERVER_BIN=/path/to/anway pnpm test:package
```

- `pnpm test` uses vitest on Node. `pnpm test:bun` uses `bun test`, because vitest does not start on Bun 1.2.
- `pnpm test:package` installs the packed tarball and checks ESM, CommonJS, types, and SIGTERM on Node and Bun.
- Specs run against a real server. Each scenario ID maps to one test.
- A new protection needs a test that fails when you remove the protection.

## Release

1. Change the version in `packages/client/package.json`.
2. Push to `main`, and wait for green CI.
3. Push a `vX.Y.Z` tag. `release.yml` tests and publishes with npm trusted publishing.

A failed test stops the publish. A published version number cannot be used again.

## Code style

- Follow `.prettierrc` and `eslint.config.mjs`.
- Use one blank line between blocks with different responsibilities: before `if`, `for`, `while`, `switch`, `try`, and `return`, and after a closing `}` when more statements follow.
- Keep lines that build the same value together.
