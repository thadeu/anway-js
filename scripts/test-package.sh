#!/usr/bin/env bash
# Installs the packed tarball, as a user does, and checks ESM, CJS, and types on
# Node and Bun. It also checks that SIGTERM ends a worker process with exit 0.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

pnpm --dir "$root/packages/client" build >/dev/null
tarball="$(cd "$root/packages/client" && pnpm pack --pack-destination "$work" | tail -1)"

cd "$work"

cat > package.json <<JSON
{ "private": true, "dependencies": { "@anway/client": "file:$tarball" } }
JSON

npm install --silent --no-audit --no-fund --no-package-lock
npm install --silent --no-audit --no-fund --no-package-lock --no-save "typescript@5" "@types/node@24"

cat > esm.mjs <<'JS'
import Anway, { Client, Job } from '@anway/client';
import * as Named from '@anway/client';

const checks = [
  Anway.Client === Client,
  Anway.Job === Job,
  Named.Worker === Anway.Worker,
  new Anway.Job('x').type === 'x',
  typeof new Anway.Client().push === 'function',
];

if (checks.includes(false)) throw new Error(`esm: ${checks}`);

console.log('esm ok');
JS

cat > cjs.cjs <<'JS'
const { Client, Job, Worker, default: Anway } = require('@anway/client');

const checks = [
  typeof Client === 'function',
  typeof Worker === 'function',
  new Job('x').type === 'x',
  Anway.Client === Client,
];

if (checks.includes(false)) throw new Error(`cjs: ${checks}`);

console.log('cjs ok');
JS

node esm.mjs
node cjs.cjs
bun esm.mjs
bun cjs.cjs

cp "$root/packages/client/tests/types/usage.ts" usage.mts
cp "$root/packages/client/tests/types/usage.ts" usage.cts

./node_modules/.bin/tsc --noEmit --strict --target es2023 --lib es2023 --skipLibCheck --types node \
  --module nodenext --moduleResolution nodenext usage.mts usage.cts

echo 'types ok'

if [ -z "${SERVER_BIN:-}" ]; then
  echo 'SERVER_BIN is not set, so the process check did not run'

  exit 0
fi

cat > worker.mjs <<'JS'
import Anway from '@anway/client';

const worker = new Anway.Worker({ queues: ['default'] });

worker.on('x', () => {});

await worker.run();

console.log('worker stopped');
JS

port=17477

BIND_ADDR="127.0.0.1:$port" DATA_DIR="$work/data" "$SERVER_BIN" 2>/dev/null &
server=$!
trap 'kill "$server" 2>/dev/null || true; rm -rf "$work"' EXIT
sleep 1

for runtime in node bun; do
  ANWAY_URL="tcp://127.0.0.1:$port" "$runtime" worker.mjs > "$runtime.log" 2>&1 &
  pid=$!
  sleep 1
  kill -TERM "$pid"
  wait "$pid"
  grep -q 'worker stopped' "$runtime.log"
  echo "$runtime SIGTERM ok"
done

echo 'package checks passed'
