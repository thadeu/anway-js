import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { connect, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import { Client, type ClientOptions } from '../../src/index';

function freePort(): Promise<number> {
  return new Promise(resolve => {
    const server = createServer();

    server.listen(0, '127.0.0.1', () => {
      const address = server.address() as { port: number };

      server.close(() => resolve(address.port));
    });
  });
}

function portOpen(port: number): Promise<boolean> {
  return new Promise(resolve => {
    const socket = connect({ host: '127.0.0.1', port });

    socket.on('connect', () => {
      socket.destroy();
      resolve(true);
    });

    socket.on('error', () => resolve(false));
  });
}

export class TestServer {
  readonly dir = mkdtempSync(join(tmpdir(), 'anway-'));
  port = 0;
  private child?: ChildProcess;
  private exited?: Promise<void>;

  constructor(private readonly env: Record<string, string> = {}) {}

  get addr(): string {
    return `127.0.0.1:${this.port}`;
  }

  get url(): string {
    return `tcp://${this.addr}`;
  }

  async start(): Promise<this> {
    this.port ||= await freePort();

    const env = {
      ...process.env,
      BIND_ADDR: this.addr,
      DATA_DIR: this.dir,
      SHUTDOWN_TIMEOUT: '1s',
      ...this.env,
    };

    const child = spawn(process.env.SERVER_BIN as string, [], { env, stdio: 'ignore' });

    this.child = child;
    this.exited = new Promise(resolve => child.once('exit', () => resolve()));

    for (let i = 0; i < 300; i++) {
      if (await portOpen(this.port)) return this;

      await sleep(10);
    }

    throw new Error('server did not start');
  }

  signal(name: NodeJS.Signals): void {
    this.child?.kill(name);
  }

  async kill(): Promise<void> {
    this.child?.kill('SIGKILL');

    await this.exited;
  }

  async restart(): Promise<void> {
    await this.kill();
    await this.start();
  }

  async cleanup(): Promise<void> {
    await this.kill();

    rmSync(this.dir, { recursive: true, force: true });
  }
}

const servers: TestServer[] = [];
const clients: Client[] = [];

export async function startServer(env: Record<string, string> = {}): Promise<TestServer> {
  const server = await new TestServer(env).start();

  servers.push(server);

  return server;
}

export function openClient(url: string, options: ClientOptions = {}): Client {
  const client = new Client({ url, wid: `test-${Math.random().toString(16).slice(2)}`, ...options });

  clients.push(client);

  return client;
}

export async function cleanup(): Promise<void> {
  clients.splice(0).forEach(client => client.close());

  await Promise.all(servers.splice(0).map(server => server.cleanup()));
}

export async function waitFor(check: () => boolean | Promise<boolean>, limit = 5_000): Promise<void> {
  const deadline = Date.now() + limit;

  while (!(await check())) {
    if (Date.now() > deadline) throw new Error('condition was not met');

    await sleep(10);
  }
}

export async function retries(client: Client): Promise<number> {
  return (await client.info()).faktory.tasks.Retries?.size ?? 0;
}

export async function processed(client: Client): Promise<number> {
  return (await client.info()).faktory.total_processed;
}
