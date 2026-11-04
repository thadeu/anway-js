import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { connect, createServer, type Server, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import { Client, type ClientOptions } from '../../src/index';

export function freePort(): Promise<number> {
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

export interface Command {
  verb: string;
  /** The text after the verb, such as the JSON of a job. */
  body: string;
  /** Date.now() when the recorder got the command. */
  at: number;
}

// A TCP proxy to a server. It records each command that a client sends.
export class Recorder {
  readonly verbs: string[] = [];
  readonly commands: Command[] = [];
  private readonly sockets = new Set<Socket>();
  private readonly server: Server;
  private port = 0;

  constructor(private readonly target: TestServer) {
    this.server = createServer(socket => this.accept(socket));
  }

  get url(): string {
    return `tcp://127.0.0.1:${this.port}`;
  }

  listen(): Promise<this> {
    return new Promise(resolve => {
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server.address() as { port: number }).port;

        resolve(this);
      });
    });
  }

  close(): void {
    this.sockets.forEach(socket => socket.destroy());
    this.server.close();
  }

  /** The JSON bodies of the commands with this verb, in order. */
  bodies<T = Record<string, unknown>>(verb: string): T[] {
    return this.commands.filter(command => command.verb === verb).map(command => JSON.parse(command.body) as T);
  }

  private accept(client: Socket): void {
    const upstream = connect({ host: '127.0.0.1', port: this.target.port });
    let buffered = '';

    for (const socket of [client, upstream]) {
      this.sockets.add(socket);
      socket.on('error', () => {});
      socket.on('close', () => {
        client.destroy();
        upstream.destroy();
      });
    }

    upstream.pipe(client);

    client.on('data', chunk => {
      buffered += chunk.toString();

      const lines = buffered.split('\n');

      buffered = lines.pop() ?? '';

      for (const line of lines) {
        const text = line.trim();
        const space = text.indexOf(' ');
        const verb = space === -1 ? text : text.slice(0, space);

        this.verbs.push(verb);
        this.commands.push({ verb, body: space === -1 ? '' : text.slice(space + 1), at: Date.now() });
      }

      upstream.write(chunk);
    });
  }
}

const servers: TestServer[] = [];
const clients: Client[] = [];
const recorders: Recorder[] = [];

export async function startServer(env: Record<string, string> = {}): Promise<TestServer> {
  const server = await new TestServer(env).start();

  servers.push(server);

  return server;
}

export async function record(target: TestServer): Promise<Recorder> {
  const recorder = await new Recorder(target).listen();

  recorders.push(recorder);

  return recorder;
}

export function openClient(url: string, options: ClientOptions = {}): Client {
  const client = new Client({ url, wid: `test-${Math.random().toString(16).slice(2)}`, ...options });

  clients.push(client);

  return client;
}

export async function cleanup(): Promise<void> {
  clients.splice(0).forEach(client => client.close());
  recorders.splice(0).forEach(recorder => recorder.close());

  await Promise.all(servers.splice(0).map(server => server.cleanup()));
}

export async function waitFor(check: () => boolean | Promise<boolean>, limit = 5_000): Promise<void> {
  const deadline = Date.now() + limit;

  while (!(await check())) {
    if (Date.now() > deadline) throw new Error('condition was not met');

    await sleep(10);
  }
}

/** A worker as the HTTP API shows it. */
export interface WorkerView {
  wid: string;
  hostname: string;
  tag: string;
  concurrency: number;
  wanted: number | null;
  scalable: boolean;
  busy: number;
}

/** The HTTP API of a test server. */
export class WebApi {
  constructor(
    private readonly addr: string,
    private readonly token: string
  ) {}

  /** The worker with this tag, or undefined. */
  async worker(tag: string): Promise<WorkerView | undefined> {
    const reply = await this.request('GET', '/api/v1/workers');
    const { workers } = (await reply.json()) as { workers: WorkerView[] };

    return workers.find(worker => worker.tag === tag);
  }

  async scale(wid: string, concurrency: number): Promise<void> {
    const reply = await this.request('PATCH', `/api/v1/workers/${wid}`, { concurrency });

    if (!reply.ok) throw new Error(`scale failed: ${reply.status} ${await reply.text()}`);
  }

  private request(method: string, path: string, body?: unknown): Promise<Response> {
    return fetch(`http://${this.addr}${path}`, {
      method,
      headers: { Authorization: `Bearer ${this.token}` },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  }
}

/** Starts a server with the HTTP API on, and waits for the API port. */
export async function startWebServer(): Promise<{ server: TestServer; api: WebApi }> {
  const port = await freePort();
  const server = await startServer({ WEB_ADDR: `127.0.0.1:${port}`, WEB_TOKEN: 't' });

  await waitFor(() => portOpen(port));

  return { server, api: new WebApi(`127.0.0.1:${port}`, 't') };
}

export async function retries(client: Client): Promise<number> {
  return (await client.info()).faktory.tasks.Retries?.size ?? 0;
}

export async function dead(client: Client): Promise<number> {
  return (await client.info()).faktory.tasks.Dead?.size ?? 0;
}

export async function processed(client: Client): Promise<number> {
  return (await client.info()).faktory.total_processed;
}
