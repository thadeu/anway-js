import { createServer, type Server, type Socket } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';

import { afterEach, describe, expect, it } from 'vitest';

import { Job, Worker, type WorkerOptions } from '../src/index';
import { cleanup, openClient, processed, retries, startServer, waitFor } from './support/server';

afterEach(cleanup);

const quiet = { logger: { warn: () => {} } };

function start(worker: Worker): { stop: () => void; done: Promise<void> } {
  const controller = new AbortController();
  const done = worker.run({ signal: controller.signal, handleSignals: false });

  stops.push(() => controller.abort());

  return { stop: () => controller.abort(), done };
}

const stops: (() => void)[] = [];

afterEach(() => {
  stops.splice(0).forEach(stop => stop());
});

function worker(url: string, options: WorkerOptions = {}): Worker {
  return new Worker({ url, concurrency: 1, ...quiet, ...options });
}

async function within(promise: Promise<void>, limit = 8_000): Promise<void> {
  await Promise.race([
    promise,
    sleep(limit).then(() => {
      throw new Error('the worker did not stop');
    }),
  ]);
}

// Replies quiet to beats 2 to the last quiet beat, and then terminate.
class ScriptedServer {
  beats = 0;
  quietFetches = 0;
  private readonly server: Server;
  private readonly sockets = new Set<Socket>();

  constructor(private readonly lastQuietBeat: number) {
    this.server = createServer(socket => this.accept(socket));
  }

  listen(): Promise<string> {
    return new Promise(resolve => {
      this.server.listen(0, '127.0.0.1', () => {
        const address = this.server.address() as { port: number };

        resolve(`tcp://127.0.0.1:${address.port}`);
      });
    });
  }

  close(): void {
    this.sockets.forEach(socket => socket.destroy());
    this.server.close();
  }

  private accept(socket: Socket): void {
    let buffered = '';

    this.sockets.add(socket);
    socket.on('error', () => {});
    socket.write('+HI {"v":2}\r\n');

    socket.on('data', async chunk => {
      buffered += chunk.toString();

      let index = buffered.indexOf('\n');

      while (index !== -1) {
        const verb = buffered.slice(0, index).split(' ')[0]?.trim();

        buffered = buffered.slice(index + 1);

        socket.write(await this.reply(verb));

        index = buffered.indexOf('\n');
      }
    });
  }

  private async reply(verb?: string): Promise<string> {
    if (verb === 'FETCH') {
      if (this.beats > 1) this.quietFetches++;

      await sleep(5);

      return '$-1\r\n';
    }

    if (verb !== 'BEAT') return '+OK\r\n';

    this.beats++;

    if (this.beats === 1) return '+OK\r\n';

    const body = JSON.stringify({ state: this.beats > this.lastQuietBeat ? 'terminate' : 'quiet' });

    return `$${body.length}\r\n${body}\r\n`;
  }
}

describe('worker conformance', () => {
  it('W01 runs a job and acks it', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    const subject = worker(server.url).register('x', () => {});

    start(subject);

    await client.push(new Job('x'));
    await waitFor(async () => (await processed(client)) === 1);
  });

  it('W02, W03, and W04 fail the job and continue', async () => {
    const server = await startServer();
    const client = openClient(server.url);

    const subject = worker(server.url)
      .register('rejected', async () => {
        throw new Error('failed');
      })
      .register('thrown', () => {
        throw new TypeError('boom');
      })
      .register('ok', () => {});

    start(subject);

    for (const kind of ['rejected', 'thrown', 'unknown', 'ok']) await client.push(new Job(kind));

    await waitFor(async () => (await retries(client)) === 3 && (await processed(client)) === 1);
  });

  it('W05 wraps a job in middleware order', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    const order: string[] = [];

    const subject = worker(server.url)
      .use(async (_job, next) => {
        order.push('a');

        return next();
      })
      .use(async (_job, next) => {
        order.push('b');

        return next();
      })
      .register('x', () => {
        order.push('job');
      });

    start(subject);

    await client.push(new Job('x'));
    await waitFor(async () => (await processed(client)) === 1);

    expect(order).toEqual(['a', 'b', 'job']);
  });

  it('W06 fails unfinished jobs at server terminate', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    let started = false;

    const subject = worker(server.url, { beatInterval: 20, shutdownTimeout: 50, cancelGrace: 100 });

    subject.register(
      'slow',
      (_job, { signal }) =>
        new Promise((_resolve, reject) => {
          started = true;

          signal.addEventListener('abort', () => reject(new Error('canceled')));
        })
    );

    const { done } = start(subject);

    await client.push(new Job('slow'));
    await waitFor(() => started);

    server.signal('SIGINT');

    await within(done);
    await server.restart();

    const fresh = openClient(server.url);

    expect(await retries(fresh)).toBe(1);

    await fresh.mutate('requeue', 'retries');

    expect((await fresh.fetch('default'))?.failure).toMatchObject({ message: 'canceled' });
  });

  it('W07 lets running jobs finish at a local stop', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    let started = false;
    let finished = false;

    const subject = worker(server.url).register('slow', async () => {
      started = true;

      await sleep(300);

      finished = true;
    });

    const { stop, done } = start(subject);

    await client.push(new Job('slow'));
    await waitFor(() => started);

    stop();

    await within(done);

    expect(finished).toBe(true);
    expect(await processed(client)).toBe(1);
  });

  it('W08 stays alive after quiet and stops fetching', async () => {
    const script = new ScriptedServer(20);
    const url = await script.listen();
    const { done } = start(worker(url, { beatInterval: 50 }));

    try {
      await within(done);

      expect(script.beats).toBeGreaterThan(20);
      expect(script.quietFetches).toBeLessThanOrEqual(2);
    } finally {
      script.close();
    }
  });

  it('W09 aborts running jobs after the timeout', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    let started = false;

    const subject = worker(server.url, { shutdownTimeout: 50 });

    subject.register(
      'slow',
      (_job, { signal }) =>
        new Promise((_resolve, reject) => {
          started = true;

          signal.addEventListener('abort', () => reject(new Error('canceled')));
        })
    );

    const { stop, done } = start(subject);

    await client.push(new Job('slow'));
    await waitFor(() => started);

    stop();

    await within(done);

    expect(await retries(client)).toBe(1);

    await client.mutate('requeue', 'retries');

    expect((await client.fetch('default'))?.failure).toMatchObject({ message: 'canceled' });
  });

  it('W10 does not let a job block the shutdown', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    let started = false;
    let release = () => {};

    const subject = worker(server.url, { shutdownTimeout: 50, cancelGrace: 50 });

    subject.register(
      'stuck',
      () =>
        new Promise<void>(resolve => {
          started = true;
          release = resolve;
        })
    );

    const { stop, done } = start(subject);

    await client.push(new Job('stuck'));
    await waitFor(() => started);

    stop();

    await within(done);

    release();

    await sleep(100);

    expect(await retries(client)).toBe(1);
    expect(await processed(client)).toBe(0);
  });
});
