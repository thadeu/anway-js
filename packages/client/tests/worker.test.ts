import { setTimeout as sleep } from 'node:timers/promises';

import { afterEach, describe, expect, it } from 'vitest';

import { Job, Worker, type WorkerOptions } from '../src/index';
import { ScriptedServer } from './support/scripted';
import { cleanup, openClient, processed, record, retries, startServer, waitFor } from './support/server';

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
    let terminate = false;
    let started = false;

    const script = new ScriptedServer(() => (terminate ? 'terminate' : undefined));
    const url = await script.listen();
    const job = new Job('slow');

    script.jobs.push(job.toJSON());

    const subject = worker(url, { beatInterval: 20, shutdownTimeout: 50, cancelGrace: 100 });

    subject.register(
      'slow',
      (_job, { signal }) =>
        new Promise((_resolve, reject) => {
          started = true;

          signal.addEventListener('abort', () => reject(new Error('canceled')));
        })
    );

    const { done } = start(subject);

    try {
      await waitFor(() => started);

      terminate = true;

      await within(done);

      expect(script.acks).toEqual([]);
      expect(script.fails).toEqual([expect.objectContaining({ jid: job.jid, message: 'canceled' })]);
    } finally {
      script.close();
    }
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
    const script = new ScriptedServer(beat => {
      if (beat === 1) return undefined;

      return beat > 20 ? 'terminate' : 'quiet';
    });

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

describe('topic worker conformance', () => {
  it('T05 sends SUBSCRIBE before the first FETCH and runs the copies', async () => {
    const server = await startServer();
    const recorder = await record(server);
    const client = openClient(server.url);
    const received: Job[] = [];

    const subject = worker(recorder.url).subscribe(
      'chat.message',
      { queue: 'realtime', jobtype: 'chat.broadcast', retry: 2 },
      job => {
        received.push(job);
      }
    );

    start(subject);

    await waitFor(() => recorder.verbs.includes('FETCH'));

    expect(recorder.verbs.indexOf('SUBSCRIBE')).toBeGreaterThan(-1);
    expect(recorder.verbs.indexOf('SUBSCRIBE')).toBeLessThan(recorder.verbs.indexOf('FETCH'));

    expect((await client.publish('chat.message', ['hi'], { id: 'msg-1' })).jobs).toBe(1);

    await waitFor(() => received.length === 1);
    await waitFor(async () => (await processed(client)) === 1);

    expect(received[0]).toMatchObject({ type: 'chat.broadcast', queue: 'realtime', args: ['hi'], retry: 2 });
    expect(received[0]?.custom).toMatchObject({ topic: 'chat.message', event: 'msg-1' });
  });

  it('T06 runs one copy of every event in each of two workers', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    const events: Record<string, string[]> = { 'chat.store': [], 'chat.notify': [] };

    const subscribers = [
      { queue: 'cassandra', jobtype: 'chat.store' },
      { queue: 'push', jobtype: 'chat.notify' },
    ];

    for (const subscriber of subscribers) {
      const recorder = await record(server);

      const subject = worker(recorder.url).subscribe('chat.message', subscriber, job => {
        events[job.type]?.push(job.custom?.event as string);
      });

      start(subject);

      await waitFor(() => recorder.verbs.includes('FETCH'));
    }

    const ids = ['msg-1', 'msg-2', 'msg-3'];

    for (const id of ids) {
      expect((await client.publish('chat.message', [id], { id })).jobs).toBe(2);
    }

    await waitFor(async () => (await processed(client)) === 6);

    expect(events['chat.store']?.sort()).toEqual(ids);
    expect(events['chat.notify']?.sort()).toEqual(ids);
  });
});
