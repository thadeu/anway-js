import { hostname } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';

import { afterEach, describe, expect, it } from 'vitest';

import { Job, Worker, type Options, type WorkerOptions } from '../src/index';
import { translateOptions } from '../src/options';
import { ScriptedServer, type BeatReply } from './support/scripted';
import {
  cleanup,
  openClient,
  processed,
  record,
  retries,
  startServer,
  startWebServer,
  waitFor,
  type Recorder,
} from './support/server';

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

/** A handler that waits for release, and counts the jobs that it runs at once. After open, jobs do not wait. */
function gate() {
  const waiting: (() => void)[] = [];
  let opened = false;

  const state = {
    running: 0,
    peak: 0,
    handler: async () => {
      state.running++;
      state.peak = Math.max(state.peak, state.running);

      if (!opened) await new Promise<void>(resolve => waiting.push(resolve));

      state.running--;
    },
    release: () => waiting.splice(0).forEach(resolve => resolve()),
    open: () => {
      opened = true;
      state.release();
    },
  };

  return state;
}

/** The concurrency of each BEAT that the recorder saw. */
function beatCounts(recorder: Recorder): (number | undefined)[] {
  return recorder.bodies<{ concurrency?: number }>('BEAT').map(body => body.concurrency);
}

describe('worker conformance', () => {
  it('W01 runs a job and acks it', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    const subject = worker(server.url).on('x', () => {});

    start(subject);

    await client.push(new Job('x'));
    await waitFor(async () => (await processed(client)) === 1);
  });

  it('W02, W03, and W04 fail the job and continue', async () => {
    const server = await startServer();
    const client = openClient(server.url);

    const subject = worker(server.url)
      .on('rejected', async () => {
        throw new Error('failed');
      })
      .on('thrown', () => {
        throw new TypeError('boom');
      })
      .on('ok', () => {});

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
      .on('x', () => {
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

    subject.on(
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

    const subject = worker(server.url).on('slow', async () => {
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

    subject.on(
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

    subject.on(
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

  it('W11 keeps running across a server restart', async () => {
    const server = await startServer();
    const client = openClient(server.url);
    const { done } = start(worker(server.url, { beatInterval: 50 }).on('x', () => {}));
    let stopped = false;

    done.then(() => (stopped = true));

    await client.push(new Job('x'));
    await waitFor(async () => (await processed(client)) === 1);

    await server.restart();

    await client.push(new Job('x'));
    await waitFor(async () => (await processed(client)) === 2, 10_000);

    expect(stopped).toBe(false);
  });

  it('W12 starts and stops slots when a beat asks, and a stopping slot finishes its job', async () => {
    const { server, api } = await startWebServer();
    const client = openClient(server.url);
    const hold = gate();
    const slow = gate();
    const tag = 'w12-scale';

    const subject = worker(server.url, { beatInterval: 50, tag }).on('hold', hold.handler).on('slow', slow.handler);

    start(subject);

    await waitFor(async () => (await api.worker(tag))?.scalable === true);

    const wid = (await api.worker(tag))?.wid as string;

    await api.scale(wid, 3);
    await waitFor(async () => {
      const view = await api.worker(tag);

      return view?.concurrency === 3 && view.wanted === null;
    });

    for (let i = 0; i < 3; i++) await client.push(new Job('hold'));

    await waitFor(() => hold.running === 3);

    hold.release();

    await waitFor(async () => (await processed(client)) === 3);

    await client.push(new Job('slow'));
    await waitFor(() => slow.running === 1);

    await api.scale(wid, 1);
    await waitFor(async () => {
      const view = await api.worker(tag);

      return view?.concurrency === 1 && view.wanted === null;
    });

    // The idle slots end their FETCH, which waits up to 2 seconds, and stop.
    await sleep(2_500);

    await client.push(new Job('hold'));
    await client.push(new Job('hold'));
    await sleep(300);

    expect(hold.running).toBe(0);

    slow.release();

    await waitFor(async () => (await processed(client)) === 4);
    await waitFor(() => hold.running === 1);
    await sleep(300);

    expect(hold.running).toBe(1);

    hold.open();

    await waitFor(async () => (await processed(client)) === 6);

    expect(await retries(client)).toBe(0);
  });

  it('W12 reports a new count at once, not at the next beat', async () => {
    const { server, api } = await startWebServer();
    const recorder = await record(server);
    const tag = 'w12-report';

    start(worker(recorder.url, { beatInterval: 2_000, tag }).on('x', () => {}));

    await waitFor(async () => (await api.worker(tag))?.scalable === true);

    await api.scale((await api.worker(tag))?.wid as string, 3);

    await waitFor(async () => {
      const view = await api.worker(tag);

      return view?.concurrency === 3 && view.wanted === null;
    }, 3_500);

    const beats = recorder.commands.filter(command => command.verb === 'BEAT');
    const reported = beatCounts(recorder).indexOf(3);
    const gap = (beats[reported]?.at ?? 0) - (beats[reported - 1]?.at ?? 0);

    expect(reported).toBeGreaterThan(0);
    expect(gap).toBeLessThan(500);
  });
});

describe('worker scaling', () => {
  it('sends its identity in HELLO, and its slots in the first BEAT', async () => {
    const server = await startServer();
    const recorder = await record(server);

    const subject = worker(recorder.url, {
      concurrency: 2,
      queues: ['critical', 'default'],
      hostname: 'worker.1',
      tag: 'shop',
      autoscale: { min: 2, max: 6 },
      maxConcurrency: 8,
    });

    start(subject.on('x', () => {}));

    await waitFor(() => recorder.verbs.includes('FETCH'));

    expect(recorder.bodies('HELLO')[0]).toMatchObject({
      hostname: 'worker.1',
      pid: process.pid,
      labels: ['js'],
      concurrency: 2,
      queues: ['critical', 'default'],
      tag: 'shop',
      autoscale: { min: 2, max: 6 },
      max_concurrency: 8,
    });

    expect(recorder.verbs.indexOf('BEAT')).toBeLessThan(recorder.verbs.indexOf('FETCH'));
    expect(beatCounts(recorder)[0]).toBe(2);

    const plain = await record(server);

    start(worker(plain.url).on('x', () => {}));

    await waitFor(() => plain.verbs.includes('FETCH'));

    const hello = plain.bodies('HELLO')[0] ?? {};

    expect(Object.keys(hello).sort()).toEqual(['concurrency', 'hostname', 'labels', 'pid', 'queues', 'v', 'wid']);
    expect(hello.hostname).toBe(hostname());
  });

  it('rejects a concurrency above maxConcurrency', () => {
    expect(() => worker('tcp://127.0.0.1:1', { concurrency: 4, maxConcurrency: 3 })).toThrow(RangeError);
    expect(() => worker('tcp://127.0.0.1:1', { concurrency: 3, maxConcurrency: 3 })).not.toThrow();
  });

  it('never runs more slots than maxConcurrency', async () => {
    const script = new ScriptedServer(beat => (beat === 2 ? { concurrency: 50 } : undefined));
    const url = await script.listen();
    const hold = gate();

    for (let i = 0; i < 5; i++) script.jobs.push(new Job('hold').toJSON());

    const { stop, done } = start(worker(url, { beatInterval: 50, maxConcurrency: 3 }).on('hold', hold.handler));

    try {
      await waitFor(() => hold.running === 3);
      await sleep(200);

      expect(hold.peak).toBe(3);
      expect(script.beatBodies.map(body => body.concurrency)).toContain(3);
      expect(Math.max(...script.beatBodies.map(body => body.concurrency ?? 0))).toBe(3);
    } finally {
      hold.open();
      stop();

      await within(done);

      script.close();
    }
  });

  it('cancels a pending stop when a beat asks for more slots', async () => {
    let next: BeatReply | undefined;

    const script = new ScriptedServer(() => {
      const reply = next;

      next = undefined;

      return reply;
    });

    const url = await script.listen();
    const hold = gate();
    const counts = () => script.beatBodies.map(body => body.concurrency);

    for (let i = 0; i < 6; i++) script.jobs.push(new Job('hold').toJSON());

    const { stop, done } = start(worker(url, { concurrency: 3, beatInterval: 50 }).on('hold', hold.handler));

    try {
      await waitFor(() => hold.running === 3);

      next = { concurrency: 1 };

      await waitFor(() => counts().includes(1));

      next = { concurrency: 3 };

      await waitFor(() => counts().lastIndexOf(3) > counts().indexOf(1));
      await sleep(200);

      expect(hold.running).toBe(3);
      expect(hold.peak).toBe(3);

      hold.release();

      await waitFor(() => script.acks.length === 3);
      await waitFor(() => hold.running === 3);
    } finally {
      hold.open();
      stop();

      await within(done);

      script.close();
    }
  });

  it('grows the connection pool for new slots', async () => {
    const script = new ScriptedServer(beat => (beat === 2 ? { concurrency: 4 } : undefined));
    const url = await script.listen();

    script.fetchWait = 300;

    const { stop, done } = start(worker(url, { beatInterval: 50 }).on('x', () => {}));

    try {
      await waitFor(() => script.peakFetches === 4, 3_000);
    } finally {
      stop();

      await within(done);

      script.close();
    }
  });

  it('obeys the state and the count of one beat reply', async () => {
    const script = new ScriptedServer(beat => {
      if (beat === 2) return { state: 'quiet', concurrency: 2 };

      return beat > 6 ? 'terminate' : undefined;
    });

    const url = await script.listen();
    const { done } = start(worker(url, { beatInterval: 50 }));

    try {
      await within(done);

      const bodies = script.beatBodies;

      expect(bodies[2]?.concurrency).toBe(2);
      expect(bodies.at(-1)).toMatchObject({ current_state: 'quiet', concurrency: 2 });
    } finally {
      script.close();
    }
  });
});

describe('topic worker conformance', () => {
  it('T05 sends SUBSCRIBE before the first FETCH and runs the copies', async () => {
    const server = await startServer();
    const recorder = await record(server);
    const client = openClient(server.url);
    const received: Job[] = [];

    const subject = worker(recorder.url).on(
      'chat.broadcast',
      job => {
        received.push(job);
      },
      { queue: 'realtime', retry: 2, topics: ['chat.message'] }
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
      { queue: 'cassandra', event: 'chat.store' },
      { queue: 'push', event: 'chat.notify' },
    ];

    for (const { queue, event } of subscribers) {
      const recorder = await record(server);

      const subject = worker(recorder.url).on(
        event,
        job => {
          events[job.type]?.push(job.custom?.event as string);
        },
        { queue, topics: ['chat.message'] }
      );

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

describe('input contract conformance', () => {
  it('I02 merges two option sets in order', async () => {
    const first: Options = {
      queue: 'first',
      retry: 1,
      uniqueFor: 60,
      topics: ['chat.message', 'chat.edit'],
      custom: { kept: 1, shared: 'first' },
    };

    const second: Options = {
      queue: 'second',
      reserveFor: 120,
      uniqueFor: 120,
      topics: ['chat.edit', 'chat.delete'],
      custom: { added: 2, shared: 'second' },
    };

    expect(translateOptions(first, second)).toEqual({
      fields: {
        queue: 'second',
        retry: 1,
        reserve_for: 120,
        custom: { kept: 1, added: 2, shared: 'second', unique_for: 120 },
      },
      topics: ['chat.message', 'chat.edit', 'chat.delete'],
    });

    const server = await startServer();
    const recorder = await record(server);

    start(worker(recorder.url).on('chat.store', () => {}, first, second));

    await waitFor(() => recorder.verbs.includes('FETCH'));

    expect(recorder.bodies('SUBSCRIBE')).toEqual(
      ['chat.message', 'chat.edit', 'chat.delete'].map(topic => ({
        topic,
        queue: 'second',
        jobtype: 'chat.store',
        retry: 1,
        reserve_for: 120,
      }))
    );
  });

  it('I03 rejects a second declaration for the same event', () => {
    const subject = worker('tcp://127.0.0.1:1').on('chat.store', () => {});

    expect(() => subject.on('chat.store', () => {}, { topics: ['chat.message'] })).toThrow(
      /handler for the event "chat.store"/
    );

    expect(() => subject.on('chat.notify', () => {})).not.toThrow();
  });

  it('I04 subscribes each topic of a declaration and runs the copies', async () => {
    const server = await startServer();
    const recorder = await record(server);
    const client = openClient(server.url);
    const received: Job[] = [];
    const topics = ['chat.message', 'chat.edit'];

    const subject = worker(recorder.url).on(
      'chat.store',
      job => {
        received.push(job);
      },
      { queue: 'cassandra', retry: 4, reserveFor: 90, topics, at: new Date(Date.now() + 3_600_000) }
    );

    start(subject);

    await waitFor(() => recorder.verbs.includes('FETCH'));

    expect(recorder.bodies('SUBSCRIBE')).toEqual(
      topics.map(topic => ({ topic, queue: 'cassandra', jobtype: 'chat.store', retry: 4, reserve_for: 90 }))
    );

    expect(recorder.verbs.lastIndexOf('SUBSCRIBE')).toBeLessThan(recorder.verbs.indexOf('FETCH'));

    for (const topic of topics) {
      expect((await client.publish(topic, [topic], { id: `${topic}-1` })).jobs).toBe(1);
    }

    await waitFor(async () => (await processed(client)) === 2);

    const copies = received.map(job => job.args[0]).sort();

    expect(copies).toEqual([...topics].sort());

    for (const job of received) {
      expect(job).toMatchObject({ type: 'chat.store', queue: 'cassandra', retry: 4, reserveFor: 90 });
    }
  });
});
