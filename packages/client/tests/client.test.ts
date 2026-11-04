import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import Anway, { ClosedError, ConnectionError, Job, NotUniqueError, ServerError } from '../src/index';
import { cleanup, dead, openClient, record, retries, startServer } from './support/server';

afterEach(cleanup);

describe('client conformance', () => {
  it('C01 connects with a URL and with ANWAY_URL', async () => {
    const server = await startServer();

    expect(await openClient(server.url).info()).toHaveProperty('server');

    process.env.ANWAY_URL = server.url;

    try {
      expect(await openClient('').info()).toHaveProperty('server');
    } finally {
      delete process.env.ANWAY_URL;
    }
  });

  it('C02 connects with a password and rejects a wrong one', async () => {
    const server = await startServer({ SERVER_PASSWORD: 'secret' });

    expect(await openClient(`tcp://:secret@${server.addr}`).info()).toHaveProperty('server');
    await expect(openClient(`tcp://:wrong@${server.addr}`).info()).rejects.toBeInstanceOf(ServerError);
  });

  it('C03 connects with tcp+tls', async () => {
    const dir = (await startServer()).dir;
    const key = join(dir, 'key.pem');
    const cert = join(dir, 'cert.pem');

    execFileSync(
      'openssl',
      [
        'req',
        '-x509',
        '-newkey',
        'rsa:2048',
        '-nodes',
        '-days',
        '1',
        '-subj',
        '/CN=localhost',
        '-addext',
        'subjectAltName=DNS:localhost',
        '-keyout',
        key,
        '-out',
        cert,
      ],
      { stdio: 'ignore' }
    );

    const server = await startServer({ TLS_CERT_FILE: cert, TLS_KEY_FILE: key });
    const url = `tcp+tls://localhost:${server.port}`;

    expect(await openClient(url, { tls: { ca: readFileSync(cert) } }).info()).toHaveProperty('server');
    await expect(openClient(url).info()).rejects.toBeInstanceOf(ConnectionError);
  });

  it('C04 pushes, fetches, and acks a job', async () => {
    const client = openClient((await startServer()).url);
    const args = ['a@b.com', 42, { k: [1.5, null] }];
    const jid = await client.push(new Job('email', args));
    const job = await client.fetch('default');

    expect(job?.jid).toBe(jid);
    expect(job?.type).toBe('email');
    expect(job?.args).toEqual(args);

    await client.ack(jid);
  });

  it('C05 returns nothing for an empty queue', async () => {
    expect(await openClient((await startServer()).url).fetch('default')).toBeNull();
  });

  it('C06 fails a job and keeps the error data', async () => {
    const client = openClient((await startServer()).url);
    const jid = await client.push(new Job('x'));

    await client.fetch('default');
    await client.fail(jid, 'Boom', 'failed');

    expect(await retries(client)).toBe(1);

    await client.mutate('requeue', 'retries');

    const job = await client.fetch('default');

    expect(job?.failure).toMatchObject({ errtype: 'Boom', message: 'failed' });
  });

  it('C07 rejects a second unique push with the not-unique kind', async () => {
    const client = openClient((await startServer()).url);
    const unique = () => new Job('x', [1], { uniqueFor: 60 });

    await client.push(unique());

    await expect(client.push(unique())).rejects.toBeInstanceOf(NotUniqueError);
    expect(await client.info()).toHaveProperty('server');
  });

  it('C08 keeps a scheduled job out of the queue', async () => {
    const client = openClient((await startServer()).url);

    await client.push(new Job('x', [], { at: new Date(Date.now() + 3_600_000) }));

    expect(await client.fetch('default')).toBeNull();
  });

  it('C09 pauses and resumes a queue', async () => {
    const client = openClient((await startServer()).url);

    await client.push(new Job('x'));
    await client.pauseQueues('default');

    expect(await client.fetch('default')).toBeNull();

    await client.resumeQueues('default');

    expect(await client.fetch('default')).not.toBeNull();
  });

  it('C10 requeues retries that match the filter', async () => {
    const client = openClient((await startServer()).url);

    for (const kind of ['a', 'b']) {
      const jid = await client.push(new Job(kind));

      await client.fetch('default');
      await client.fail(jid, 'Boom', 'failed');
    }

    await client.mutate('requeue', 'retries', { event: 'a' });

    expect(await retries(client)).toBe(1);
    expect((await client.fetch('default'))?.type).toBe('a');
  });

  it('C11 runs complete, then success', async () => {
    const client = openClient((await startServer()).url);
    const bid = await client.batchNew({ success: new Job('on-success'), complete: new Job('on-complete') });

    await client.push(new Job('member').setCustom('bid', bid));
    await client.batchCommit(bid);

    for (const expected of ['member', 'on-complete', 'on-success']) {
      const job = await client.fetch('default');

      expect(job?.type).toBe(expected);

      await client.ack(job?.jid as string);
    }

    expect(await client.batchStatus(bid)).toMatchObject({ total: 1, pending: 0 });
  });

  it('C12 sets and gets job progress', async () => {
    const client = openClient((await startServer()).url);
    const jid = await client.push(new Job('x', [], { track: true }));

    await client.trackSet({ jid, percent: 40, desc: 'step' });

    expect(await client.trackGet(jid)).toMatchObject({ percent: 40, desc: 'step' });
  });

  it('C13 returns queue sizes', async () => {
    const client = openClient((await startServer()).url);

    await client.push(new Job('x'));

    expect(await client.queueSizes()).toEqual({ default: 1 });
  });

  it('C14 works from many concurrent callers', async () => {
    const client = openClient((await startServer()).url);

    await Promise.all(Array.from({ length: 100 }, () => client.push(new Job('x'))));

    expect((await client.queueSizes()).default).toBe(100);
  });

  it('C15 reconnects after a server restart', async () => {
    const server = await startServer();
    const client = openClient(server.url);

    await client.push(new Job('x'));
    await server.restart();
    await client.push(new Job('x'));

    expect((await client.queueSizes()).default).toBe(2);
  });

  it('C16 rejects a call on a closed client', async () => {
    const client = openClient((await startServer()).url);

    client.close();

    await expect(client.push(new Job('x'))).rejects.toBeInstanceOf(ClosedError);
  });
});

describe('topic conformance', () => {
  const chat = [
    { queue: 'realtime', event: 'chat.broadcast' },
    { queue: 'cassandra', event: 'chat.store' },
    { queue: 'push', event: 'chat.notify' },
  ];

  it('T01 publishes one copy to each subscriber queue', async () => {
    const client = openClient((await startServer()).url);
    const args = [{ room: 'general', body: 'hi' }];

    for (const subscriber of chat) {
      await client.subscribe({ topic: 'chat.message', ...subscriber, retry: 3, reserveFor: 60 });
    }

    const published = await client.publish('chat.message', args, { id: 'msg-42', custom: { source: 'test' } });

    expect(published).toEqual({ event: 'msg-42', jobs: 3, duplicate: false });

    for (const { queue, event } of chat) {
      const job = await client.fetch(queue);

      expect(job).toMatchObject({ type: event, queue, args, retry: 3, reserveFor: 60 });
      expect(job?.custom).toMatchObject({ topic: 'chat.message', event: 'msg-42', source: 'test' });
    }
  });

  it('T02 keeps one subscription and unsubscribes twice without an error', async () => {
    const client = openClient((await startServer()).url);
    const subscription = { topic: 'chat.message', queue: 'realtime', event: 'chat.broadcast' };

    await client.subscribe(subscription);
    await client.subscribe({ ...subscription, retry: 5 });

    expect((await client.publish('chat.message')).jobs).toBe(1);
    expect((await client.fetch('realtime'))?.retry).toBe(5);

    await client.unsubscribe(subscription);
    await client.unsubscribe(subscription);

    expect((await client.publish('chat.message')).jobs).toBe(0);
  });

  it('T03 returns duplicate for a second publish of the same event id', async () => {
    const client = openClient((await startServer()).url);

    await client.subscribe({ topic: 'chat.message', queue: 'realtime', event: 'chat.broadcast' });

    expect(await client.publish('chat.message', [1], { id: 'msg-1' })).toMatchObject({ duplicate: false, jobs: 1 });
    expect(await client.publish('chat.message', [1], { id: 'msg-1' })).toMatchObject({ duplicate: true, jobs: 0 });
    expect(await client.queueSizes()).toEqual({ realtime: 1 });

    for (let i = 0; i < 2; i++) {
      expect(await client.publish('chat.message', [2], { id: 'msg-2', uniqueFor: 0 })).toMatchObject({
        duplicate: false,
        jobs: 1,
      });
    }

    expect(await client.queueSizes()).toEqual({ realtime: 3 });
  });

  it('T04 sends an event with no subscription to the dead set', async () => {
    const client = openClient((await startServer()).url);
    const published = await client.publish('chat.message', [{ body: 'hi' }]);

    expect(published).toMatchObject({ jobs: 0, duplicate: false });
    expect(published.event).toBeTypeOf('string');
    expect(await dead(client)).toBe(1);
  });

  it('rejects publish args that are not JSON types', async () => {
    const client = openClient((await startServer()).url);

    await expect(client.publish('chat.message', [new Date()])).rejects.toBeInstanceOf(TypeError);
  });
});

describe('input contract conformance', () => {
  it('I01 sends the contract keys of a push in custom, and the event as jobtype', async () => {
    const server = await startServer();
    const recorder = await record(server);
    const client = openClient(recorder.url);
    const at = new Date(Date.now() + 3_600_000);
    const expiresAt = new Date(Date.now() + 7_200_000);

    const job = new Job('mail.send', ['a@b.com'], {
      queue: 'mail',
      retry: 3,
      reserveFor: 60,
      backtrace: 10,
      at,
      uniqueFor: 600,
      uniqueUntil: 'start',
      expiresIn: 300,
      expiresAt,
      track: true,
      onDead: { event: 'mail.bounced', queue: 'rails' },
      custom: { unique_for: 1, track: 0, source: 'test' },
      topics: ['ignored.by.push'],
    });

    await client.push(job);

    expect(recorder.bodies('PUSH')).toEqual([
      {
        jid: job.jid,
        jobtype: 'mail.send',
        args: ['a@b.com'],
        queue: 'mail',
        retry: 3,
        reserve_for: 60,
        backtrace: 10,
        at: at.toISOString(),
        custom: {
          source: 'test',
          unique_for: 600,
          unique_until: 'start',
          expires_in: 300,
          expires_at: expiresAt.toISOString(),
          track: 1,
          on_dead: { jobtype: 'mail.bounced', queue: 'rails' },
        },
      },
    ]);

    await client.push(new Job('mail.send', [], { onDead: { event: 'mail.bounced' } }));

    expect(recorder.bodies('PUSH')[1]?.custom).toEqual({ on_dead: { jobtype: 'mail.bounced' } });
  });
});

describe('job', () => {
  it('rejects args that are not JSON types', () => {
    const invalid = [new Date(), undefined, () => 1, new Map(), Number.NaN, BigInt(1), new Job('x')];

    for (const value of invalid) expect(() => new Job('x', [value])).toThrow(TypeError);
  });

  it('exposes the default export as a namespace', () => {
    expect(new Anway.Job('x').type).toBe('x');
    expect(Anway.Client).toBeTypeOf('function');
    expect(Anway.Worker).toBeTypeOf('function');
  });
});
