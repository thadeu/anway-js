import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import Anway, { ClosedError, ConnectionError, Job, NotUniqueError, ServerError } from '../src/index';
import { cleanup, openClient, retries, startServer } from './support/server';

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
    const unique = () => new Job('x', [1]).setCustom('unique_for', 60);

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

    await client.mutate('requeue', 'retries', { jobtype: 'a' });

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
    const jid = await client.push(new Job('x').setCustom('track', 1));

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
