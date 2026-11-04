import Anway, { Client } from '@anway/client';
import * as Named from '@anway/client';

const job: Anway.Job<[string, number]> = new Anway.Job<[string, number]>('email.send', ['a', 1]);
const address: string = job.args[0];
const client: Client = new Anway.Client();
const worker: Anway.Worker = new Named.Worker({ queues: ['default'] });
const options: Anway.WorkerOptions = { concurrency: 2 };

const autoscale: Anway.Autoscale = { min: 2, max: 20 };

const scaled = new Anway.Worker({
  concurrency: 4,
  hostname: 'worker.1',
  tag: 'shop',
  autoscale,
  maxConcurrency: 20,
});

// @ts-expect-error autoscale needs max.
new Anway.Worker({ autoscale: { min: 2 } });

async function bulk(): Promise<Record<string, string>> {
  const failures: Record<string, string> = await client.pushBulk([new Anway.Job('x'), new Anway.Job('y')]);
  const heartbeat: Anway.Heartbeat = await client.heartbeat(undefined, { concurrency: 4 });
  const count: number | null = heartbeat.concurrency;

  client.growPool(count ?? 4);

  return failures;
}

const declaration: Anway.Options = { queue: 'realtime', retry: 3, topics: ['chat.message'] };
const late: Named.Options = { reserveFor: 60, topics: ['chat.edit'], custom: { team: 'chat' } };

worker.on<[string]>('email.send', async (received, { signal }) => {
  const value: string = received.args[0];
  const event: string = received.type;

  signal.throwIfAborted();

  return [value, event];
});

worker.on<[string]>(
  'chat.broadcast',
  received => {
    const body: string = received.args[0];

    return body;
  },
  declaration,
  late
);

const pushed = new Anway.Job('mail.send', ['a@b.com'], {
  queue: 'mail',
  at: new Date(),
  uniqueFor: 600,
  uniqueUntil: 'success',
  expiresIn: 60,
  expiresAt: new Date(),
  track: true,
  onDead: { event: 'mail.bounced', queue: 'rails' },
  custom: { source: 'usage' },
});

// @ts-expect-error uniqueUntil is success or start.
new Anway.Job('x', [], { uniqueUntil: 'end' });

// @ts-expect-error The old register method does not exist.
worker.register('x', () => {});

const subscription: Anway.Subscription = {
  topic: 'chat.message',
  queue: 'push',
  event: 'chat.notify',
  reserveFor: 60,
};
const publishOptions: Anway.PublishOptions = { id: 'msg-42', uniqueFor: 0, custom: { room: 'general' } };

async function publish(): Promise<number> {
  await client.subscribe(subscription);

  const published: Anway.Published = await client.publish('chat.message', [{ body: 'hi' }], publishOptions);

  await client.unsubscribe(subscription);

  return published.jobs;
}

// @ts-expect-error The args do not match the type.
new Anway.Job<[string]>('x', [1]);

export { address, bulk, client, options, publish, pushed, scaled };
