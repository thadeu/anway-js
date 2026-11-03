import Anway, { Client } from '@anway/client';
import * as Named from '@anway/client';

const job: Anway.Job<[string, number]> = new Anway.Job<[string, number]>('email.send', ['a', 1]);
const address: string = job.args[0];
const client: Client = new Anway.Client();
const worker: Anway.Worker = new Named.Worker({ queues: ['default'] });
const options: Anway.WorkerOptions = { concurrency: 2 };

worker.register<[string]>('email.send', async (received, { signal }) => {
  const value: string = received.args[0];

  signal.throwIfAborted();

  return value;
});

worker.subscribe<[string]>('chat.message', { queue: 'realtime', jobtype: 'chat.broadcast' }, received => {
  const body: string = received.args[0];

  return body;
});

const subscription: Anway.Subscription = {
  topic: 'chat.message',
  queue: 'push',
  jobtype: 'chat.notify',
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

export { address, client, options, publish };
