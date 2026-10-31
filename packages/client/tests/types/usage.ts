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

// @ts-expect-error The args do not match the type.
new Anway.Job<[string]>('x', [1]);

export { address, client, options };
