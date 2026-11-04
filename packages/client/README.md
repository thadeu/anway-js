<p>
  <a href="https://github.com/thadeu/anway">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/thadeu/anway/main/brand/png/lockup-on-dark.png">
      <img src="https://raw.githubusercontent.com/thadeu/anway/main/brand/png/lockup-on-light.png" alt="Anway" width="320">
    </picture>
  </a>
</p>

# @anway/client

TypeScript client and worker for [Anway](https://github.com/thadeu/anway). Node 24 or later, or Bun 1.2 or later. ESM and CommonJS. No runtime dependencies.

```bash
pnpm add @anway/client
```

The server address comes from `ANWAY_URL`, for example `tcp://:password@localhost:7419`. Use `tcp+tls://` for TLS.

## Push

```ts
import Anway from '@anway/client';

const client = new Anway.Client();

await client.push(new Anway.Job('email.send', ['a@b.com']));
```

`new Anway.Job(event, args, options)` takes the keys of the [options](#options). Args must be JSON types. Dates, class instances, `undefined`, and `NaN` throw a `TypeError`.

```ts
new Anway.Job('report', [42], { queue: 'low', at: new Date(Date.now() + 60_000) });
new Anway.Job('sync', [42], { uniqueFor: 600, uniqueUntil: 'start' });
new Anway.Job('mail.send', ['a@b.com'], { track: true, onDead: { event: 'mail.bounced', queue: 'rails' } });
```

One client works for many concurrent callers.

## Worker

```ts
import Anway from '@anway/client';

const worker = new Anway.Worker({ queues: ['critical', 'default'], concurrency: 10 });

worker.on<[string]>('email.send', async (job, { signal }) => {
  const [address] = job.args;

  await sendEmail(address, { signal });
});

worker.use(async (job, next) => {
  const started = Date.now();
  const result = await next();

  console.log(job.type, Date.now() - started);

  return result;
});

await worker.run();
```

Run the file as a normal process: `node worker.js` or `bun worker.ts`.

`worker.on(event, handler, ...options)` declares the handler of an event. In the handler, `job.type` is the event of the job.

- A worker has one handler for each event. A second `on` for the same event throws an `Error`. For more than one flow on one topic, use more than one event, and subscribe each event to the topic.
- The option sets merge in order. See [options](#options).
- When the declaration has `topics` or `queue`, the worker also fetches from that queue.
- `on` uses only `queue`, `retry`, `reserveFor`, and `topics`. The keys for a push, such as `at`, `uniqueFor`, and `custom`, have no effect in `on`.

A thrown error or a rejected promise fails the job, and the server retries it. Delivery is at least once, so handlers must be idempotent.

### Concurrency

`concurrency` is the number of jobs in progress, as promises. It is not threads. Use it for I/O work. Synchronous CPU work blocks the event loop, and this also stops the heartbeat. Move heavy CPU work to `worker_threads`, or run more processes.

### Shutdown

The worker stops at SIGTERM, SIGINT, the server `terminate` command, or the `signal` given to `run`. To handle signals yourself, use `worker.run({ handleSignals: false })`. Server `quiet` stops fetching, and the worker stays alive.

1. Running jobs get `shutdownTimeout` (25 seconds) to finish.
2. The worker aborts `context.signal`, and jobs get `cancelGrace` (5 seconds) to return.
3. The worker fails jobs that still run, and `run` resolves. A late result from these handlers is ignored.

Handlers must respect `signal`. JavaScript cannot stop a promise, so a handler that ignores it continues to run until the process ends. Set the container stop timeout above 30 seconds.

## Topics

A topic sends one event to many queues. `publish` makes one job for each subscription of the topic, in one transaction. Each copy is a normal job in the queue of its subscriber, with its own retries and dead set.

In this example, one `chat.message` event goes to three queues: `realtime`, `cassandra`, and `push`.

```ts
import Anway from '@anway/client';

const client = new Anway.Client();

const published = await client.publish('chat.message', [{ room: 'general', body: 'hi' }], { id: 'msg-42' });

console.log(published);
// { event: 'msg-42', jobs: 3, duplicate: false }
```

Each app declares its subscriptions in its worker, with the `topics` key of `on`. The worker records one subscription for each topic, with the event as the job type, and the `queue`, `retry`, and `reserveFor` of the declaration. It adds the queue to the queues of the worker. When `run` starts, the worker sends `SUBSCRIBE` for each subscription, before its first fetch.

```ts
const worker = new Anway.Worker();

worker.on('chat.broadcast', async job => await broadcast(job.args[0]), {
  queue: 'realtime',
  topics: ['chat.message'],
});

worker.on('chat.store', async job => await store(job.custom?.event, job.args[0]), {
  queue: 'cassandra',
  retry: 10,
  topics: ['chat.message', 'chat.edit'],
});

worker.on('chat.notify', async job => await notify(job.args[0]), {
  queue: 'push',
  reserveFor: 60,
  topics: ['chat.message'],
});

await worker.run();
```

- The server saves subscriptions on disk. A copy goes to the queue also when the subscriber app is down.
- `publish(topic, args, { id, uniqueFor, custom })`: with an `id`, a second publish of the same topic and id within `uniqueFor` seconds makes no job, and the reply has `duplicate: true`. The default of `uniqueFor` is one day. `0` turns the check off.
- The server adds `topic` and `event` to `custom` of each copy. Use `custom.event` as the key to make a handler idempotent.
- An event with no subscription goes to the dead set as one job, and the reply has `jobs: 0`.
- `client.subscribe({ topic, queue, event, retry, reserveFor })` is idempotent. `client.unsubscribe({ topic, queue, event })` does not fail for a missing subscription.

See [topics](https://github.com/thadeu/anway/blob/main/docs/dev/topics.md) for the server rules.

## Options

`new Anway.Job` and `worker.on` take the same keys in every Anway client. This is the [input contract](https://github.com/thadeu/anway/blob/main/docs/dev/clients.md#input-contract). The client changes the keys to the fields of the protocol.

| Key | Wire field | Note |
|---|---|---|
| `queue` | `queue` | The default is `default`. |
| `retry` | `retry` | The server default is 25. `0` discards at the first failure. `-1` goes to the dead set at once. |
| `reserveFor` | `reserve_for` | Seconds. |
| `backtrace` | `backtrace` | Lines. |
| `at` | `at` | A `Date` to run later. For a push only. |
| `uniqueFor` | `custom.unique_for` | Seconds. |
| `uniqueUntil` | `custom.unique_until` | `'success'` or `'start'`. |
| `expiresIn` | `custom.expires_in` | Seconds. |
| `expiresAt` | `custom.expires_at` | A `Date`. It goes out in ISO 8601. |
| `track` | `custom.track` | `true` sends `1`. |
| `onDead` | `custom.on_dead` | `{ event, queue }`. The `event` goes out as `jobtype`. |
| `custom` | `custom` | The keys of the app. The keys above win over a key with the same name here. |
| `topics` | One `SUBSCRIBE` for each topic | For `worker.on` only. A push ignores it. |

`worker.on` takes more than one option set. The sets merge in order:

- A single value, such as `queue` or `retry`: the last set that has it wins.
- `topics`: the topics of all sets add up, in order, with no duplicates.
- `custom`: the objects merge, and a later key wins.

```ts
const realtime: Anway.Options = { queue: 'realtime', retry: 3 };

worker.on('chat.broadcast', broadcast, realtime, { topics: ['chat.message'] });
```

## Client

`Anway.Client` has the operations of [anway-go](https://github.com/thadeu/anway-go), in camelCase.

| Group | Operations |
|---|---|
| Jobs | `push`, `fetch`, `ack`, `fail`, `beat` |
| Topics | `publish`, `subscribe`, `unsubscribe` |
| Queues | `pauseQueues`, `resumeQueues`, `removeQueues`, `queueSizes`, `info` |
| Admin | `mutate` |
| Batches | `batchNew`, `batchOpen`, `batchCommit`, `batchStatus` |
| Tracking | `trackGet`, `trackSet` |

`mutate(command, target, { event, jids, regexp })` selects jobs by event, jid, or a regular expression.

The error kinds are `NotUniqueError`, `ClosedError`, `ServerError`, and `ConnectionError`. All extend `AnwayError`. A failed call does not resend.

Named imports also work: `import { Client, Job, Worker } from '@anway/client'`.

## License

MIT
