import { randomBytes } from 'node:crypto';
import { hostname } from 'node:os';
import type { ConnectionOptions as TlsOptions } from 'node:tls';
import { setTimeout as sleep } from 'node:timers/promises';

import { Client, type Heartbeat, type Subscription } from './client';
import type { Autoscale } from './connection';
import { UnknownJobError } from './errors';
import type { Job } from './job';
import { translateOptions, type Options } from './options';

const EMPTY_FETCH_MINIMUM = 200;
const ERROR_PAUSE = 1_000;
const BACKTRACE_LINES = 30;

export interface JobContext {
  /** Aborts when the shutdown timeout ends. */
  signal: AbortSignal;
}

export type Handler<Args extends unknown[] = unknown[]> = (job: Job<Args>, context: JobContext) => unknown;

export type Middleware = (job: Job, next: () => Promise<unknown>) => Promise<unknown>;

export type Logger = Pick<Console, 'warn'>;

export interface WorkerOptions {
  /** A blank URL uses ANWAY_URL. */
  url?: string;
  queues?: string[];
  /**
   * The job slots: jobs that run at the same time. A slot is a fetch-and-run
   * loop in the event loop, not a thread. The server can change this count.
   */
  concurrency?: number;
  /** The name of the worker in the web UI, with tag. The default is the machine name. */
  hostname?: string;
  /** A short name for the app or the role of the process, such as "shop". */
  tag?: string;
  /** Starts the worker with autoscale on. The default min is concurrency. */
  autoscale?: Autoscale;
  /** The most slots that the worker can run, such as the size of its database pool. */
  maxConcurrency?: number;
  /** Milliseconds for running jobs to finish at a stop. */
  shutdownTimeout?: number;
  /** Milliseconds for canceled jobs to return. */
  cancelGrace?: number;
  /** Milliseconds between heartbeats. */
  beatInterval?: number;
  labels?: string[];
  logger?: Logger;
  tls?: TlsOptions;
  /** Milliseconds for each server call. */
  timeout?: number;
}

export interface RunOptions {
  signal?: AbortSignal;
  /** Stop at SIGTERM and SIGINT. The default is true. */
  handleSignals?: boolean;
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve = () => {};

  const promise = new Promise<void>(done => {
    resolve = done;
  });

  return { promise, resolve };
}

function describe(error: unknown): { type: string; message: string; backtrace: string[] } {
  if (error instanceof Error) {
    const lines = error.stack?.split('\n').slice(1, BACKTRACE_LINES + 1) ?? [];

    return { type: error.name, message: error.message, backtrace: lines.map(line => line.trim()) };
  }

  return { type: 'Error', message: String(error), backtrace: [] };
}

/** Fetches and runs jobs until a stop, or until the server sends terminate. */
export class Worker {
  private readonly options: Required<
    Pick<WorkerOptions, 'queues' | 'concurrency' | 'shutdownTimeout' | 'cancelGrace' | 'beatInterval' | 'logger'>
  > &
    WorkerOptions;

  private readonly handlers = new Map<string, Handler>();
  private readonly middleware: Middleware[] = [];
  private readonly subscriptions: Subscription[] = [];

  /** Throws a RangeError when concurrency is above maxConcurrency. */
  constructor(options: WorkerOptions = {}) {
    this.options = {
      queues: ['default'],
      concurrency: 10,
      shutdownTimeout: 25_000,
      cancelGrace: 5_000,
      beatInterval: 15_000,
      logger: console,
      ...options,
    };

    const { concurrency, maxConcurrency } = this.options;

    if (maxConcurrency !== undefined && concurrency > maxConcurrency) {
      throw new RangeError(`concurrency ${concurrency} is above maxConcurrency ${maxConcurrency}`);
    }
  }

  /**
   * Declares the handler of an event. The option sets merge in order, as the
   * input contract tells. The worker records one subscription for each topic,
   * with the event, queue, retry, and reserveFor of the declaration, and run
   * sends SUBSCRIBE before its first FETCH. When the declaration has topics or
   * a queue, the worker also fetches from that queue. Keys for a push only,
   * such as at, uniqueFor, and custom, have no effect here.
   * A second declaration for the same event throws.
   */
  on<Args extends unknown[] = unknown[]>(event: string, handler: Handler<Args>, ...options: Options[]): this {
    if (this.handlers.has(event)) {
      const name = JSON.stringify(event);

      throw new Error(`the worker has a handler for the event ${name}: use one handler for each event`);
    }

    const { fields, topics } = translateOptions(...options);
    const queue = fields.queue ?? 'default';

    this.handlers.set(event, handler as Handler);

    for (const topic of topics) {
      this.subscriptions.push({ topic, queue, event, retry: fields.retry, reserveFor: fields.reserve_for });
    }

    const fetches = topics.length > 0 || fields.queue !== undefined;

    if (fetches && !this.options.queues.includes(queue)) {
      this.options.queues = [...this.options.queues, queue];
    }

    return this;
  }

  /** The first middleware added is the outermost. */
  use(middleware: Middleware): this {
    this.middleware.push(middleware);

    return this;
  }

  /**
   * Resolves after a stop. After quiet, the worker stops fetching and stays
   * alive. At a stop, running jobs get shutdownTimeout to finish. Then their
   * signals abort, and they get cancelGrace to return. The worker then fails
   * jobs that still run. A late result from these handlers is ignored.
   */
  async run(options: RunOptions = {}): Promise<void> {
    const run = new WorkerRun(this.options, this.handlers, this.middleware, [...this.subscriptions]);

    await run.start(options);
  }
}

/**
 * One run of a worker. A slot is one fetch-and-run loop. concurrency is the
 * count that the worker reports. While slots wait to stop after their job,
 * the live slots are concurrency plus retiring.
 */
class WorkerRun {
  private readonly client: Client;
  private readonly jobs = new AbortController();
  private readonly fetching = new AbortController();
  private readonly stopRequest = deferred();
  private readonly running = new Map<string, Job>();
  private readonly slots = new Set<Promise<void>>();
  private concurrency: number;
  private retiring = 0;
  private state: string | undefined;

  constructor(
    private readonly options: Worker['options'],
    private readonly handlers: Map<string, Handler>,
    private readonly middleware: Middleware[],
    private readonly subscriptions: Subscription[]
  ) {
    this.concurrency = options.concurrency;

    this.client = new Client({
      url: options.url,
      tls: options.tls,
      timeout: options.timeout,
      wid: randomBytes(12).toString('hex'),
      labels: ['js', ...(options.labels ?? [])],
      poolSize: options.concurrency + 1,
      identity: {
        hostname: options.hostname || hostname(),
        tag: options.tag,
        queues: options.queues,
        concurrency: options.concurrency,
        autoscale: options.autoscale,
        maxConcurrency: options.maxConcurrency,
      },
    });
  }

  /**
   * The first BEAT reports the slots, so the web UI can scale the worker at
   * once. The worker obeys its reply when the slots run, so a saved count
   * applies at once, not at the next beat.
   */
  async start({ signal, handleSignals = true }: RunOptions): Promise<void> {
    const stop = () => this.stopRequest.resolve();

    try {
      const first = await this.client.heartbeat(undefined, { concurrency: this.concurrency });

      for (const subscription of this.subscriptions) {
        await this.client.subscribe(subscription);
      }

      if (handleSignals) {
        process.once('SIGTERM', stop);
        process.once('SIGINT', stop);
      }

      if (signal?.aborted) stop();

      signal?.addEventListener('abort', stop, { once: true });

      for (let slot = 0; slot < this.concurrency; slot++) this.startSlot();

      this.beatLoop();

      await this.obey(first);

      await this.stopRequest.promise;
      await this.shutdown();
    } finally {
      process.off('SIGTERM', stop);
      process.off('SIGINT', stop);
      signal?.removeEventListener('abort', stop);

      this.fetching.abort();
      this.client.close();
    }
  }

  private async beatLoop(): Promise<void> {
    const ended = this.stopRequest.promise;

    while (true) {
      const timer = sleep(this.options.beatInterval, 'beat', { ref: false }).catch(() => 'beat');

      if ((await Promise.race([timer, ended])) !== 'beat') return;

      await this.beat();
    }
  }

  private async beat(): Promise<void> {
    try {
      await this.obey(await this.client.heartbeat(this.state, { concurrency: this.concurrency }));
    } catch (error) {
      this.options.logger.warn(`beat failed: ${String(error)}`);
    }
  }

  /**
   * Reads the count and the state of a reply apart, because a reply can have
   * both. A new count goes to the server at once, not at the next beat, so the
   * web UI shows the slots that run, and the next autoscale step starts from
   * them. The server does not run autoscale on this BEAT.
   */
  private async obey(reply: Heartbeat): Promise<void> {
    if (reply.concurrency !== null && this.resize(reply.concurrency)) {
      try {
        await this.client.heartbeat(this.state, { concurrency: this.concurrency });
      } catch (error) {
        this.options.logger.warn(`beat failed: ${String(error)}`);
      }
    }

    if (reply.state === 'quiet') {
      this.state = reply.state;
      this.fetching.abort();
    }

    if (reply.state === 'terminate') this.stopRequest.resolve();
  }

  /**
   * Starts slots, or asks slots to stop after their job. A stop that is still
   * pending is canceled first, so the slots never go above the count. The
   * count never goes above maxConcurrency. Returns true when the count changed.
   */
  private resize(wanted: number): boolean {
    if (!Number.isInteger(wanted) || wanted < 1 || this.fetching.signal.aborted) return false;

    const size = Math.min(wanted, this.options.maxConcurrency ?? wanted);
    const change = size - this.concurrency;

    if (change === 0) return false;

    if (change > 0) {
      const kept = Math.min(this.retiring, change);

      this.retiring -= kept;
      this.client.growPool(size + 1);

      for (let slot = kept; slot < change; slot++) this.startSlot();
    } else {
      this.retiring -= change;
    }

    this.concurrency = size;

    return true;
  }

  private startSlot(): void {
    const slot: Promise<void> = this.fetchLoop().finally(() => this.slots.delete(slot));

    this.slots.add(slot);
  }

  /** A slot that must stop takes one pending stop. */
  private retire(): boolean {
    if (this.retiring === 0) return false;

    this.retiring--;

    return true;
  }

  /** A fetch in progress is not canceled, because its reply can hold a job. */
  private async fetchLoop(): Promise<void> {
    while (!this.fetching.signal.aborted && !this.retire()) {
      const started = Date.now();

      try {
        const job = await this.client.fetch(...this.options.queues);

        if (job) await this.process(job);
        else if (Date.now() - started < EMPTY_FETCH_MINIMUM) await this.pause(EMPTY_FETCH_MINIMUM);
      } catch (error) {
        this.options.logger.warn(`fetch failed: ${String(error)}`);

        await this.pause(ERROR_PAUSE);
      }
    }
  }

  private async pause(milliseconds: number): Promise<void> {
    await sleep(milliseconds, undefined, { signal: this.fetching.signal }).catch(() => undefined);
  }

  private async process(job: Job): Promise<void> {
    this.running.set(job.jid, job);

    let failure: unknown;

    try {
      await this.invoke(job);
    } catch (error) {
      failure = error ?? new Error('job failed');
    }

    if (this.running.delete(job.jid)) await this.report(job, failure);
  }

  private invoke(job: Job): Promise<unknown> {
    const handler = this.handlers.get(job.type);

    if (!handler) throw new UnknownJobError(`no handler for job type ${JSON.stringify(job.type)}`);

    const context = { signal: this.jobs.signal };
    const base = async () => handler(job, context);

    const chain = this.middleware.reduceRight<() => Promise<unknown>>(
      (next, middleware) => () => middleware(job, next),
      base
    );

    return chain();
  }

  private async report(job: Job, failure: unknown): Promise<void> {
    try {
      if (failure === undefined) {
        await this.client.ack(job.jid);
      } else {
        const { type, message, backtrace } = describe(failure);

        await this.client.fail(job.jid, type, message, backtrace);
      }
    } catch (error) {
      this.options.logger.warn(`job report failed, the reservation will expire: ${job.jid}: ${String(error)}`);
    }
  }

  /** No slot starts after fetching stops, so the slots of now are all of them. */
  private async shutdown(): Promise<void> {
    this.fetching.abort();

    const finished = Promise.all(this.slots);

    if (await this.settled(finished, this.options.shutdownTimeout)) return;

    this.jobs.abort();

    if (await this.settled(finished, this.options.cancelGrace)) return;

    for (const job of [...this.running.values()]) {
      this.running.delete(job.jid);
      this.options.logger.warn(`job did not stop: ${job.jid} ${job.type}`);

      await this.report(job, new Error('the worker stopped before the job finished'));
    }
  }

  private async settled(finished: Promise<unknown>, limit: number): Promise<boolean> {
    const timer = new AbortController();
    const done = finished.then(() => true);
    const expired = sleep(limit, false, { signal: timer.signal }).catch(() => false);
    const result = await Promise.race([done, expired]);

    timer.abort();

    return result;
  }
}
