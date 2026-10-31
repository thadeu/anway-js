import { randomBytes } from 'node:crypto';
import type { ConnectionOptions as TlsOptions } from 'node:tls';
import { setTimeout as sleep } from 'node:timers/promises';

import { Client } from './client';
import { UnknownJobError } from './errors';
import type { Job } from './job';

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
  /** Jobs that run at the same time. These are promises, not threads. */
  concurrency?: number;
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
  }

  register<Args extends unknown[] = unknown[]>(jobType: string, handler: Handler<Args>): this {
    this.handlers.set(jobType, handler as Handler);

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
    const run = new WorkerRun(this.options, this.handlers, this.middleware);

    await run.start(options);
  }
}

class WorkerRun {
  private readonly client: Client;
  private readonly jobs = new AbortController();
  private readonly fetching = new AbortController();
  private readonly stopRequest = deferred();
  private readonly running = new Map<string, Job>();
  private state: string | undefined;

  constructor(
    private readonly options: Worker['options'],
    private readonly handlers: Map<string, Handler>,
    private readonly middleware: Middleware[]
  ) {
    this.client = new Client({
      url: options.url,
      tls: options.tls,
      timeout: options.timeout,
      wid: randomBytes(12).toString('hex'),
      labels: ['js', ...(options.labels ?? [])],
      poolSize: options.concurrency + 1,
    });
  }

  async start({ signal, handleSignals = true }: RunOptions): Promise<void> {
    const stop = () => this.stopRequest.resolve();

    try {
      await this.client.beat();

      if (handleSignals) {
        process.once('SIGTERM', stop);
        process.once('SIGINT', stop);
      }

      if (signal?.aborted) stop();

      signal?.addEventListener('abort', stop, { once: true });

      const loops = Array.from({ length: this.options.concurrency }, () => this.fetchLoop());
      const finished = Promise.all(loops);

      this.beatLoop();

      await this.stopRequest.promise;
      await this.shutdown(finished);
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
      const next = await this.client.beat(this.state);

      if (next === 'quiet') {
        this.state = next;
        this.fetching.abort();
      }

      if (next === 'terminate') this.stopRequest.resolve();
    } catch (error) {
      this.options.logger.warn(`beat failed: ${String(error)}`);
    }
  }

  /** A fetch in progress is not canceled, because its reply can hold a job. */
  private async fetchLoop(): Promise<void> {
    while (!this.fetching.signal.aborted) {
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

  private async shutdown(finished: Promise<unknown>): Promise<void> {
    this.fetching.abort();

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
