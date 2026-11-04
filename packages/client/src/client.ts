import type { ConnectionOptions as TlsOptions } from 'node:tls';

import { Connection, type Endpoint, type WorkerIdentity } from './connection';
import { Job, validateJson, type JobPayload, type JsonValue } from './job';
import { Pool } from './pool';

const DEFAULT_URL = 'tcp://127.0.0.1:7419';
const DEFAULT_PORT = 7419;
const FETCH_WAIT = 2_000;
const MAX_BULK = 1_000;

/** The argument of PUBLISH, and of each event of PUBLISHB. */
function eventBody(topic: string, args: unknown[], options: PublishOptions) {
  validateJson(args);

  return {
    topic,
    args,
    id: options.id,
    unique_for: options.uniqueFor,
    custom: options.custom,
  };
}

export interface ClientOptions {
  /** A blank URL uses ANWAY_URL, then the local default. */
  url?: string;
  poolSize?: number;
  /** Milliseconds. */
  timeout?: number;
  tls?: TlsOptions;
  /** Identifies a worker. It is required for fetch and beat. */
  wid?: string;
  labels?: string[];
  /** More HELLO fields of a worker. Used only with wid. */
  identity?: WorkerIdentity;
}

/** The reply to a BEAT. A field is null when the reply does not have it. */
export interface Heartbeat {
  /** "quiet" or "terminate". */
  state: string | null;
  /** The number of jobs that the server wants the worker to run at once. */
  concurrency: number | null;
}

/** Selects jobs for mutate. Blank fields match all jobs. */
export interface Filter {
  /** The event of the jobs. It is the jobtype field on the wire. */
  event?: string;
  jids?: string[];
  regexp?: string;
}

export type MutateCommand = 'kill' | 'discard' | 'requeue' | 'clear';

export type MutateTarget = 'retries' | 'scheduled' | 'dead';

export interface Batch {
  parentBid?: string;
  description?: string;
  success?: Job;
  complete?: Job;
}

export interface BatchStatus {
  bid: string;
  parent_bid?: string;
  description?: string;
  created_at: string;
  complete_st: string;
  success_st: string;
  total: number;
  pending: number;
  failed: number;
}

export interface Track {
  jid: string;
  state: string;
  updated_at?: string;
  percent?: number;
  desc?: string;
}

export interface TrackUpdate {
  jid: string;
  percent?: number;
  desc?: string;
  reserveUntil?: Date;
}

/** The reply to publish. */
export interface Published {
  /** The event id: the given id, or an id that the server made. */
  event: string;
  /** The number of copies, one for each subscription. */
  jobs: number;
  /** True when the same event id was published within uniqueFor. */
  duplicate: boolean;
}

export interface PublishOptions {
  /** The event id. A second publish of the same topic and id within uniqueFor does nothing. */
  id?: string;
  /** Seconds. The server default is 86,400 when id is set. 0 turns the check off. */
  uniqueFor?: number;
  /** Goes to each copy. The server adds topic and event. */
  custom?: Record<string, JsonValue>;
}

/** One event of publishBulk: a topic, its args, and the options of publish. */
export interface BulkEvent extends PublishOptions {
  topic: string;
  args?: unknown[];
}

/** One result of publishBulk. error is the reason when the server refused the event. */
export interface PublishResult extends Published {
  error?: string;
}

/** Sends the events of a topic to a queue, as jobs of one event. */
export interface Subscription {
  topic: string;
  queue: string;
  /** The event of the copies. It is the jobtype field on the wire. */
  event: string;
  retry?: number;
  /** Seconds. */
  reserveFor?: number;
}

// Server data. The faktory hash has this name in the public wire format.
export interface Info {
  server?: Record<string, unknown>;
  faktory: {
    queues?: Record<string, number>;
    total_processed: number;
    total_failures: number;
    tasks: Record<string, { size: number }>;
  };
}

function parseUrl(raw: string): Endpoint {
  const url = new URL(raw);

  if (url.protocol !== 'tcp:' && url.protocol !== 'tcp+tls:') {
    throw new TypeError(`unsupported scheme: ${url.protocol}`);
  }

  return {
    host: url.hostname,
    port: url.port ? Number(url.port) : DEFAULT_PORT,
    password: decodeURIComponent(url.password),
    tls: url.protocol === 'tcp+tls:',
  };
}

/** Same operations as the anway-go client. One client works for many callers. */
export class Client {
  private readonly pool: Pool;
  private readonly wid?: string;

  constructor(options: ClientOptions = {}) {
    const endpoint = parseUrl(options.url || process.env.ANWAY_URL || DEFAULT_URL);
    const timeout = options.timeout ?? 10_000;

    this.wid = options.wid;

    this.pool = new Pool(
      options.poolSize ?? 8,
      () =>
        Connection.open(endpoint, {
          timeout,
          tls: options.tls,
          wid: options.wid,
          labels: options.labels,
          identity: options.identity,
        }),
      timeout
    );
  }

  close(): void {
    this.pool.close();
  }

  /** Rejects with NotUniqueError when a unique lock blocks the job. */
  async push(job: Job): Promise<string> {
    await this.call('PUSH', job);

    return job.jid;
  }

  /**
   * Pushes up to 1,000 jobs with one write. Returns the reason for each job
   * that is not in, by jid, or {} when all are in. A refused job, such as a
   * duplicate jid or a unique lock, does not stop the others. More than 1,000
   * jobs reject with a RangeError, and nothing goes to the server.
   */
  async pushBulk(jobs: Job[]): Promise<Record<string, string>> {
    if (jobs.length > MAX_BULK) {
      throw new RangeError(`a bulk push has at most ${MAX_BULK} jobs, not ${jobs.length}`);
    }

    if (jobs.length === 0) return {};

    return JSON.parse((await this.call('PUSHB', jobs)) ?? '{}');
  }

  /** Returns the next job, or null when no job arrives in about 2 seconds. */
  async fetch<Args extends unknown[] = unknown[]>(...queues: string[]): Promise<Job<Args> | null> {
    const reply = await this.call('FETCH', queues.join(' '), FETCH_WAIT);

    return reply ? Job.fromPayload<Args>(JSON.parse(reply) as JobPayload) : null;
  }

  async ack(jid: string): Promise<void> {
    await this.call('ACK', { jid });
  }

  async fail(jid: string, errType: string, message: string, backtrace: string[] = []): Promise<void> {
    await this.call('FAIL', { jid, errtype: errType, message, backtrace });
  }

  /** Returns null, "quiet", or "terminate". */
  async beat(currentState?: string): Promise<string | null> {
    return (await this.heartbeat(currentState)).state;
  }

  /**
   * Sends BEAT. A worker that sends its concurrency can get another count
   * in the reply. The reply can have a state, a count, or both.
   */
  async heartbeat(currentState?: string, options: { concurrency?: number } = {}): Promise<Heartbeat> {
    const body = { wid: this.wid, current_state: currentState, concurrency: options.concurrency };
    const reply = await this.call('BEAT', body);

    if (reply === 'OK' || reply === null) return { state: null, concurrency: null };

    const data = JSON.parse(reply) as { state?: string; concurrency?: number };

    return { state: data.state ?? null, concurrency: data.concurrency ?? null };
  }

  /** Lets the client hold size connections at once, for a worker that adds job slots. */
  growPool(size: number): void {
    this.pool.grow(size);
  }

  async info(): Promise<Info> {
    return JSON.parse((await this.call('INFO')) ?? '{}');
  }

  async queueSizes(): Promise<Record<string, number>> {
    const info = await this.info();

    return info.faktory.queues ?? {};
  }

  async pauseQueues(...queues: string[]): Promise<void> {
    await this.call('QUEUE', `PAUSE ${queues.join(' ')}`);
  }

  async resumeQueues(...queues: string[]): Promise<void> {
    await this.call('QUEUE', `RESUME ${queues.join(' ')}`);
  }

  async removeQueues(...queues: string[]): Promise<void> {
    await this.call('QUEUE', `REMOVE ${queues.join(' ')}`);
  }

  async mutate(command: MutateCommand, target: MutateTarget, filter: Filter = {}): Promise<void> {
    const { event, ...rest } = filter;

    await this.call('MUTATE', { cmd: command, target, filter: { ...rest, jobtype: event } });
  }

  async batchNew(batch: Batch): Promise<string> {
    const definition = {
      parent_bid: batch.parentBid,
      description: batch.description,
      success: batch.success,
      complete: batch.complete,
    };

    return (await this.call('BATCH', `NEW ${JSON.stringify(definition)}`)) as string;
  }

  async batchOpen(bid: string): Promise<void> {
    await this.call('BATCH', `OPEN ${bid}`);
  }

  async batchCommit(bid: string): Promise<void> {
    await this.call('BATCH', `COMMIT ${bid}`);
  }

  async batchStatus(bid: string): Promise<BatchStatus> {
    return JSON.parse((await this.call('BATCH', `STATUS ${bid}`)) as string);
  }

  async trackGet(jid: string): Promise<Track> {
    return JSON.parse((await this.call('TRACK', `GET ${jid}`)) as string);
  }

  async trackSet(update: TrackUpdate): Promise<void> {
    const body = {
      jid: update.jid,
      percent: update.percent,
      desc: update.desc,
      reserve_until: update.reserveUntil?.toISOString(),
    };

    await this.call('TRACK', `SET ${JSON.stringify(body)}`);
  }

  /** Makes one job for each subscription of the topic. With no subscription, the event goes to the dead set. */
  async publish(topic: string, args: unknown[] = [], options: PublishOptions = {}): Promise<Published> {
    return JSON.parse((await this.call('PUBLISH', eventBody(topic, args, options))) as string);
  }

  /**
   * Publishes up to 1,000 events with one write. Each event works as one
   * publish. Returns one result for each event, in the same order. A refused
   * event, such as a bad topic, has error, and does not stop the others. More
   * than 1,000 events reject with a RangeError, and nothing goes to the server.
   */
  async publishBulk(events: BulkEvent[]): Promise<PublishResult[]> {
    if (events.length > MAX_BULK) {
      throw new RangeError(`a bulk publish has at most ${MAX_BULK} events, not ${events.length}`);
    }

    if (events.length === 0) return [];

    const body = events.map(({ topic, args = [], ...options }) => eventBody(topic, args, options));

    return JSON.parse((await this.call('PUBLISHB', body)) as string);
  }

  /** Idempotent. A second call sets the new retry and reserveFor. */
  async subscribe(subscription: Subscription): Promise<void> {
    const body = {
      topic: subscription.topic,
      queue: subscription.queue,
      jobtype: subscription.event,
      retry: subscription.retry,
      reserve_for: subscription.reserveFor,
    };

    await this.call('SUBSCRIBE', body);
  }

  /** A missing subscription is not an error. */
  async unsubscribe(subscription: Pick<Subscription, 'topic' | 'queue' | 'event'>): Promise<void> {
    const { topic, queue, event } = subscription;

    await this.call('UNSUBSCRIBE', { topic, queue, jobtype: event });
  }

  private call(verb: string, argument?: unknown, wait = 0): Promise<string | null> {
    return this.pool.use(connection => connection.command(verb, argument, wait));
  }
}
