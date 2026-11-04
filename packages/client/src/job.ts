import { randomBytes } from 'node:crypto';

import { translateOptions, type Options } from './options';

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export interface Failure {
  retry_count: number;
  failed_at: string;
  next_at?: string;
  message?: string;
  errtype?: string;
  backtrace?: string[];
}

export interface JobPayload {
  jid: string;
  jobtype: string;
  args: unknown[];
  queue?: string;
  reserve_for?: number;
  at?: string;
  retry?: number;
  backtrace?: number;
  created_at?: string;
  enqueued_at?: string;
  failure?: Failure;
  custom?: Record<string, JsonValue>;
}

export function validateJson(value: unknown): void {
  if (value === null) return;

  switch (typeof value) {
    case 'string':
    case 'boolean':
      return;
    case 'number':
      if (Number.isFinite(value)) return;

      break;
    case 'object':
      if (Array.isArray(value)) {
        value.forEach(validateJson);

        return;
      }

      if (Object.getPrototypeOf(value) === Object.prototype) {
        Object.values(value).forEach(validateJson);

        return;
      }

      break;
  }

  throw new TypeError(`job args must be JSON types: ${String(value)}`);
}

/** One unit of work. Other languages read the args, so only JSON types are valid. */
export class Job<Args extends unknown[] = unknown[]> {
  jid: string;
  /** The event of the job. It is the jobtype field on the wire. */
  type: string;
  args: Args;
  queue?: string;
  retry?: number;
  reserveFor?: number;
  backtrace?: number;
  at?: string;
  custom?: Record<string, JsonValue>;
  createdAt?: string;
  enqueuedAt?: string;
  failure?: Failure;

  static fromPayload<Args extends unknown[]>(payload: JobPayload): Job<Args> {
    const job = Object.create(Job.prototype) as Job<Args>;

    job.jid = payload.jid;
    job.type = payload.jobtype;
    job.args = (payload.args ?? []) as Args;
    job.queue = payload.queue;
    job.retry = payload.retry;
    job.reserveFor = payload.reserve_for;
    job.backtrace = payload.backtrace;
    job.at = payload.at;
    job.custom = payload.custom;
    job.createdAt = payload.created_at;
    job.enqueuedAt = payload.enqueued_at;
    job.failure = payload.failure;

    return job;
  }

  /** Takes the keys of the input contract. The topics key is for a declaration, so a push ignores it. */
  constructor(event: string, args?: Args, options: Options = {}) {
    const values = (args ?? []) as Args;

    validateJson(values);

    const { fields } = translateOptions(options);

    this.jid = randomBytes(12).toString('hex');
    this.type = event;
    this.args = values;
    this.queue = fields.queue;
    this.retry = fields.retry;
    this.reserveFor = fields.reserve_for;
    this.backtrace = fields.backtrace;
    this.at = fields.at;
    this.custom = fields.custom;
  }

  runAt(at: Date): this {
    this.at = at.toISOString();

    return this;
  }

  setCustom(key: string, value: JsonValue): this {
    this.custom = { ...this.custom, [key]: value };

    return this;
  }

  toJSON(): JobPayload {
    return {
      jid: this.jid,
      jobtype: this.type,
      args: this.args,
      queue: this.queue,
      reserve_for: this.reserveFor,
      at: this.at,
      retry: this.retry,
      backtrace: this.backtrace,
      created_at: this.createdAt,
      enqueued_at: this.enqueuedAt,
      failure: this.failure,
      custom: this.custom,
    };
  }
}
