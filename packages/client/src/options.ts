import type { JsonValue } from './job';

/** When the job dies, the server pushes a job with this event to this queue. */
export interface OnDead {
  event: string;
  queue?: string;
}

/**
 * The input contract: the same keys in every Anway client. See "Input contract"
 * in docs/dev/clients.md of thadeu/anway.
 */
export interface Options {
  /** The default is "default". */
  queue?: string;
  /** The server default is 25. 0 discards at the first failure. -1 goes to the dead set at once. */
  retry?: number;
  /** Seconds. */
  reserveFor?: number;
  /** Lines. */
  backtrace?: number;
  /** A time to run later. For a push only. */
  at?: Date;
  /** Seconds. */
  uniqueFor?: number;
  uniqueUntil?: 'success' | 'start';
  /** Seconds. */
  expiresIn?: number;
  expiresAt?: Date;
  track?: boolean;
  onDead?: OnDead;
  /** The keys of the app. A contract key wins over a key with the same name here. */
  custom?: Record<string, JsonValue>;
  /** One SUBSCRIBE for each topic. For a declaration only. */
  topics?: string[];
}

/** The fields of a job on the wire. */
export interface JobFields {
  queue?: string;
  retry?: number;
  reserve_for?: number;
  backtrace?: number;
  at?: string;
  custom?: Record<string, JsonValue>;
}

export interface Translation {
  fields: JobFields;
  /** The topics of all sets, in order, with no duplicates. */
  topics: string[];
}

type Single = Exclude<keyof Options, 'custom' | 'topics'>;

const SINGLE_KEYS: Single[] = [
  'queue',
  'retry',
  'reserveFor',
  'backtrace',
  'at',
  'uniqueFor',
  'uniqueUntil',
  'expiresIn',
  'expiresAt',
  'track',
  'onDead',
];

/**
 * Merges option sets in order and turns them into the wire fields of a job.
 * A single value from a later set wins. The topics add up with no duplicates.
 * The custom objects merge, and a later key wins. The contract keys that go
 * to custom win over a custom key with the same name.
 */
export function translateOptions(...sets: Options[]): Translation {
  const merged: Partial<Record<Single, unknown>> = {};
  const topics: string[] = [];

  let custom: Record<string, JsonValue> | undefined;

  for (const set of sets) {
    for (const key of SINGLE_KEYS) {
      if (set[key] !== undefined) merged[key] = set[key];
    }

    if (set.custom) custom = { ...custom, ...set.custom };

    for (const topic of set.topics ?? []) {
      if (!topics.includes(topic)) topics.push(topic);
    }
  }

  const options = merged as Options;
  const contract = customFields(options);

  if (Object.keys(contract).length > 0) custom = { ...custom, ...contract };

  if (options.track === false && custom) delete custom.track;

  const fields: JobFields = {
    queue: options.queue,
    retry: options.retry,
    reserve_for: options.reserveFor,
    backtrace: options.backtrace,
    at: options.at?.toISOString(),
    custom,
  };

  return { fields, topics };
}

/** The contract keys that go to custom, with their wire names. */
function customFields(options: Options): Record<string, JsonValue> {
  const fields: Record<string, JsonValue> = {};

  if (options.uniqueFor !== undefined) fields.unique_for = options.uniqueFor;

  if (options.uniqueUntil !== undefined) fields.unique_until = options.uniqueUntil;

  if (options.expiresIn !== undefined) fields.expires_in = options.expiresIn;

  if (options.expiresAt !== undefined) fields.expires_at = options.expiresAt.toISOString();

  if (options.track) fields.track = 1;

  if (options.onDead) {
    const { event, queue } = options.onDead;

    fields.on_dead = queue === undefined ? { jobtype: event } : { jobtype: event, queue };
  }

  return fields;
}
