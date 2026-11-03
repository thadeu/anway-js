import { createServer, type Server, type Socket } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';

import type { JobPayload } from '../../src/index';

/** Returns the state for a BEAT reply, such as "quiet" or "terminate". Undefined replies OK. */
export type BeatScript = (beat: number) => string | undefined;

export interface FailPayload {
  jid: string;
  errtype: string;
  message: string;
  backtrace?: string[];
}

/**
 * A fake server for states that a real server does not send in a test, such as
 * terminate. FETCH gives out the jobs in order, and then replies with no job.
 * The server records each ACK and FAIL.
 */
export class ScriptedServer {
  beats = 0;
  /** Fetches after the first beat. */
  quietFetches = 0;
  readonly jobs: JobPayload[] = [];
  readonly acks: string[] = [];
  readonly fails: FailPayload[] = [];
  private readonly server: Server;
  private readonly sockets = new Set<Socket>();

  constructor(private readonly script: BeatScript) {
    this.server = createServer(socket => this.accept(socket));
  }

  listen(): Promise<string> {
    return new Promise(resolve => {
      this.server.listen(0, '127.0.0.1', () => {
        const address = this.server.address() as { port: number };

        resolve(`tcp://127.0.0.1:${address.port}`);
      });
    });
  }

  close(): void {
    this.sockets.forEach(socket => socket.destroy());
    this.server.close();
  }

  private accept(socket: Socket): void {
    let buffered = '';

    this.sockets.add(socket);
    socket.on('error', () => {});
    socket.write('+HI {"v":2}\r\n');

    socket.on('data', async chunk => {
      buffered += chunk.toString();

      let index = buffered.indexOf('\n');

      while (index !== -1) {
        const line = buffered.slice(0, index).trim();
        const space = line.indexOf(' ');
        const verb = space === -1 ? line : line.slice(0, space);
        const argument = space === -1 ? '' : line.slice(space + 1);

        buffered = buffered.slice(index + 1);

        socket.write(await this.reply(verb, argument));

        index = buffered.indexOf('\n');
      }
    });
  }

  private async reply(verb: string, argument: string): Promise<string> {
    switch (verb) {
      case 'FETCH':
        return this.fetch();
      case 'BEAT':
        return this.beat();
      case 'ACK':
        this.acks.push((JSON.parse(argument) as { jid: string }).jid);

        return '+OK\r\n';
      case 'FAIL':
        this.fails.push(JSON.parse(argument) as FailPayload);

        return '+OK\r\n';
      default:
        return '+OK\r\n';
    }
  }

  private async fetch(): Promise<string> {
    if (this.beats > 1) this.quietFetches++;

    const job = this.jobs.shift();

    if (job) return bulk(JSON.stringify(job));

    await sleep(5);

    return '$-1\r\n';
  }

  private beat(): string {
    this.beats++;

    const state = this.script(this.beats);

    if (state === undefined) return '+OK\r\n';

    return bulk(JSON.stringify({ state }));
  }
}

function bulk(body: string): string {
  return `$${Buffer.byteLength(body)}\r\n${body}\r\n`;
}
