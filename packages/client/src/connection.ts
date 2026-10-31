import { createHash } from 'node:crypto';
import { connect as connectTcp, type Socket } from 'node:net';
import { hostname } from 'node:os';
import { connect as connectTls, type ConnectionOptions as TlsOptions } from 'node:tls';

import { AnwayError, ConnectionError, NotUniqueError, ServerError } from './errors';

const PROTOCOL_VERSION = 2;

export interface Endpoint {
  host: string;
  port: number;
  password: string;
  tls: boolean;
}

export interface ConnectionSettings {
  timeout: number;
  tls?: TlsOptions;
  wid?: string;
  labels?: string[];
}

function passwordHash(password: string, salt: string, iterations: number): string {
  let digest = createHash('sha256')
    .update(password + salt)
    .digest();

  for (let i = 1; i < iterations; i++) {
    digest = createHash('sha256').update(digest).digest();
  }

  return digest.toString('hex');
}

function parseError(body: string): AnwayError {
  const [code = 'ERR', ...rest] = body.split(' ');

  if (code === 'NOTUNIQUE') return new NotUniqueError();

  return new ServerError(code, rest.join(' '));
}

/** One socket. Only one command runs at a time, and the pool enforces this. */
export class Connection {
  private buffer: Buffer = Buffer.alloc(0);
  private notify?: () => void;
  private failure?: ConnectionError;
  private readonly socket: Socket;
  private readonly timeout: number;

  static async open(endpoint: Endpoint, settings: ConnectionSettings): Promise<Connection> {
    const connection = new Connection(endpoint, settings);

    try {
      await connection.handshake(endpoint, settings);
    } catch (error) {
      connection.destroy();

      throw error;
    }

    return connection;
  }

  private constructor(endpoint: Endpoint, settings: ConnectionSettings) {
    this.timeout = settings.timeout;
    this.socket = this.openSocket(endpoint, settings);

    this.socket.on('data', (chunk: Buffer) => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this.wake();
    });

    this.socket.on('error', (error: Error) => this.fail(error.message));
    this.socket.on('close', () => this.fail('server closed the connection'));
  }

  get closed(): boolean {
    return this.failure !== undefined;
  }

  async command(verb: string, argument?: unknown, wait = 0): Promise<string | null> {
    const deadline = Date.now() + this.timeout + wait;
    const text = typeof argument === 'string' ? argument : JSON.stringify(argument);
    const line = argument === undefined ? verb : `${verb} ${text}`;

    this.socket.write(`${line}\r\n`);

    return this.readReply(deadline);
  }

  destroy(): void {
    this.socket.destroy();
    this.fail('connection closed');
  }

  private openSocket(endpoint: Endpoint, settings: ConnectionSettings): Socket {
    const { host, port } = endpoint;

    if (!endpoint.tls) return connectTcp({ host, port });

    return connectTls({ host, port, servername: host, ...settings.tls });
  }

  private async handshake(endpoint: Endpoint, settings: ConnectionSettings): Promise<void> {
    const reply = await this.readReply(Date.now() + this.timeout);
    const greeting = JSON.parse((reply ?? '').replace(/^HI /, '')) as { v: number; s?: string; i?: number };

    if (greeting.v !== PROTOCOL_VERSION) {
      throw new AnwayError(`unsupported protocol version ${greeting.v}`);
    }

    const hello: Record<string, unknown> = { v: PROTOCOL_VERSION };

    if (greeting.s) hello.pwdhash = passwordHash(endpoint.password, greeting.s, greeting.i ?? 1);

    if (settings.wid) {
      hello.wid = settings.wid;
      hello.hostname = hostname();
      hello.pid = process.pid;
      hello.labels = settings.labels ?? [];
    }

    await this.command('HELLO', hello);
  }

  private async readReply(deadline: number): Promise<string | null> {
    const line = await this.readLine(deadline);

    if (line.length === 0) throw new ConnectionError('empty reply');

    const kind = line[0];
    const body = line.slice(1);

    switch (kind) {
      case '+':
        return body;
      case '-':
        throw parseError(body);
      case '$':
        return this.readBulk(Number(body), deadline);
      default:
        throw new ConnectionError(`unexpected reply: ${line}`);
    }
  }

  private async readLine(deadline: number): Promise<string> {
    let index = this.buffer.indexOf('\n');

    while (index === -1) {
      await this.waitForData(deadline);

      index = this.buffer.indexOf('\n');
    }

    const line = this.buffer.subarray(0, index).toString('utf8').replace(/\r$/, '');

    this.buffer = this.buffer.subarray(index + 1);

    return line;
  }

  private async readBulk(size: number, deadline: number): Promise<string | null> {
    if (size < 0) return null;

    while (this.buffer.length < size + 2) {
      await this.waitForData(deadline);
    }

    const body = this.buffer.subarray(0, size).toString('utf8');

    this.buffer = this.buffer.subarray(size + 2);

    return body;
  }

  private waitForData(deadline: number): Promise<void> {
    if (this.failure) return Promise.reject(this.failure);

    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => {
          this.notify = undefined;

          this.destroy();
          reject(new ConnectionError('timeout'));
        },
        Math.max(deadline - Date.now(), 0)
      );

      this.notify = () => {
        clearTimeout(timer);

        this.notify = undefined;

        if (this.failure) reject(this.failure);
        else resolve();
      };
    });
  }

  private wake(): void {
    this.notify?.();
  }

  private fail(message: string): void {
    this.failure ??= new ConnectionError(message);

    this.wake();
  }
}
