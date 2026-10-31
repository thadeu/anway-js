import { Connection } from './connection';
import { ClosedError, ConnectionError, NotUniqueError, ServerError } from './errors';

type Waiter = {
  resolve: (slot: Connection | null) => void;
  reject: (error: Error) => void;
};

/** Connections open at first use, and the newest idle connection goes first. */
export class Pool {
  private readonly slots: (Connection | null)[];
  private readonly waiters: Waiter[] = [];
  private isClosed = false;

  constructor(
    size: number,
    private readonly open: () => Promise<Connection>,
    private readonly timeout: number
  ) {
    this.slots = new Array<Connection | null>(size).fill(null);
  }

  async use<Result>(work: (connection: Connection) => Promise<Result>): Promise<Result> {
    let connection = await this.checkout();
    let healthy = false;

    try {
      if (!connection || connection.closed) connection = await this.open();

      const result = await work(connection);

      healthy = true;

      return result;
    } catch (error) {
      healthy = error instanceof ServerError || error instanceof NotUniqueError;

      throw error;
    } finally {
      this.checkin(connection, healthy);
    }
  }

  close(): void {
    this.isClosed = true;

    for (const connection of this.slots) connection?.destroy();

    this.slots.length = 0;

    for (const waiter of this.waiters.splice(0)) waiter.reject(new ClosedError());
  }

  private checkout(): Promise<Connection | null> {
    if (this.isClosed) return Promise.reject(new ClosedError());

    if (this.slots.length > 0) {
      const connected = this.slots.findLastIndex(slot => slot !== null);
      const index = connected === -1 ? this.slots.length - 1 : connected;
      const [slot] = this.slots.splice(index, 1);

      return Promise.resolve(slot ?? null);
    }

    return new Promise((resolve, reject) => {
      const waiter: Waiter = {
        resolve: slot => {
          clearTimeout(timer);
          resolve(slot);
        },
        reject: error => {
          clearTimeout(timer);
          reject(error);
        },
      };

      const timer = setTimeout(() => {
        this.waiters.splice(this.waiters.indexOf(waiter), 1);
        reject(new ConnectionError('no free connection'));
      }, this.timeout);

      this.waiters.push(waiter);
    });
  }

  private checkin(connection: Connection | null, healthy: boolean): void {
    if (!healthy) connection?.destroy();

    const slot = healthy ? connection : null;

    if (this.isClosed) {
      slot?.destroy();

      return;
    }

    const waiter = this.waiters.shift();

    if (waiter) waiter.resolve(slot);
    else this.slots.push(slot);
  }
}
