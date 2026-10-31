export class AnwayError extends Error {
  constructor(message: string) {
    super(message);

    this.name = new.target.name;
  }
}

export class NotUniqueError extends AnwayError {
  constructor() {
    super('job not unique');
  }
}

export class ClosedError extends AnwayError {
  constructor() {
    super('client is closed');
  }
}

export class ConnectionError extends AnwayError {}

export class UnknownJobError extends AnwayError {}

export class ServerError extends AnwayError {
  readonly code: string;

  constructor(code: string, message: string) {
    super(`${code} ${message}`);

    this.code = code;
  }
}
