import { execFile, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { promisify } from "node:util";

export type CodexRequestId = string | number;
export interface CodexMessage {
  readonly method: string;
  readonly params: unknown;
  readonly id?: CodexRequestId;
}

export interface CodexClient {
  readonly pid: number;
  request(method: string, params: unknown): Promise<unknown>;
  notify(method: string, params?: unknown): Promise<void>;
  respond(id: CodexRequestId, result: unknown): Promise<void>;
  reject(id: CodexRequestId, message: string): Promise<void>;
  close(): Promise<void>;
}

export function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("codex-invalid-response");
  }
  return value as Record<string, unknown>;
}

export function text(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("codex-invalid-response");
  }
  return value;
}

export async function startCodexClient(
  executable: string,
  onMessage: (message: CodexMessage) => void,
  onClose: () => void,
): Promise<CodexClient> {
  const { stdout } = await promisify(execFile)(executable, ["--version"], {
    windowsHide: true,
    timeout: 15_000,
    maxBuffer: 4096,
  });
  if (stdout.trim() !== "codex-cli 0.163.0") {
    throw new Error("codex-unsupported-version:expected-0.163.0");
  }
  const child = spawn(executable, ["app-server", "--listen", "stdio://"], {
    stdio: ["pipe", "pipe", "pipe"],
    windowsHide: true,
  });
  return new StdioCodexClient(child, onMessage, onClose);
}

export class StdioCodexClient implements CodexClient {
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #pending = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof setTimeout>;
  }>();
  readonly #onClose: () => void;
  #nextId = 0;
  #closed = false;
  #closing = false;
  #buffer = Buffer.alloc(0);
  readonly #exited: Promise<void>;

  constructor(
    child: ChildProcessWithoutNullStreams,
    onMessage: (message: CodexMessage) => void,
    onClose: () => void,
  ) {
    this.#child = child;
    this.#onClose = onClose;
    this.#exited = new Promise((resolve) => child.once("close", resolve));
    child.on("error", () => this.#fail("codex-process-error"));
    child.stdin.on("error", () => this.#fail("codex-write-error"));
    child.stderr.resume();
    child.once("exit", () => this.#fail("codex-disconnected"));
    child.stdout.on("data", (chunk: Buffer) => {
      try {
        this.#buffer = Buffer.concat([this.#buffer, chunk]);
        for (;;) {
          const index = this.#buffer.indexOf(10);
          if (index < 0) {
            break;
          }
          if (index > 32 * 1024 * 1024) {
            throw new Error("codex-frame-too-large");
          }
          const line = this.#buffer.subarray(0, index).toString("utf8").trim();
          this.#buffer = this.#buffer.subarray(index + 1);
          if (!line) {
            continue;
          }
          const message = record(JSON.parse(line));
          if (typeof message.method === "string") {
            if (message.id !== undefined &&
                typeof message.id !== "string" && typeof message.id !== "number") {
              throw new Error("codex-invalid-request-id");
            }
            onMessage({
              method: message.method,
              params: message.params,
              ...(message.id === undefined ? {} : { id: message.id }),
            });
          } else if (typeof message.id === "number") {
            const pending = this.#pending.get(message.id);
            if (pending) {
              const error = message.error === undefined ? undefined : record(message.error);
              this.#pending.delete(message.id);
              clearTimeout(pending.timer);
              if (error !== undefined) {
                const code = error.code;
                pending.reject(new Error(`codex-rpc-error:${typeof code === "number" && Number.isFinite(code) ? code : "invalid-code"}`));
              } else if ("result" in message) {
                pending.resolve(message.result);
              } else {
                pending.reject(new Error("codex-invalid-response"));
              }
            }
          } else {
            throw new Error("codex-invalid-message");
          }
        }
        if (this.#buffer.length > 32 * 1024 * 1024) {
          throw new Error("codex-frame-too-large");
        }
      } catch {
        this.#fail("codex-invalid-frame");
        child.kill();
      }
    });
  }

  get pid(): number {
    return this.#child.pid ?? 0;
  }

  request(method: string, params: unknown): Promise<unknown> {
    if (this.#closed) {
      return Promise.reject(new Error("codex-disconnected"));
    }
    const id = ++this.#nextId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(id);
        reject(new Error("codex-request-timeout"));
        // A timed-out mutation has an unknown outcome and must never be replayed.
        this.#fail("codex-request-timeout");
        this.#child.kill();
      }, 30_000);
      this.#pending.set(id, { resolve, reject, timer });
      void this.#write({ id, method, params }).catch(() => {
        this.#fail("codex-write-error");
      });
    });
  }

  respond(id: CodexRequestId, result: unknown): Promise<void> {
    return this.#write({ id, result });
  }

  notify(method: string, params?: unknown): Promise<void> {
    return this.#write({ method, ...(params === undefined ? {} : { params }) });
  }

  reject(id: CodexRequestId, message: string): Promise<void> {
    return this.#write({ id, error: { code: -32601, message } });
  }

  async close(): Promise<void> {
    this.#closing = true;
    this.#child.stdin.end();
    const timer = setTimeout(() => this.#child.kill(), 3_000);
    try {
      await this.#exited;
    } finally {
      clearTimeout(timer);
      this.#fail("codex-closed");
    }
  }

  #write(message: unknown): Promise<void> {
    if (this.#closed) {
      return Promise.reject(new Error("codex-disconnected"));
    }
    return new Promise((resolve, reject) => {
      this.#child.stdin.write(`${JSON.stringify(message)}\n`, (error) => {
        if (error) {
          reject(new Error("codex-write-error"));
        } else {
          resolve();
        }
      });
    });
  }

  #fail(code: string): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(new Error(code));
    }
    this.#pending.clear();
    if (!this.#closing) {
      this.#onClose();
    }
  }
}
