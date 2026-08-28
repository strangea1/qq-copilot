import net from "node:net";
import { randomUUID } from "node:crypto";

const IPC_VERSION = 1;
const MAX_MESSAGE_BYTES = 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 30_000;

interface RpcResponse {
  readonly version: number;
  readonly request_id: string;
  readonly result?: unknown;
  readonly error?: {
    readonly code: string;
    readonly message: string;
  };
}

export type BridgeRequest = Readonly<Record<string, unknown>> & {
  readonly operation: string;
};

export class BridgeRpcError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "BridgeRpcError";
    this.code = code;
  }
}

export class BridgeClient {
  readonly #pipePath: string;

  readonly #token: string;

  constructor(pipePath: string, token: string) {
    this.#pipePath = pipePath;
    this.#token = token;
  }

  call<T>(
    request: BridgeRequest,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  ): Promise<T> {
    const requestId = randomUUID();
    const message = Buffer.from(
      `${JSON.stringify({
        version: IPC_VERSION,
        request_id: requestId,
        auth_token: this.#token,
        request,
      })}\n`,
      "utf8",
    );
    if (message.byteLength > MAX_MESSAGE_BYTES) {
      return Promise.reject(new Error("Bridge request exceeds IPC size limit"));
    }

    return new Promise<T>((resolve, reject) => {
      const socket = net.createConnection(this.#pipePath);
      const chunks: Buffer[] = [];
      let receivedBytes = 0;
      let settled = false;
      const timer = setTimeout(() => {
        finish(new Error("Bridge request timed out"));
      }, timeoutMs);
      timer.unref();

      const finish = (error?: Error, value?: T): void => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        socket.destroy();
        if (error) {
          reject(error);
        } else {
          resolve(value as T);
        }
      };

      socket.once("connect", () => {
        socket.write(message);
      });
      socket.on("data", (chunk: Buffer) => {
        receivedBytes += chunk.byteLength;
        if (receivedBytes > MAX_MESSAGE_BYTES) {
          finish(new Error("Bridge response exceeds IPC size limit"));
          return;
        }
        chunks.push(chunk);
        const combined = Buffer.concat(chunks);
        const newline = combined.indexOf(0x0a);
        if (newline < 0) {
          return;
        }
        try {
          const response = JSON.parse(
            combined.subarray(0, newline).toString("utf8"),
          ) as RpcResponse;
          if (
            response.version !== IPC_VERSION ||
            response.request_id !== requestId
          ) {
            finish(new Error("Bridge response identity did not match"));
            return;
          }
          if (response.error) {
            finish(
              new BridgeRpcError(
                response.error.code,
                response.error.message,
              ),
            );
            return;
          }
          if (!("result" in response)) {
            finish(new Error("Bridge response omitted result"));
            return;
          }
          finish(undefined, response.result as T);
        } catch (error) {
          finish(
            new Error("Bridge response was invalid JSON", { cause: error }),
          );
        }
      });
      socket.once("error", () => {
        finish(new Error("Bridge IPC connection failed"));
      });
      socket.once("end", () => {
        if (!settled) {
          finish(new Error("Bridge closed without a response"));
        }
      });
    });
  }
}
