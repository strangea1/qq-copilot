import { createHmac, timingSafeEqual } from "node:crypto";
import {
  createServer,
  type Server,
  type Socket,
} from "node:net";

const BROKER_VERSION = 1;
const MAX_REQUEST_BYTES = 32 * 1024;
const MAX_TOKEN_BYTES = 64 * 1024;
const REQUEST_TIMEOUT_MS = 5 * 60 * 1_000;
const GITHUB_RESOURCE = "https://api.github.com";
const GITHUB_AUTHORIZATION_SERVER = "https://github.com/login/oauth";
const GITHUB_ALLOWED_SCOPES = new Set(["read:user", "user:email"]);

export interface AuthBrokerIdentity {
  readonly pipePath: string;
  readonly authorization: string;
}

export interface AuthBrokerRequest {
  readonly requestId: string;
  readonly provider: string;
  readonly resource: string;
  readonly authorizationServers: readonly string[];
  readonly scopes: readonly string[];
}

export interface BrokeredAuthentication {
  readonly accessToken: string;
}

export type ResolveAuthentication = (
  request: AuthBrokerRequest,
) => Promise<BrokeredAuthentication>;

export type ReportServerError = (error: Error) => void;

export class AuthBrokerError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "AuthBrokerError";
    this.code = code;
  }
}

export function bridgeIpcTokenFromToml(contents: string): string {
  let inBridge = false;
  for (const line of contents.split(/\r?\n/u)) {
    const section = /^\s*\[([^\]]+)\]\s*(?:#.*)?$/u.exec(line);
    if (section) {
      inBridge = section[1] === "bridge";
      continue;
    }
    if (!inBridge) {
      continue;
    }
    const token = /^\s*ipc_token\s*=\s*"([a-fA-F0-9]{64,})"\s*(?:#.*)?$/u.exec(
      line,
    );
    if (token?.[1]) {
      return token[1];
    }
  }
  throw new AuthBrokerError(
    "invalid_bridge_config",
    "Bridge IPC token is unavailable",
  );
}

export function deriveAuthBrokerIdentity(
  bridgeToken: string,
): AuthBrokerIdentity {
  if (!/^[a-fA-F0-9]{64,}$/u.test(bridgeToken)) {
    throw new AuthBrokerError(
      "invalid_bridge_config",
      "Bridge IPC token is invalid",
    );
  }
  const authorization = createHmac(
    "sha256",
    Buffer.from(bridgeToken, "hex"),
  )
    .update("qq-copilot-auth-broker-v1", "utf8")
    .digest("hex");
  return {
    pipePath: `\\\\.\\pipe\\qq-copilot-auth-${authorization.slice(0, 32)}`,
    authorization,
  };
}

export class AuthBrokerServer {
  readonly #identity: AuthBrokerIdentity;

  readonly #resolveAuthentication: ResolveAuthentication;

  readonly #reportServerError: ReportServerError;

  #server: Server | undefined;

  readonly #sockets = new Set<Socket>();

  constructor(
    identity: AuthBrokerIdentity,
    resolveAuthentication: ResolveAuthentication,
    reportServerError: ReportServerError,
  ) {
    this.#identity = identity;
    this.#resolveAuthentication = resolveAuthentication;
    this.#reportServerError = reportServerError;
  }

  async start(): Promise<boolean> {
    if (this.#server) {
      return true;
    }
    const server = createServer((socket) => this.#accept(socket));
    const listening = await new Promise<boolean>((resolve, reject) => {
      const onError = (error: NodeJS.ErrnoException): void => {
        server.off("listening", onListening);
        if (error.code === "EADDRINUSE") {
          resolve(false);
          return;
        }
        reject(error);
      };
      const onListening = (): void => {
        server.off("error", onError);
        resolve(true);
      };
      server.once("error", onError);
      server.once("listening", onListening);
      server.listen(this.#identity.pipePath);
    });
    if (listening) {
      this.#server = server;
      server.on("error", (error) => {
        if (this.#server === server) {
          this.#server = undefined;
          this.#destroySockets();
        }
        this.#reportServerError(error);
      });
    }
    return listening;
  }

  dispose(): void {
    const server = this.#server;
    this.#server = undefined;
    server?.close();
    this.#destroySockets();
  }

  #accept(socket: Socket): void {
    this.#sockets.add(socket);
    socket.once("close", () => this.#sockets.delete(socket));
    socket.setEncoding("utf8");
    socket.setTimeout(REQUEST_TIMEOUT_MS, () => socket.destroy());
    let contents = "";
    let handled = false;
    socket.on("data", (chunk: string) => {
      if (handled) {
        return;
      }
      contents += chunk;
      if (Buffer.byteLength(contents, "utf8") > MAX_REQUEST_BYTES) {
        handled = true;
        this.#sendError(socket, undefined, "request_too_large");
        return;
      }
      const newline = contents.indexOf("\n");
      if (newline < 0) {
        return;
      }
      handled = true;
      const trailing = contents.slice(newline + 1).trim();
      if (trailing.length > 0) {
        this.#sendError(socket, undefined, "invalid_request");
        return;
      }
      void this.#handle(socket, contents.slice(0, newline));
    });
    socket.on("error", () => undefined);
  }

  #destroySockets(): void {
    for (const socket of this.#sockets) {
      socket.destroy();
    }
    this.#sockets.clear();
  }

  async #handle(socket: Socket, line: string): Promise<void> {
    let requestId: string | undefined;
    try {
      const parsed: unknown = JSON.parse(line);
      requestId = authenticatedRequestId(
        parsed,
        this.#identity.authorization,
      );
      const request = validateAuthBrokerRequest(
        parsed,
        this.#identity.authorization,
      );
      requestId = request.requestId;
      const authentication = await this.#resolveAuthentication(request);
      if (
        authentication.accessToken.length === 0 ||
        Buffer.byteLength(authentication.accessToken, "utf8") > MAX_TOKEN_BYTES
      ) {
        throw new AuthBrokerError(
          "authentication_unavailable",
          "Authentication provider returned an invalid token",
        );
      }
      socket.end(
        `${JSON.stringify({
          version: BROKER_VERSION,
          request_id: request.requestId,
          ok: true,
          token: authentication.accessToken,
        })}\n`,
      );
    } catch (error) {
      this.#sendError(
        socket,
        requestId,
        error instanceof AuthBrokerError ? error.code : "invalid_request",
      );
    }
  }

  #sendError(socket: Socket, requestId: string | undefined, code: string): void {
    socket.end(
      `${JSON.stringify({
        version: BROKER_VERSION,
        ...(requestId ? { request_id: requestId } : {}),
        ok: false,
        error: code,
      })}\n`,
    );
  }
}

function authenticatedRequestId(
  value: unknown,
  expectedAuthorization: string,
): string | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }
  const record = value as Record<string, unknown>;
  if (
    record.version !== BROKER_VERSION ||
    typeof record.authorization !== "string" ||
    !sameSecret(record.authorization, expectedAuthorization) ||
    typeof record.request_id !== "string" ||
    !/^[A-Za-z0-9._-]{1,128}$/u.test(record.request_id)
  ) {
    return undefined;
  }
  return record.request_id;
}

function validateAuthBrokerRequest(
  value: unknown,
  expectedAuthorization: string,
): AuthBrokerRequest {
  const record = requireRecord(value);
  if (record.version !== BROKER_VERSION) {
    throw new AuthBrokerError("unsupported_version", "Unsupported version");
  }
  const authorization = requireString(record.authorization, "authorization");
  if (!sameSecret(authorization, expectedAuthorization)) {
    throw new AuthBrokerError("unauthorized", "Unauthorized");
  }
  const requestId = requireString(record.request_id, "request_id");
  if (!/^[A-Za-z0-9._-]{1,128}$/u.test(requestId)) {
    throw new AuthBrokerError("invalid_request", "Invalid request ID");
  }
  const provider = requireString(record.provider, "provider");
  if (!/^[A-Za-z0-9._-]{1,100}$/u.test(provider)) {
    throw new AuthBrokerError("invalid_request", "Invalid provider");
  }
  const resource = normalizeEndpoint(requireString(record.resource, "resource"));
  const authorizationServers = requireStringArray(
    record.authorization_servers,
    "authorization_servers",
  ).map(normalizeEndpoint);
  const scopes = requireStringArray(record.scopes, "scopes");
  if (
    resource !== GITHUB_RESOURCE ||
    authorizationServers.length !== 1 ||
    authorizationServers[0] !== GITHUB_AUTHORIZATION_SERVER
  ) {
    throw new AuthBrokerError(
      "unsupported_resource",
      "Protected resource is unsupported",
    );
  }
  if (
    scopes.length > 32 ||
    new Set(scopes).size !== scopes.length ||
    scopes.some((scope) => !/^[A-Za-z0-9:_-]{1,100}$/u.test(scope))
  ) {
    throw new AuthBrokerError("invalid_request", "Invalid scopes");
  }
  if (scopes.some((scope) => !GITHUB_ALLOWED_SCOPES.has(scope))) {
    throw new AuthBrokerError(
      "unsupported_scope",
      "GitHub scope is unsupported",
    );
  }
  return {
    requestId,
    provider,
    resource,
    authorizationServers,
    scopes,
  };
}

function requireRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new AuthBrokerError("invalid_request", "Invalid request");
  }
  return value as Record<string, unknown>;
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new AuthBrokerError("invalid_request", `${name} is invalid`);
  }
  return value;
}

function requireStringArray(value: unknown, name: string): string[] {
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    value.some((item) => typeof item !== "string" || item.length === 0)
  ) {
    throw new AuthBrokerError("invalid_request", `${name} is invalid`);
  }
  return value;
}

function normalizeEndpoint(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

function sameSecret(actual: string, expected: string): boolean {
  const actualBytes = Buffer.from(actual, "utf8");
  const expectedBytes = Buffer.from(expected, "utf8");
  return (
    actualBytes.length === expectedBytes.length &&
    timingSafeEqual(actualBytes, expectedBytes)
  );
}
