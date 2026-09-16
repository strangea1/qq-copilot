import { createHmac, randomUUID } from "node:crypto";
import { connect } from "node:net";

const BROKER_VERSION = 1;
const MAX_RESPONSE_BYTES = 128 * 1024;
const DEFAULT_TIMEOUT_MS = 5 * 60 * 1_000;

export interface ProtectedResourceRequest {
  readonly provider: string;
  readonly resource: string;
  readonly authorizationServers: readonly string[];
  readonly scopes: readonly string[];
}

export interface BrokeredCredential {
  readonly token: string;
}

export class CredentialBrokerError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "CredentialBrokerError";
    this.code = code;
  }
}

export function deriveCredentialBrokerIdentity(bridgeToken: string): {
  readonly pipePath: string;
  readonly authorization: string;
} {
  if (!/^[a-fA-F0-9]{64,}$/u.test(bridgeToken)) {
    throw new CredentialBrokerError(
      "invalid-bridge-config",
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

export async function requestBrokeredCredential(
  bridgeToken: string,
  request: ProtectedResourceRequest,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  signal?: AbortSignal,
): Promise<BrokeredCredential> {
  if (signal?.aborted) {
    throw new CredentialBrokerError(
      "authentication-broker-cancelled",
      "Authentication broker request was cancelled",
    );
  }
  const identity = deriveCredentialBrokerIdentity(bridgeToken);
  const requestId = randomUUID();
  const payload = `${JSON.stringify({
    version: BROKER_VERSION,
    authorization: identity.authorization,
    request_id: requestId,
    provider: request.provider,
    resource: request.resource,
    authorization_servers: request.authorizationServers,
    scopes: request.scopes,
  })}\n`;
  return new Promise<BrokeredCredential>((resolve, reject) => {
    const socket = connect(identity.pipePath);
    let settled = false;
    let contents = "";
    const cleanup = (): void => {
      signal?.removeEventListener("abort", aborted);
    };
    const fail = (error: CredentialBrokerError): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      socket.destroy();
      reject(error);
    };
    const aborted = (): void => {
      fail(
        new CredentialBrokerError(
          "authentication-broker-cancelled",
          "Authentication broker request was cancelled",
        ),
      );
    };
    signal?.addEventListener("abort", aborted, { once: true });
    socket.setEncoding("utf8");
    socket.setTimeout(timeoutMs, () =>
      fail(
        new CredentialBrokerError(
          "authentication-broker-timeout",
          "Authentication broker timed out",
        ),
      ),
    );
    socket.on("connect", () => socket.write(payload));
    socket.on("data", (chunk: string) => {
      if (settled) {
        return;
      }
      contents += chunk;
      if (Buffer.byteLength(contents, "utf8") > MAX_RESPONSE_BYTES) {
        fail(
          new CredentialBrokerError(
            "authentication-broker-response-too-large",
            "Authentication broker response was too large",
          ),
        );
        return;
      }
      const newline = contents.indexOf("\n");
      if (newline < 0) {
        return;
      }
      try {
        const response = requireRecord(JSON.parse(contents.slice(0, newline)));
        if (
          response.version !== BROKER_VERSION ||
          response.request_id !== requestId
        ) {
          throw new CredentialBrokerError(
            "authentication-broker-invalid-response",
            "Authentication broker response did not match the request",
          );
        }
        if (response.ok !== true) {
          const code =
            typeof response.error === "string"
              ? sanitizeCode(response.error)
              : "authentication-unavailable";
          throw new CredentialBrokerError(
            code,
            "Authentication broker rejected the request",
          );
        }
        if (
          typeof response.token !== "string" ||
          response.token.length === 0
        ) {
          throw new CredentialBrokerError(
            "authentication-broker-invalid-response",
            "Authentication broker omitted the token",
          );
        }
        settled = true;
        cleanup();
        socket.end();
        resolve({ token: response.token });
      } catch (error) {
        fail(
          error instanceof CredentialBrokerError
            ? error
            : new CredentialBrokerError(
                "authentication-broker-invalid-response",
                "Authentication broker returned invalid JSON",
              ),
        );
      }
    });
    socket.on("error", () =>
      fail(
        new CredentialBrokerError(
          "authentication-broker-unavailable",
          "Authentication broker is unavailable",
        ),
      ),
    );
    socket.on("end", () => {
      if (!settled) {
        fail(
          new CredentialBrokerError(
            "authentication-broker-disconnected",
            "Authentication broker disconnected before replying",
          ),
        );
      }
    });
  });
}

function requireRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new CredentialBrokerError(
      "authentication-broker-invalid-response",
      "Authentication broker returned an invalid response",
    );
  }
  return value as Record<string, unknown>;
}

function sanitizeCode(value: string): string {
  return value.replace(/[^A-Za-z0-9_.-]/gu, "-").slice(0, 100) || "unknown";
}
