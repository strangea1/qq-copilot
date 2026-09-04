import { createHash } from "node:crypto";
import { lstat, readFile, readdir } from "node:fs/promises";
import { basename, join } from "node:path";

const ENTRY_SCHEMA_VERSION = 2;
const MAX_ENTRY_BYTES = 64 * 1024;
const ENTRY_FILE_PATTERN = /^[a-f0-9]{64}\.json$/;
const BASE64URL_PATTERN = /^[A-Za-z0-9_-]+$/;
const SEMVER_PATTERN = /^\d+\.\d+\.\d+$/;

export interface SocketEndpoint {
  readonly type: "socket";
  readonly path: string;
}

export interface TcpEndpoint {
  readonly type: "tcp";
  readonly host: string;
  readonly port: number;
}

export interface WebSocketEndpoint {
  readonly type: "websocket";
  readonly url: string;
}

export interface EndpointRegistryEntry {
  readonly schemaVersion: 2;
  readonly type: "editor" | "standalone";
  readonly pid: number;
  readonly instanceId: string;
  readonly protocolVersion: string;
  // Some standalone launchers version their registry independently from the wire.
  // This field is assigned only by the audited managed-target adapter, never from disk.
  readonly wireProtocolVersion?: string;
  readonly connectionToken: string;
  readonly endpoint: SocketEndpoint | TcpEndpoint | WebSocketEndpoint;
  readonly sourceFile: string;
}

export function defaultUserDataDirectory(): string {
  const configured = process.env.VSCODE_APPDATA;
  if (configured) {
    return configured;
  }
  const roaming = process.env.APPDATA;
  if (!roaming) {
    throw new Error("APPDATA is not set; provide --user-data-dir");
  }
  return join(roaming, "Code");
}

export async function discoverEditorEndpoints(
  userDataDirectory: string,
): Promise<EndpointRegistryEntry[]> {
  const entriesDirectory = join(
    userDataDirectory,
    "agent-host",
    "local-endpoint",
    "entries",
  );
  let names: string[];
  try {
    names = await readdir(entriesDirectory);
  } catch (error) {
    if (isNodeError(error, "ENOENT")) {
      return [];
    }
    throw new Error("failed to enumerate the Agent Host endpoint registry", {
      cause: error,
    });
  }

  const entries: EndpointRegistryEntry[] = [];
  const identities = new Set<string>();
  for (const name of names.sort()) {
    if (!ENTRY_FILE_PATTERN.test(name)) {
      continue;
    }
    const sourceFile = join(entriesDirectory, name);
    const entry = await readEntry(sourceFile, name);
    if (
      !entry ||
      entry.type !== "editor" ||
      entry.endpoint.type !== "socket" ||
      !isProcessAlive(entry.pid)
    ) {
      continue;
    }
    const identity = `${entry.type}\0${entry.pid}\0${entry.instanceId}`;
    if (identities.has(identity)) {
      continue;
    }
    identities.add(identity);
    entries.push(entry);
  }
  return entries;
}

export interface WatchEditorEndpointsOptions {
  readonly signal?: AbortSignal;
  readonly pollIntervalMs?: number;
}

/**
 * Polls the editor-owned registry and yields only when its validated endpoint
 * set changes. Polling is intentional: the entries directory may not exist yet
 * and can be atomically replaced while VS Code starts or updates.
 */
export async function* watchEditorEndpoints(
  userDataDirectory: string,
  options: WatchEditorEndpointsOptions = {},
): AsyncGenerator<readonly EndpointRegistryEntry[]> {
  const pollIntervalMs = options.pollIntervalMs ?? 1_000;
  if (!Number.isFinite(pollIntervalMs) || pollIntervalMs <= 0) {
    throw new RangeError("pollIntervalMs must be a positive finite number");
  }

  let previousFingerprint: string | undefined;
  while (!options.signal?.aborted) {
    const entries = await discoverEditorEndpoints(userDataDirectory);
    const fingerprint = registryFingerprint(entries);
    if (fingerprint !== previousFingerprint) {
      previousFingerprint = fingerprint;
      yield entries;
    }
    if (!(await waitForNextScan(pollIntervalMs, options.signal))) {
      return;
    }
  }
}

function registryFingerprint(entries: readonly EndpointRegistryEntry[]): string {
  const hash = createHash("sha256");
  for (const entry of entries) {
    hash.update(entryIdentityHash(entry), "utf8");
    hash.update("\0", "utf8");
    hash.update(entry.protocolVersion, "utf8");
    hash.update("\0", "utf8");
    hash.update(entry.connectionToken, "utf8");
    hash.update("\0", "utf8");
    if (entry.endpoint.type === "socket") {
      hash.update(entry.endpoint.path, "utf8");
    } else if (entry.endpoint.type === "tcp") {
      hash.update(`${entry.endpoint.host}\0${entry.endpoint.port}`, "utf8");
    } else {
      hash.update(entry.endpoint.url, "utf8");
    }
    hash.update("\0", "utf8");
  }
  return hash.digest("hex");
}

function waitForNextScan(
  delayMs: number,
  signal: AbortSignal | undefined,
): Promise<boolean> {
  if (signal?.aborted) {
    return Promise.resolve(false);
  }
  return new Promise((resolve) => {
    let settled = false;
    const finish = (shouldContinue: boolean): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", aborted);
      resolve(shouldContinue);
    };
    const aborted = (): void => finish(false);
    const timer = setTimeout(() => finish(true), delayMs);
    timer.unref();
    signal?.addEventListener("abort", aborted, { once: true });
    if (signal?.aborted) {
      finish(false);
    }
  });
}

async function readEntry(
  sourceFile: string,
  fileName: string,
): Promise<EndpointRegistryEntry | undefined> {
  try {
    const metadata = await lstat(sourceFile);
    if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size > MAX_ENTRY_BYTES) {
      return undefined;
    }
    const raw: unknown = JSON.parse(await readFile(sourceFile, "utf8"));
    const entry = parseEntry(raw, sourceFile);
    if (!entry || `${entryIdentityHash(entry)}.json` !== fileName) {
      return undefined;
    }
    return entry;
  } catch {
    return undefined;
  }
}

function parseEntry(
  raw: unknown,
  sourceFile: string,
): EndpointRegistryEntry | undefined {
  if (!isRecord(raw) || raw.schemaVersion !== ENTRY_SCHEMA_VERSION) {
    return undefined;
  }
  if (
    (raw.type !== "editor" && raw.type !== "standalone") ||
    !isSafeInteger(raw.pid) ||
    raw.pid <= 0 ||
    typeof raw.instanceId !== "string" ||
    raw.instanceId.length < 16 ||
    !BASE64URL_PATTERN.test(raw.instanceId) ||
    typeof raw.protocolVersion !== "string" ||
    !SEMVER_PATTERN.test(raw.protocolVersion) ||
    typeof raw.connectionToken !== "string" ||
    raw.connectionToken.length < 32 ||
    !BASE64URL_PATTERN.test(raw.connectionToken) ||
    !isRecord(raw.endpoint)
  ) {
    return undefined;
  }

  let endpoint: SocketEndpoint | TcpEndpoint | WebSocketEndpoint;
  if (
    raw.endpoint.type === "socket" &&
    typeof raw.endpoint.path === "string" &&
    raw.endpoint.path.startsWith("\\\\.\\pipe\\")
  ) {
    endpoint = {
      type: "socket",
      path: raw.endpoint.path,
    };
  } else if (
    raw.endpoint.type === "tcp" &&
    typeof raw.endpoint.host === "string" &&
    isSafeInteger(raw.endpoint.port) &&
    raw.endpoint.port > 0 &&
    raw.endpoint.port <= 65_535
  ) {
    endpoint = {
      type: "tcp",
      host: raw.endpoint.host,
      port: raw.endpoint.port,
    };
  } else if (
    raw.endpoint.type === "websocket" &&
    typeof raw.endpoint.url === "string" &&
    /^wss?:\/\//u.test(raw.endpoint.url)
  ) {
    endpoint = {
      type: "websocket",
      url: raw.endpoint.url,
    };
  } else {
    return undefined;
  }

  return {
    schemaVersion: ENTRY_SCHEMA_VERSION,
    type: raw.type,
    pid: raw.pid,
    instanceId: raw.instanceId,
    protocolVersion: raw.protocolVersion,
    connectionToken: raw.connectionToken,
    endpoint,
    sourceFile,
  };
}

function entryIdentityHash(
  entry: Pick<EndpointRegistryEntry, "type" | "pid" | "instanceId">,
): string {
  return createHash("sha256")
    .update(`${entry.type}\0${entry.pid}\0${entry.instanceId}`, "utf8")
    .digest("hex");
}

export function endpointPublicId(entry: EndpointRegistryEntry): string {
  return basename(entry.sourceFile, ".json").slice(0, 12);
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return isNodeError(error, "EPERM");
  }
}

function isSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNodeError(error: unknown, code: string): boolean {
  return error instanceof Error && "code" in error && error.code === code;
}
