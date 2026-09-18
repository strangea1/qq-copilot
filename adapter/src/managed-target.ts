import { createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import net from "node:net";
import { win32 } from "node:path";
import { pathToFileURL } from "node:url";

import {
  SUPPORTED_PROTOCOL_VERSIONS,
  type RootState,
  type SessionSummary,
  type Snapshot,
  type URI,
} from "@microsoft/agent-host-protocol";
import {
  AhpClient,
  AhpStateMirror,
  ClientClosedError,
  type Subscription,
} from "@microsoft/agent-host-protocol/client";

import { openEndpointTransport } from "./named-pipe-transport.js";
import type { AdapterConfig } from "./config.js";
import type {
  EndpointRegistryEntry,
  SocketEndpoint,
  TcpEndpoint,
  WebSocketEndpoint,
} from "./endpoint-registry.js";

export type ManagedTarget =
  | {
      readonly kind: "local";
      readonly path: string;
    }
  | {
      readonly kind: "ssh";
      readonly alias: string;
      readonly path: string;
      readonly user: string;
      readonly host: string;
      readonly port: number;
      readonly hostKeyFingerprints: readonly string[];
    };

function localWorkspaceUriMatches(
  targetPath: string,
  workspaceUri: string,
): boolean {
  let candidate: URL;
  try {
    candidate = new URL(workspaceUri);
  } catch {
    return false;
  }
  if (
    candidate.protocol !== "file:" ||
    candidate.search !== "" ||
    candidate.hash !== ""
  ) {
    return false;
  }
  let decodedPath: string;
  try {
    decodedPath = decodeURIComponent(candidate.pathname);
  } catch {
    return false;
  }
  const windowsPath =
    candidate.host.length > 0
      ? `\\\\${candidate.host}${decodedPath.replaceAll("/", "\\")}`
      : /^\/[A-Za-z]:\//u.test(decodedPath)
        ? decodedPath.slice(1).replaceAll("/", "\\")
        : undefined;
  return (
    windowsPath !== undefined &&
    normalizeWindowsPath(windowsPath) === normalizeWindowsPath(targetPath)
  );
}

function normalizeWindowsPath(path: string): string {
  return win32
    .normalize(path)
    .replace(/\\+$/u, "")
    .toLocaleLowerCase("en-US");
}

export interface PrepareTargetResult {
  readonly endpoint_id: string;
  readonly host_instance_id: string;
  readonly provider: string;
  readonly workspace_uri: string;
  readonly host_label: string;
  readonly editor_client_tools_available: boolean;
}

export async function prepareTargetResult(
  connection: ConnectedManagedTarget,
): Promise<PrepareTargetResult> {
  const provider = defaultManagedProvider(connection.rootState);
  return {
    endpoint_id: connection.prepared.endpointId,
    host_instance_id: connection.prepared.entry.instanceId,
    provider,
    workspace_uri: connection.prepared.workspaceUri,
    host_label: connection.prepared.hostLabel,
    editor_client_tools_available:
      connection.prepared.editorClientToolsAvailable,
  };
}

function beforeDeadline<T>(
  startOperation: () => Promise<T>,
  deadline: number,
  timeoutCode: string,
): Promise<T> {
  const remaining = deadline - Date.now();
  if (remaining <= 0) {
    return Promise.reject(codedError(timeoutCode));
  }
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      settled = true;
      reject(codedError(timeoutCode));
    }, remaining);
    let operation: Promise<T>;
    try {
      operation = startOperation();
    } catch (error) {
      clearTimeout(timer);
      reject(error);
      return;
    }
    operation.then(
      (value) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

interface CodeAgentEndpointsDocument {
  readonly endpoints: readonly CodeAgentEndpoint[];
}

interface CodeAgentEndpoint {
  readonly schemaVersion: number;
  readonly type: "editor" | "standalone";
  readonly pid: number;
  readonly instanceId: string;
  readonly protocolVersion: string;
  readonly connectionToken: string;
  readonly endpoint: SocketEndpoint | TcpEndpoint | WebSocketEndpoint;
}

export interface PreparedTarget {
  readonly target: ManagedTarget;
  readonly entry: EndpointRegistryEntry;
  readonly endpointId: string;
  readonly hostLabel: string;
  readonly workspaceUri: string;
  readonly editorClientToolsAvailable: boolean;
  readonly host?: ChildProcess;
  readonly hostExecutable?: string;
  readonly tunnel?: ChildProcess;
}

const NEGOTIATED_PROTOCOL_VERSIONS = [...SUPPORTED_PROTOCOL_VERSIONS];
const MANAGED_REQUEST_TIMEOUT_MS = 295_000;
const MANAGED_CONTROL_REQUEST_TIMEOUT_MS = 30_000;
const LOCAL_STANDALONE_START_TIMEOUT_MS = 240_000;
const LOCAL_STANDALONE_POLL_INTERVAL_MS = 250;
const LOCAL_STANDALONE_OUTPUT_LIMIT = 64 * 1024;
const STANDALONE_REGISTRY_PROTOCOL_ALIASES = new Map<string, string>([
  ["0.1.0", "0.9.0"],
]);
const PREFERRED_MANAGED_PROVIDERS = ["copilotcli", "copilot"] as const;

export function standaloneWireProtocolVersion(
  registryProtocolVersion: string,
): string | undefined {
  if (NEGOTIATED_PROTOCOL_VERSIONS.includes(registryProtocolVersion)) {
    return registryProtocolVersion;
  }
  return STANDALONE_REGISTRY_PROTOCOL_ALIASES.get(registryProtocolVersion);
}

export interface ConnectedManagedTarget {
  readonly prepared: PreparedTarget;
  readonly client: AhpClient;
  readonly rootState: RootState;
  readonly sessions: readonly SessionSummary[];
  releasePreparedOwnership(): void;
  disconnect(): Promise<void>;
  close(): Promise<void>;
}

class ManagedRootTracker {
  readonly #mirror = new AhpStateMirror();

  readonly #subscription: Subscription;

  #pump: Promise<void> | undefined;

  #error: unknown;

  #closed = false;

  constructor(snapshots: readonly Snapshot[], subscription: Subscription) {
    for (const snapshot of snapshots) {
      this.#mirror.applySnapshot(snapshot);
    }
    this.#subscription = subscription;
  }

  get root(): RootState {
    this.assertHealthy();
    return this.#mirror.root;
  }

  assertHealthy(): void {
    if (this.#error !== undefined) {
      throw this.#error;
    }
  }

  start(): void {
    if (this.#pump) {
      return;
    }
    this.#pump = this.#run().catch((error) => {
      this.#error = error;
    });
  }

  beginClose(): void {
    this.#closed = true;
  }

  async close(): Promise<void> {
    this.beginClose();
    await this.#subscription.close();
    await this.#pump;
  }

  async #run(): Promise<void> {
    for await (const event of this.#subscription) {
      if (event.type === "action") {
        this.#mirror.apply(event.params);
      } else if (event.type === "authRequired") {
        throw codedError(
          "authentication-required",
          "Sign in to the Agent Host on the PC.",
        );
      }
    }
    if (!this.#closed) {
      throw codedError("managed-root-subscription-ended");
    }
  }
}

export async function connectManagedTarget(
  config: AdapterConfig,
  target: ManagedTarget,
): Promise<ConnectedManagedTarget> {
  const prepared = await prepareManagedTarget(config, target);
  return connectPreparedTarget(prepared, true);
}

export async function connectPreparedManagedTarget(
  prepared: PreparedTarget,
): Promise<ConnectedManagedTarget> {
  return connectPreparedTarget(prepared, false);
}

async function connectPreparedTarget(
  prepared: PreparedTarget,
  stopPreparedOnClose: boolean,
): Promise<ConnectedManagedTarget> {
  let client: AhpClient | undefined;
  let rootTracker: ManagedRootTracker | undefined;
  let ownsPrepared = stopPreparedOnClose;
  try {
    const transport = await openEndpointTransport(
      prepared.entry.endpoint,
      prepared.entry.connectionToken,
    );
    client = new AhpClient(transport, {
      requestTimeoutMs: MANAGED_REQUEST_TIMEOUT_MS,
    });
    const managedClient = client;
    const rootSubscription = managedClient.attachSubscription("ahp-root://");
    managedClient.connect();
    const initialized = await beforeDeadline(
      () =>
        managedClient.request("initialize", {
          channel: "ahp-root://",
          clientId: `qq-copilot-managed-${prepared.entry.instanceId}`,
          clientInfo: {
            name: "qq-copilot-ahp-adapter",
            version: "0.1.0",
            title: "QQ Copilot AHP Adapter",
          },
          protocolVersions: [...NEGOTIATED_PROTOCOL_VERSIONS],
          initialSubscriptions: ["ahp-root://"],
          locale: "zh-CN",
        }),
      Date.now() + MANAGED_CONTROL_REQUEST_TIMEOUT_MS,
      "managed-initialize-timeout",
    );
    const expectedWireProtocolVersion =
      prepared.entry.wireProtocolVersion ?? prepared.entry.protocolVersion;
    if (
      initialized.protocolVersion !== expectedWireProtocolVersion ||
      !NEGOTIATED_PROTOCOL_VERSIONS.includes(initialized.protocolVersion)
    ) {
      throw codedError(
        "incompatible-protocol",
        `incompatible-protocol:${prepared.entry.protocolVersion}->${initialized.protocolVersion}`,
      );
    }
    rootTracker = new ManagedRootTracker(
      initialized.snapshots ?? [],
      rootSubscription,
    );
    const activeRootTracker = rootTracker;
    activeRootTracker.start();
    const sessions = await listSessions(managedClient);
    activeRootTracker.assertHealthy();
    const connectedClient = managedClient;
    return {
      prepared,
      client: connectedClient,
      get rootState(): RootState {
        return activeRootTracker.root;
      },
      sessions,
      releasePreparedOwnership(): void {
        ownsPrepared = false;
      },
      async disconnect(): Promise<void> {
        await shutdownManagedConnection(connectedClient, activeRootTracker);
      },
      async close(): Promise<void> {
        const errors: unknown[] = [];
        try {
          await shutdownManagedConnection(connectedClient, activeRootTracker);
        } catch (error) {
          errors.push(error);
        }
        if (ownsPrepared) {
          try {
            await stopManagedTargetProcesses(prepared);
          } catch (error) {
            errors.push(error);
          }
        }
        throwCleanupErrors(errors, "managed-target-close-failed");
      },
    };
  } catch (error) {
    const errors: unknown[] = [error];
    if (client) {
      try {
        if (rootTracker) {
          await shutdownManagedConnection(client, rootTracker);
        } else {
          await shutdownManagedClient(client);
        }
      } catch (shutdownError) {
        errors.push(shutdownError);
      }
    }
    if (ownsPrepared) {
      try {
        await stopManagedTargetProcesses(prepared);
      } catch (cleanupError) {
        errors.push(cleanupError);
      }
    }
    if (errors.length > 1) {
      throw new AggregateError(errors, "managed-target-cleanup-failed");
    }
    throw error;
  }
}

async function shutdownManagedClient(client: AhpClient): Promise<void> {
  try {
    await client.shutdown();
  } catch (error) {
    if (!(error instanceof ClientClosedError)) {
      throw error;
    }
  }
}

async function shutdownManagedConnection(
  client: AhpClient,
  rootTracker: ManagedRootTracker,
): Promise<void> {
  const errors: unknown[] = [];
  rootTracker.beginClose();
  try {
    await shutdownManagedClient(client);
  } catch (error) {
    errors.push(error);
  }
  try {
    await rootTracker.close();
  } catch (error) {
    errors.push(error);
  }
  throwCleanupErrors(errors, "managed-connection-shutdown-failed");
}

function throwCleanupErrors(errors: unknown[], message: string): void {
  if (errors.length === 1) {
    throw errors[0];
  }
  if (errors.length > 1) {
    throw new AggregateError(errors, message);
  }
}

export async function stopManagedTargetProcesses(
  prepared: Pick<
    PreparedTarget,
    "entry" | "host" | "hostExecutable" | "tunnel"
  >,
): Promise<void> {
  stopChildProcess(prepared.tunnel);
  if (!prepared.hostExecutable) {
    stopChildProcess(prepared.host);
    return;
  }

  await stopOwnedLocalStandalone(
    prepared.hostExecutable,
    prepared.host,
    prepared.entry,
  );
}

async function stopOwnedLocalStandalone(
  executable: string,
  host: ChildProcess | undefined,
  entry: Pick<CodeAgentEndpoint, "instanceId" | "pid">,
): Promise<void> {
  let killError: unknown;
  try {
    await runProcess(
      executable,
      ["agent", "kill", "--instance-id", entry.instanceId],
      10_000,
    );
  } catch (error) {
    killError = error;
  }
  stopChildProcess(host);
  const stopped = await waitForLocalStandaloneStopped(
    executable,
    entry,
    host,
  );
  stopChildProcess(host);
  if (!stopped) {
    throw new Error("managed-standalone-cleanup-timeout", {
      cause: killError,
    });
  }
}

async function prepareManagedTarget(
  config: AdapterConfig,
  target: ManagedTarget,
): Promise<PreparedTarget> {
  if (target.kind === "local") {
    if (!config.codeExecutable) {
      throw codedError("code-not-configured");
    }
    const started = await startLocalStandaloneEndpoint(config.codeExecutable);
    const endpoint = started.endpoint;
    const entry = toRegistryEntry(endpoint);
    return {
      target,
      entry,
      endpointId: endpointPublicId(entry),
      hostLabel: "local",
      workspaceUri: fileUri(target.path),
      editorClientToolsAvailable: false,
      host: started.host,
      hostExecutable: config.codeExecutable,
    };
  }
  const sshExecutable = config.sshExecutable;
  if (!sshExecutable) {
    throw codedError("ssh-not-configured");
  }
  const identity = await resolveSshAlias(sshExecutable, target.alias);
  if (
    identity.user !== target.user ||
    identity.host !== target.host ||
    identity.port !== target.port
  ) {
    throw codedError("ssh-identity-changed");
  }
  const remote = await prepareRemoteStandaloneEndpoint(sshExecutable, target);
  const localPort = await allocateLocalPort();
  const tunnel = spawn(
    sshExecutable,
    [
      "-N",
      "-T",
      "-o",
      "BatchMode=yes",
      "-o",
      "StrictHostKeyChecking=yes",
      "-o",
      "ConnectTimeout=60",
      "-o",
      "ExitOnForwardFailure=yes",
      "-L",
      `${localPort}:${remote.forwardHost}:${remote.forwardPort}`,
      target.alias,
    ],
    {
      stdio: ["ignore", "ignore", "pipe"],
      windowsHide: true,
    },
  );
  try {
    await waitForTunnelStart(tunnel, localPort);
  } catch (error) {
    if (!tunnel.killed) {
      tunnel.kill();
    }
    tunnel.stderr?.destroy();
    throw error;
  }
  const endpoint: TcpEndpoint = {
    type: "tcp",
    host: "127.0.0.1",
    port: localPort,
  };
  const entry = toRegistryEntry({
    ...remote.endpoint,
    endpoint,
  });
  return {
    target,
    entry,
    endpointId: endpointPublicId(entry),
    hostLabel: `ssh:${target.alias}`,
    workspaceUri: posixFileUri(target.path),
    editorClientToolsAvailable: false,
    tunnel,
  };
}

async function prepareRemoteStandaloneEndpoint(
  sshExecutable: string,
  target: Extract<ManagedTarget, { kind: "ssh" }>,
): Promise<{
  readonly endpoint: CodeAgentEndpoint;
  readonly forwardHost: string;
  readonly forwardPort: number;
}> {
  const hostKeyFingerprints = await readRemoteHostKeyFingerprints(
    sshExecutable,
    target.alias,
  );
  const expectedHostKeyFingerprints = [
    ...target.hostKeyFingerprints,
  ].sort();
  if (
    hostKeyFingerprints.length !== expectedHostKeyFingerprints.length ||
    hostKeyFingerprints.some(
      (fingerprint, index) =>
        fingerprint !== expectedHostKeyFingerprints[index],
    )
  ) {
    throw codedError("ssh-host-key-changed");
  }
  const script = `set -eu
if [ "$(uname -s)" != "Linux" ]; then
  exit 20
fi
if ! command -v code >/dev/null 2>&1; then
  exit 21
fi
code agent host --help >/dev/null 2>&1
canonical_path="$(realpath "$1")"
if [ "$canonical_path" != "$2" ] || [ ! -d "$canonical_path" ]; then
  exit 22
fi
started_endpoint="$(code agent host 2>&1)"
printf '%s\n' "__QQ_STARTED_ENDPOINT__"
printf '%s\n' "$started_endpoint"
printf '%s\n' "__QQ_ENDPOINTS__"
code agent endpoints
`;
  const output = await runSshScript(
    sshExecutable,
    target.alias,
    script,
    [normalizePosixPath(target.path), normalizePosixPath(target.path)],
    300_000,
  );
  const startedEndpointMarker = "__QQ_STARTED_ENDPOINT__\n";
  const endpointsMarker = "\n__QQ_ENDPOINTS__\n";
  const startedEndpointStart = output.indexOf(startedEndpointMarker);
  const endpointsStart = output.indexOf(
    endpointsMarker,
    startedEndpointStart + startedEndpointMarker.length,
  );
  if (startedEndpointStart < 0 || endpointsStart < 0) {
    throw codedError("remote-host-identity-missing");
  }
  const document = parseEndpointsDocument(
    output.slice(endpointsStart + endpointsMarker.length),
  );
  const endpoint = selectStandaloneEndpoint(
    document.endpoints,
    output.slice(
      startedEndpointStart + startedEndpointMarker.length,
      endpointsStart,
    ),
  );
  if (endpoint.endpoint.type !== "tcp") {
    throw codedError("remote-endpoint-not-tcp");
  }
  return {
    endpoint,
    forwardHost: endpoint.endpoint.host,
    forwardPort: endpoint.endpoint.port,
  };
}

async function readRemoteHostKeyFingerprints(
  sshExecutable: string,
  alias: string,
): Promise<string[]> {
  const script = `set -eu
if [ "$(uname -s)" != "Linux" ]; then
  exit 20
fi
if ! command -v ssh-keygen >/dev/null 2>&1; then
  exit 23
fi
for key in /etc/ssh/ssh_host_*_key.pub; do
  if [ -r "$key" ]; then
    ssh-keygen -E sha256 -lf "$key" | awk '{print $2}'
  fi
done | LC_ALL=C sort -u
`;
  return parseHostKeyFingerprints(
    await runSshScript(sshExecutable, alias, script, []),
  );
}

async function resolveSshAlias(
  sshExecutable: string,
  alias: string,
): Promise<{ readonly user: string; readonly host: string; readonly port: number }> {
  validateSshAlias(alias);
  const output = await runProcess(sshExecutable, ["-G", alias]);
  let user: string | undefined;
  let host: string | undefined;
  let port: number | undefined;
  for (const line of output.stdout.split(/\r?\n/gu)) {
    const [key, value] = line.split(/\s+/u, 2);
    if (key === "user" && value) {
      user = value;
    } else if (key === "hostname" && value) {
      host = value;
    } else if (key === "port" && value) {
      port = Number.parseInt(value, 10);
    }
  }
  if (!user || !host || !port || !Number.isSafeInteger(port) || port <= 0) {
    throw codedError("invalid-ssh-alias");
  }
  return { user, host, port };
}

async function listCodeAgentEndpoints(
  codeExecutable: string,
  timeoutMs = 30_000,
): Promise<CodeAgentEndpointsDocument> {
  const output = await runProcess(
    codeExecutable,
    ["agent", "endpoints"],
    timeoutMs,
  );
  return parseEndpointsDocument(output.stdout);
}

function parseEndpointsDocument(raw: string): CodeAgentEndpointsDocument {
  const parsed = JSON.parse(raw) as CodeAgentEndpointsDocument;
  if (!parsed || !Array.isArray(parsed.endpoints)) {
    throw codedError("invalid-agent-endpoints");
  }
  return parsed;
}

function selectStandaloneEndpoint(
  endpoints: readonly CodeAgentEndpoint[],
  startedOutput?: string,
): CodeAgentEndpoint {
  const standalone = endpoints.filter(
    (candidate) =>
      candidate.schemaVersion === 2 &&
      candidate.type === "standalone",
  );
  const compatible = standalone.filter(
    (candidate) =>
      standaloneWireProtocolVersion(candidate.protocolVersion) !== undefined,
  );
  if (startedOutput) {
    const startedUrl = startedOutput.match(
      /\bws:\/\/[^\s\u0000-\u001f]+/u,
    )?.[0];
    if (startedUrl) {
      const token = new URL(startedUrl).searchParams.get("tkn");
      const endpoint = compatible.find(
        (candidate) => token && candidate.connectionToken === token,
      );
      if (endpoint) {
        return endpoint;
      }
    }
  }
  if (compatible.length === 1 && compatible[0]) {
    return compatible[0];
  }
  if (compatible.length === 0) {
    if (standalone.length > 0) {
      throw codedError(
        "incompatible-standalone-protocol",
        `unsupported standalone registry protocols: ${[
          ...new Set(standalone.map((candidate) => candidate.protocolVersion)),
        ].join(",")}`,
      );
    }
    throw codedError("standalone-endpoint-not-found");
  }
  throw codedError("standalone-endpoint-ambiguous");
}

function toRegistryEntry(endpoint: CodeAgentEndpoint): EndpointRegistryEntry {
  const wireProtocolVersion = standaloneWireProtocolVersion(
    endpoint.protocolVersion,
  );
  if (!wireProtocolVersion) {
    throw codedError("incompatible-standalone-protocol");
  }
  const identity = `${endpoint.type}\0${endpoint.pid}\0${endpoint.instanceId}`;
  const sourceFile = `${createHash("sha256").update(identity, "utf8").digest("hex")}.json`;
  return {
    schemaVersion: 2,
    type: endpoint.type,
    pid: endpoint.pid,
    instanceId: endpoint.instanceId,
    protocolVersion: endpoint.protocolVersion,
    ...(wireProtocolVersion === endpoint.protocolVersion
      ? {}
      : { wireProtocolVersion }),
    connectionToken: endpoint.connectionToken,
    endpoint: endpoint.endpoint,
    sourceFile,
  };
}

function endpointPublicId(entry: EndpointRegistryEntry): string {
  return entry.sourceFile.replace(/\.json$/u, "").slice(0, 12);
}

export function managedTargetWorkspaceUri(target: ManagedTarget): URI {
  return target.kind === "local" ? fileUri(target.path) : posixFileUri(target.path);
}

export function managedTargetMatchesWorkspaceUri(
  target: ManagedTarget,
  workspaceUri: string,
): boolean {
  if (target.kind === "local") {
    return localWorkspaceUriMatches(target.path, workspaceUri);
  }
  let candidate: URL;
  try {
    candidate = new URL(workspaceUri);
  } catch {
    return false;
  }
  if (
    candidate.protocol !== "file:" &&
    candidate.protocol !== "vscode-remote:"
  ) {
    return false;
  }
  if (candidate.protocol === "file:" && candidate.host !== "") {
    return false;
  }
  if (
    candidate.username !== "" ||
    candidate.password !== "" ||
    candidate.search !== "" ||
    candidate.hash !== ""
  ) {
    return false;
  }
  try {
    if (
      candidate.protocol === "vscode-remote:" &&
      decodeURIComponent(candidate.host).toLocaleLowerCase("en-US") !==
        `ssh-remote+${target.alias}`.toLocaleLowerCase("en-US")
    ) {
      return false;
    }
    return normalizePosixPath(decodeURIComponent(candidate.pathname)) ===
      normalizePosixPath(target.path);
  } catch {
    return false;
  }
}

function fileUri(path: string): URI {
  return pathToFileURL(path).href;
}

function posixFileUri(path: string): URI {
  const encodedPath = normalizePosixPath(path)
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  return `file://${encodedPath}`;
}

function normalizePosixPath(path: string): string {
  if (!path.startsWith("/")) {
    throw codedError("posix-path-required");
  }
  const segments: string[] = [];
  for (const segment of path.split("/")) {
    if (!segment || segment === ".") {
      continue;
    }
    if (segment === "..") {
      segments.pop();
      continue;
    }
    segments.push(segment);
  }
  return `/${segments.join("/")}`;
}

async function listSessions(client: AhpClient): Promise<readonly SessionSummary[]> {
  const sessions = new Map<string, SessionSummary>();
  let cursor: string | undefined;
  const seen = new Set<string>();
  const deadline = Date.now() + MANAGED_CONTROL_REQUEST_TIMEOUT_MS;
  for (let page = 0; page < 100; page += 1) {
    const result = await beforeDeadline(
      () =>
        client.request("listSessions", {
          channel: "ahp-root://",
          limit: 100,
          ...(cursor ? { cursor } : {}),
        }),
      deadline,
      "managed-list-sessions-timeout",
    );
    for (const item of result.items) {
      sessions.set(item.resource, item);
    }
    if (!result.nextCursor) {
      return [...sessions.values()];
    }
    if (seen.has(result.nextCursor)) {
      throw new Error("repeated-session-cursor");
    }
    seen.add(result.nextCursor);
    cursor = result.nextCursor;
  }
  throw new Error("session-pagination-limit-exceeded");
}

async function runProcess(
  executable: string,
  args: readonly string[],
  timeoutMs = 30_000,
): Promise<{ readonly stdout: string; readonly stderr: string }> {
  const child = spawn(executable, args, {
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout?.on("data", (chunk: Buffer) => stdout.push(chunk));
  child.stderr?.on("data", (chunk: Buffer) => stderr.push(chunk));
  let code: number | null;
  try {
    code = await waitForChildClose(child, timeoutMs);
  } catch (error) {
    if (!child.killed) {
      child.kill();
    }
    child.stdout?.destroy();
    child.stderr?.destroy();
    throw error;
  }
  const out = Buffer.concat(stdout).toString("utf8");
  const err = Buffer.concat(stderr).toString("utf8");
  if (code !== 0) {
    throw new Error(err.trim() || `${executable} exited with code ${code}`);
  }
  return { stdout: out, stderr: err };
}

async function startLocalStandaloneEndpoint(
  executable: string,
): Promise<{
  readonly endpoint: CodeAgentEndpoint;
  readonly host: ChildProcess;
}> {
  const child = spawn(
    executable,
    [
      "agent",
      "host",
      "--new-instance",
      "--foreground",
      "--idle-timeout",
      "30",
    ],
    {
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    },
  );
  let stdout = "";
  let stderr = "";
  let spawnError: Error | undefined;
  let ownedEndpoint: CodeAgentEndpoint | undefined;
  const appendStdout = (chunk: Buffer): void => {
    stdout = appendBoundedOutput(stdout, chunk);
  };
  const appendStderr = (chunk: Buffer): void => {
    stderr = appendBoundedOutput(stderr, chunk);
  };
  const recordSpawnError = (error: Error): void => {
    spawnError = error;
  };
  child.stdout?.on("data", appendStdout);
  child.stderr?.on("data", appendStderr);
  child.once("error", recordSpawnError);

  const deadline = Date.now() + LOCAL_STANDALONE_START_TIMEOUT_MS;
  let lastEndpointError: unknown;
  try {
    for (;;) {
      if (spawnError) {
        throw spawnError;
      }
      if (child.exitCode !== null || child.signalCode !== null) {
        throw codedError(
          "standalone-host-exited",
          sanitizeHostOutput(stderr).trim() || "standalone-host-exited",
        );
      }

      try {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          throw codedError("standalone-endpoint-start-timeout");
        }
        const endpoints = await listCodeAgentEndpoints(
          executable,
          Math.min(30_000, remaining),
        );
        const output = `${stdout}\n${stderr}`;
        const startedToken = startedEndpointToken(output);
        const matching = endpoints.endpoints.find(
          (candidate) =>
            candidate.schemaVersion === 2 &&
            candidate.type === "standalone" &&
            ((startedToken !== undefined &&
              candidate.connectionToken === startedToken) ||
              (child.pid !== undefined && candidate.pid === child.pid)),
        );
        if (matching) {
          ownedEndpoint = matching;
          return {
            endpoint: selectStandaloneEndpoint([matching], output),
            host: child,
          };
        }
      } catch (error) {
        if (
          typeof error === "object" &&
          error !== null &&
          "code" in error &&
          error.code === "incompatible-standalone-protocol"
        ) {
          throw error;
        }
        lastEndpointError = error;
      }

      if (Date.now() >= deadline) {
        throw new Error("standalone-endpoint-start-timeout", {
          cause: lastEndpointError,
        });
      }
      await new Promise<void>((resolve) => {
        setTimeout(resolve, LOCAL_STANDALONE_POLL_INTERVAL_MS);
      });
    }
  } catch (error) {
    if (ownedEndpoint) {
      try {
        await stopOwnedLocalStandalone(executable, child, ownedEndpoint);
      } catch (cleanupError) {
        throw new AggregateError(
          [error, cleanupError],
          "standalone-start-cleanup-failed",
        );
      }
    } else {
      stopChildProcess(child);
    }
    throw error;
  }
}

function appendBoundedOutput(current: string, chunk: Buffer): string {
  const remaining = LOCAL_STANDALONE_OUTPUT_LIMIT - Buffer.byteLength(current);
  if (remaining <= 0) {
    return current;
  }
  return current + chunk.subarray(0, remaining).toString("utf8");
}

function startedEndpointToken(output: string): string | undefined {
  const startedUrl = output.match(/\bws:\/\/[^\s\u0000-\u001f]+/u)?.[0];
  if (!startedUrl) {
    return undefined;
  }
  try {
    return new URL(startedUrl).searchParams.get("tkn") ?? undefined;
  } catch {
    return undefined;
  }
}

function sanitizeHostOutput(output: string): string {
  return output.replace(/([?&]tkn=)[^&\s]+/gu, "$1[redacted]");
}

function stopChildProcess(child: ChildProcess | undefined): void {
  if (!child) {
    return;
  }
  if (
    child.exitCode === null &&
    child.signalCode === null &&
    !child.killed
  ) {
    child.kill();
  }
  child.stdin?.destroy();
  child.stdout?.destroy();
  child.stderr?.destroy();
}

async function waitForLocalStandaloneStopped(
  executable: string,
  entry: Pick<CodeAgentEndpoint, "instanceId" | "pid">,
  host: ChildProcess | undefined,
): Promise<boolean> {
  const deadline = Date.now() + 35_000;
  let staleRegistryCleanupAttempted = false;
  for (;;) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      return false;
    }
    let registered = true;
    try {
      const endpoints = await listCodeAgentEndpoints(
        executable,
        Math.min(5_000, remaining),
      );
      registered = endpoints.endpoints.some(
        (candidate) => candidate.instanceId === entry.instanceId,
      );
    } catch {
      registered = true;
    }
    const hostStopped =
      !host || host.exitCode !== null || host.signalCode !== null;
    const processStopped = hostStopped || !processIsAlive(entry.pid);
    if (!registered && processStopped) {
      return true;
    }
    if (
      registered &&
      processStopped &&
      !staleRegistryCleanupAttempted
    ) {
      staleRegistryCleanupAttempted = true;
      try {
        await runProcess(
          executable,
          ["agent", "kill", "--instance-id", entry.instanceId],
          10_000,
        );
      } catch {
        // The next registry read determines whether cleanup succeeded.
      }
      continue;
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, Math.min(250, deadline - Date.now()));
    });
  }
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (
      error instanceof Error &&
      "code" in error &&
      error.code === "EPERM"
    );
  }
}

async function runSshScript(
  sshExecutable: string,
  alias: string,
  script: string,
  args: readonly string[],
  timeoutMs = 90_000,
): Promise<string> {
  validateSshAlias(alias);
  const remoteCommand = ["sh", "-s", "--", ...args]
    .map(posixShellQuote)
    .join(" ");
  const child = spawn(
    sshExecutable,
    [
      "-T",
      "-o",
      "BatchMode=yes",
      "-o",
      "StrictHostKeyChecking=yes",
      "-o",
      "ConnectTimeout=60",
      alias,
      remoteCommand,
    ],
    {
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    },
  );
  if (!child.stdin) {
    child.kill();
    throw new Error("ssh-stdin-unavailable");
  }
  child.stdin.end(script, "utf8");
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout?.on("data", (chunk: Buffer) => stdout.push(chunk));
  child.stderr?.on("data", (chunk: Buffer) => stderr.push(chunk));
  let code: number | null;
  try {
    code = await waitForChildClose(child, timeoutMs);
  } catch (error) {
    if (!child.killed) {
      child.kill();
    }
    child.stdout?.destroy();
    child.stderr?.destroy();
    throw error;
  }
  const out = Buffer.concat(stdout).toString("utf8");
  const err = Buffer.concat(stderr).toString("utf8");
  if (code !== 0) {
    throw new Error(err.trim() || out.trim() || `ssh:${alias} exited with ${code}`);
  }
  return out;
}

function waitForChildClose(
  child: ChildProcess,
  timeoutMs?: number,
): Promise<number | null> {
  return waitForChild(child, "close", timeoutMs);
}

function waitForChild(
  child: ChildProcess,
  event: "close" | "exit",
  timeoutMs?: number,
): Promise<number | null> {
  return new Promise((resolve, reject) => {
    let timer: NodeJS.Timeout | undefined;
    const completed = (code: number | null): void => {
      cleanup();
      resolve(code);
    };
    const failed = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const cleanup = (): void => {
      if (timer) {
        clearTimeout(timer);
      }
      child.off(event, completed);
      child.off("error", failed);
    };
    child.once(event, completed);
    child.once("error", failed);
    if (timeoutMs !== undefined) {
      timer = setTimeout(() => {
        cleanup();
        if (!child.killed) {
          child.kill();
        }
        reject(codedError("agent-cli-timeout"));
      }, timeoutMs);
    }
  });
}

function validateSshAlias(alias: string): void {
  if (
    alias.length === 0 ||
    alias.length > 255 ||
    alias.startsWith("-") ||
    !/^[A-Za-z0-9._-]+$/u.test(alias)
  ) {
    throw codedError("invalid-ssh-alias");
  }
}

function posixShellQuote(value: string): string {
  if (/[\u0000\r\n]/u.test(value)) {
    throw codedError("invalid-remote-argument");
  }
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function parseHostKeyFingerprints(value: string): string[] {
  const fingerprints = [
    ...new Set(
      value
        .split(/\r?\n/u)
        .map((line) => line.trim())
        .filter(Boolean),
    ),
  ].sort();
  if (
    fingerprints.length === 0 ||
    fingerprints.some(
      (fingerprint) =>
        !fingerprint.startsWith("SHA256:") || /\s/u.test(fingerprint),
    )
  ) {
    throw codedError("remote-host-identity-invalid");
  }
  return fingerprints;
}

async function allocateLocalPort(): Promise<number> {
  const server = net.createServer();
  server.unref();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  server.close();
  if (!address || typeof address === "string") {
    throw new Error("allocate-port-failed");
  }
  return address.port;
}

async function waitForTunnelStart(
  tunnel: ChildProcess,
  localPort: number,
): Promise<void> {
  const deadline = Date.now() + 60_000;
  let spawnError: Error | undefined;
  const failed = (error: Error): void => {
    spawnError = error;
  };
  tunnel.on("error", failed);
  try {
    while (Date.now() < deadline) {
      if (spawnError) {
        throw spawnError;
      }
      if (tunnel.exitCode !== null) {
        throw new Error("ssh-tunnel-exited");
      }
      if (await canConnectToLocalPort(localPort)) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } finally {
    tunnel.off("error", failed);
  }
  throw new Error("ssh-tunnel-start-timeout");
}

function canConnectToLocalPort(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    const finish = (connected: boolean): void => {
      socket.removeAllListeners();
      socket.destroy();
      resolve(connected);
    };
    const timer = setTimeout(() => finish(false), 500);
    timer.unref();
    socket.once("connect", () => {
      clearTimeout(timer);
      finish(true);
    });
    socket.once("error", () => {
      clearTimeout(timer);
      finish(false);
    });
  });
}

export function defaultManagedProvider(
  rootState: Pick<RootState, "agents">,
): string {
  for (const provider of PREFERRED_MANAGED_PROVIDERS) {
    if (rootState.agents.some((agent) => agent.provider === provider)) {
      return provider;
    }
  }
  if (rootState.agents.length === 1 && rootState.agents[0]) {
    return rootState.agents[0].provider;
  }
  throw codedError("provider-selection-required");
}

function codedError(
  code: string,
  message = code,
): Error & { readonly code: string } {
  const error = new Error(message) as Error & { readonly code: string };
  Object.defineProperty(error, "code", {
    configurable: false,
    enumerable: true,
    value: code,
    writable: false,
  });
  return error;
}
