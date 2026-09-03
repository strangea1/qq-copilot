import { createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import net from "node:net";
import { pathToFileURL } from "node:url";

import {
  SUPPORTED_PROTOCOL_VERSIONS,
  type ConfigPropertySchema,
  type ConfigSchema,
  type RootState,
  type SessionSummary,
  type URI,
} from "@microsoft/agent-host-protocol";
import {
  AhpClient,
  type JsonRpcMessage,
  TransportError,
  type AhpTransport,
  type TransportFrame,
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

export interface SessionConfigOption {
  readonly value: unknown;
  readonly label: string;
  readonly description?: string;
}

export interface SupportedSessionField {
  readonly property: string;
  readonly options: readonly SessionConfigOption[];
  readonly selected: unknown;
}

export interface PrepareTargetResult {
  readonly endpoint_id: string;
  readonly host_instance_id: string;
  readonly provider: string;
  readonly workspace_uri: string;
  readonly host_label: string;
  readonly editor_client_tools_available: boolean;
  readonly resolved_values: unknown;
  readonly model?: SupportedSessionField;
  readonly approval?: SupportedSessionField;
}

export interface CreateSessionResult {
  readonly endpoint_id: string;
  readonly host_instance_id: string;
  readonly workspace_uri: string;
  readonly host_label: string;
  readonly editor_client_tools_available: boolean;
  readonly session: SessionSummary;
}

export async function prepareTargetResult(
  connection: ConnectedManagedTarget,
  advanced: boolean,
  config: Record<string, unknown> = {},
): Promise<PrepareTargetResult> {
  const provider = defaultProvider(connection.rootState);
  const resolved = await resolveSessionConfig(
    connection.client,
    provider,
    connection.prepared.workspaceUri,
    config,
  );
  if (
    !advanced &&
    ((resolved.model &&
      fieldNeedsExplicitSelection(resolved.model, resolved.values)) ||
      (resolved.approval &&
        fieldNeedsExplicitSelection(resolved.approval, resolved.values)))
  ) {
    throw codedError(
      "requires-new-advanced",
      "Quick mode can only use host defaults; use /new advanced for this target.",
    );
  }
  return {
    endpoint_id: connection.prepared.endpointId,
    host_instance_id: connection.prepared.entry.instanceId,
    provider,
    workspace_uri: connection.prepared.workspaceUri,
    host_label: connection.prepared.hostLabel,
    editor_client_tools_available:
      connection.prepared.editorClientToolsAvailable,
    resolved_values: resolved.values,
    ...(advanced && resolved.model ? { model: resolved.model } : {}),
    ...(advanced && resolved.approval ? { approval: resolved.approval } : {}),
  };
}

export async function createManagedSession(
  connection: ConnectedManagedTarget,
  request: {
    readonly provider: string;
    readonly sessionUri: string;
    readonly workspaceUri: string;
    readonly resolvedValues: unknown;
    readonly overrides: unknown;
  },
): Promise<CreateSessionResult> {
  const mergedConfig = mergeConfigValues(request.resolvedValues, request.overrides);
  const resolved = await resolveSessionConfig(
    connection.client,
    request.provider,
    request.workspaceUri,
    mergedConfig,
  );
  if (
    (resolved.model &&
      fieldNeedsExplicitSelection(resolved.model, mergedConfig) &&
      fieldNeedsExplicitSelection(resolved.model, resolved.values)) ||
    (resolved.approval &&
      fieldNeedsExplicitSelection(resolved.approval, mergedConfig) &&
      fieldNeedsExplicitSelection(resolved.approval, resolved.values))
  ) {
    throw codedError(
      "requires-explicit-selection",
      "The target still requires an explicit model or approval selection.",
    );
  }
  const existing = (await refreshManagedSessions(connection.client)).find(
    (session) => session.resource === request.sessionUri,
  );
  if (existing) {
    requireMatchingCreatedSession(connection, request, existing);
    return managedSessionResult(connection, existing);
  }
  const progressToken = `create-${request.sessionUri}`;
  try {
    connection.setProgressToken(progressToken);
    try {
      await connection.client.request("createSession", {
        channel: request.sessionUri,
        provider: request.provider,
        workingDirectories: [request.workspaceUri],
        config: asRecord(resolved.values),
        progressToken,
      });
    } catch (requestError) {
      let reconciled: SessionSummary | undefined;
      try {
        reconciled = await waitForManagedSession(
          connection.client,
          request.sessionUri,
        );
      } catch (reconciliationError) {
        throw new AggregateError(
          [requestError, reconciliationError],
          "created-session-reconciliation-failed",
        );
      }
      if (reconciled) {
        requireMatchingCreatedSession(connection, request, reconciled);
        return managedSessionResult(connection, reconciled);
      }
      throw requestError;
    } finally {
      connection.setProgressToken(undefined);
    }
    const session = await waitForManagedSession(
      connection.client,
      request.sessionUri,
    );
    if (session) {
      requireMatchingCreatedSession(connection, request, session);
      return managedSessionResult(connection, session);
    }
    throw new Error("created-session-not-listed");
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "session-uri-conflict"
    ) {
      throw error;
    }
    try {
      await disposeManagedSession(connection, request.sessionUri);
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "created-session-cleanup-failed",
      );
    }
    throw error;
  }
}

async function waitForManagedSession(
  client: AhpClient,
  sessionUri: string,
): Promise<SessionSummary | undefined> {
  for (let attempt = 0; attempt < 25; attempt += 1) {
    const session = (await refreshManagedSessions(client)).find(
      (candidate) => candidate.resource === sessionUri,
    );
    if (session) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  return undefined;
}

function requireMatchingCreatedSession(
  connection: ConnectedManagedTarget,
  request: {
    readonly provider: string;
    readonly workspaceUri: string;
  },
  session: SessionSummary,
): void {
  if (
    session.provider !== request.provider ||
    !session.workingDirectories?.some((workspaceUri) =>
      managedTargetMatchesWorkspaceUri(connection.prepared.target, workspaceUri),
    )
  ) {
    throw codedError("session-uri-conflict");
  }
}

export async function disposeManagedSession(
  connection: ConnectedManagedTarget,
  sessionUri: string,
): Promise<void> {
  const exists = (await refreshManagedSessions(connection.client)).some(
    (session) => session.resource === sessionUri,
  );
  if (!exists) {
    return;
  }
  await connection.client.request("disposeSession", {
    channel: sessionUri,
  });
}

function managedSessionResult(
  connection: ConnectedManagedTarget,
  session: SessionSummary,
): CreateSessionResult {
  return {
    endpoint_id: connection.prepared.endpointId,
    host_instance_id: connection.prepared.entry.instanceId,
    workspace_uri: connection.prepared.workspaceUri,
    host_label: connection.prepared.hostLabel,
    editor_client_tools_available:
      connection.prepared.editorClientToolsAvailable,
    session,
  };
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

interface ProgressNotification {
  readonly channel: string;
  readonly progressToken: string;
  readonly progress: number;
  readonly total?: number;
  readonly message?: string;
}

interface PreparedTarget {
  readonly target: ManagedTarget;
  readonly entry: EndpointRegistryEntry;
  readonly endpointId: string;
  readonly hostLabel: string;
  readonly workspaceUri: string;
  readonly editorClientToolsAvailable: boolean;
  readonly tunnel?: ChildProcess;
}

const NEGOTIATED_PROTOCOL_VERSIONS = [...SUPPORTED_PROTOCOL_VERSIONS];
const MANAGED_REQUEST_TIMEOUT_MS = 295_000;

export interface ConnectedManagedTarget {
  readonly prepared: PreparedTarget;
  readonly client: AhpClient;
  readonly rootState: RootState;
  readonly sessions: readonly SessionSummary[];
  readonly setProgressToken: (token: string | undefined) => void;
  close(): Promise<void>;
}

export async function connectManagedTarget(
  config: AdapterConfig,
  target: ManagedTarget,
  onProgress?: (progress: ProgressNotification) => Promise<void> | void,
): Promise<ConnectedManagedTarget> {
  const prepared = await prepareManagedTarget(config, target);
  let client: AhpClient | undefined;
  try {
    const transport = new ProgressTapTransport(
      await openEndpointTransport(
        prepared.entry.endpoint,
        prepared.entry.connectionToken,
      ),
      onProgress,
    );
    client = new AhpClient(transport, {
      requestTimeoutMs: MANAGED_REQUEST_TIMEOUT_MS,
    });
    client.connect();
    const initialized = await client.request("initialize", {
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
    });
    if (!NEGOTIATED_PROTOCOL_VERSIONS.includes(initialized.protocolVersion)) {
      throw codedError(
        "incompatible-protocol",
        `incompatible-protocol:${initialized.protocolVersion}`,
      );
    }
    const rootState = extractRootState(initialized.snapshots);
    const sessions = await listSessions(client);
    const connectedClient = client;
    return {
      prepared,
      client: connectedClient,
      rootState,
      sessions,
      setProgressToken: (token) => transport.setProgressToken(token),
      async close(): Promise<void> {
        await connectedClient.shutdown().catch(() => undefined);
        if (prepared.tunnel && !prepared.tunnel.killed) {
          prepared.tunnel.kill();
        }
      },
    };
  } catch (error) {
    await client?.shutdown().catch(() => undefined);
    if (prepared.tunnel && !prepared.tunnel.killed) {
      prepared.tunnel.kill();
    }
    throw error;
  }
}

export async function refreshManagedSessions(
  client: AhpClient,
): Promise<readonly SessionSummary[]> {
  return listSessions(client);
}

function extractRootState(snapshots: readonly unknown[] | undefined): RootState {
  for (const snapshot of snapshots ?? []) {
    if (
      typeof snapshot === "object" &&
      snapshot !== null &&
      "resource" in snapshot &&
      snapshot.resource === "ahp-root://" &&
      "state" in snapshot &&
      typeof snapshot.state === "object" &&
      snapshot.state !== null
    ) {
      return snapshot.state as RootState;
    }
  }
  return { agents: [] };
}

async function prepareManagedTarget(
  config: AdapterConfig,
  target: ManagedTarget,
): Promise<PreparedTarget> {
  if (target.kind === "local") {
    if (!config.codeExecutable) {
      throw codedError("code-not-configured");
    }
    const started = await runProcessUntilExit(config.codeExecutable, [
      "agent",
      "host",
    ]);
    const endpoints = await listCodeAgentEndpoints(config.codeExecutable);
    const endpoint = selectStandaloneEndpoint(
      endpoints.endpoints,
      `${started.stdout}\n${started.stderr}`,
    );
    const entry = toRegistryEntry(endpoint);
    return {
      target,
      entry,
      endpointId: endpointPublicId(entry),
      hostLabel: "local",
      workspaceUri: fileUri(target.path),
      editorClientToolsAvailable: false,
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
): Promise<CodeAgentEndpointsDocument> {
  const output = await runProcess(codeExecutable, ["agent", "endpoints"]);
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
  const compatible = endpoints.filter(
    (candidate) =>
      candidate.schemaVersion === 2 &&
      candidate.type === "standalone" &&
      NEGOTIATED_PROTOCOL_VERSIONS.includes(candidate.protocolVersion),
  );
  if (startedOutput) {
    const startedUrl = startedOutput.match(/\bws:\/\/[^\s]+/u)?.[0];
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
    throw codedError("standalone-endpoint-not-found");
  }
  throw codedError("standalone-endpoint-ambiguous");
}

function toRegistryEntry(endpoint: CodeAgentEndpoint): EndpointRegistryEntry {
  const identity = `${endpoint.type}\0${endpoint.pid}\0${endpoint.instanceId}`;
  const sourceFile = `${createHash("sha256").update(identity, "utf8").digest("hex")}.json`;
  return {
    schemaVersion: 2,
    type: endpoint.type,
    pid: endpoint.pid,
    instanceId: endpoint.instanceId,
    protocolVersion: endpoint.protocolVersion,
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
    return workspaceUri.toLocaleLowerCase("en-US") ===
      fileUri(target.path).toLocaleLowerCase("en-US");
  }
  let candidate: URL;
  try {
    candidate = new URL(workspaceUri);
  } catch {
    return false;
  }
  if (
    candidate.protocol === "vscode-remote:" &&
    candidate.host.toLocaleLowerCase("en-US") !==
      `ssh-remote+${target.alias}`.toLocaleLowerCase("en-US")
  ) {
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
  if (candidate.search !== "" || candidate.hash !== "") {
    return false;
  }
  try {
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
  for (let page = 0; page < 100; page += 1) {
    const result = await client.request("listSessions", {
      channel: "ahp-root://",
      limit: 100,
      ...(cursor ? { cursor } : {}),
    });
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
    code = await waitForChildClose(child);
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

async function runProcessUntilExit(
  executable: string,
  args: readonly string[],
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
    code = await waitForChildExit(child);
  } catch (error) {
    if (!child.killed) {
      child.kill();
    }
    throw error;
  } finally {
    child.stdout?.destroy();
    child.stderr?.destroy();
  }
  if (code !== 0) {
    const error = Buffer.concat(stderr).toString("utf8").trim();
    throw new Error(error || `${executable} exited with code ${code}`);
  }
  return {
    stdout: Buffer.concat(stdout).toString("utf8"),
    stderr: Buffer.concat(stderr).toString("utf8"),
  };
}

async function runSshScript(
  sshExecutable: string,
  alias: string,
  script: string,
  args: readonly string[],
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
    code = await waitForChildClose(child);
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

function waitForChildClose(child: ChildProcess): Promise<number | null> {
  return waitForChild(child, "close");
}

function waitForChildExit(child: ChildProcess): Promise<number | null> {
  return waitForChild(child, "exit");
}

function waitForChild(
  child: ChildProcess,
  event: "close" | "exit",
): Promise<number | null> {
  return new Promise((resolve, reject) => {
    const completed = (code: number | null): void => {
      cleanup();
      resolve(code);
    };
    const failed = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const cleanup = (): void => {
      child.off(event, completed);
      child.off("error", failed);
    };
    child.once(event, completed);
    child.once("error", failed);
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

class ProgressTapTransport implements AhpTransport {
  readonly #inner: AhpTransport;
  readonly #onProgress:
    | ((progress: ProgressNotification) => Promise<void> | void)
    | undefined;
  #progressToken: string | undefined;

  constructor(
    inner: AhpTransport,
    onProgress?: (progress: ProgressNotification) => Promise<void> | void,
  ) {
    this.#inner = inner;
    this.#onProgress = onProgress;
  }

  setProgressToken(token: string | undefined): void {
    this.#progressToken = token;
  }

  send(message: JsonRpcMessage | string): Promise<void> | void {
    return this.#inner.send(message);
  }

  async recv(): Promise<TransportFrame | null> {
    const frame = await this.#inner.recv();
    if (!frame) {
      return frame;
    }
    const notification = readProgressNotification(frame);
    if (
      notification &&
      this.#onProgress &&
      notification.progressToken === this.#progressToken
    ) {
      await this.#onProgress(notification);
    }
    return frame;
  }

  close(): Promise<void> | void {
    return this.#inner.close();
  }
}

function readProgressNotification(
  frame: TransportFrame,
): ProgressNotification | undefined {
  try {
    const message =
      frame.kind === "parsed"
        ? frame.message
        : JSON.parse(
            frame.kind === "text"
              ? frame.text
              : Buffer.from(frame.data).toString("utf8"),
          );
    if (
      !message ||
      typeof message !== "object" ||
      message.method !== "root/progress" ||
      typeof message.params !== "object" ||
      message.params === null
    ) {
      return undefined;
    }
    const params = message.params as Record<string, unknown>;
    if (
      typeof params.channel !== "string" ||
      typeof params.progressToken !== "string" ||
      typeof params.progress !== "number"
    ) {
      return undefined;
    }
    return {
      channel: params.channel,
      progressToken: params.progressToken,
      progress: params.progress,
      ...(typeof params.total === "number" ? { total: params.total } : {}),
      ...(typeof params.message === "string" ? { message: params.message } : {}),
    };
  } catch (error) {
    if (error instanceof TransportError) {
      throw error;
    }
    return undefined;
  }
}

function defaultProvider(rootState: RootState): string {
  const copilot = rootState.agents.find(
    (agent) => agent.provider === "copilot",
  );
  if (copilot) {
    return copilot.provider;
  }
  if (rootState.agents.length === 1 && rootState.agents[0]) {
    return rootState.agents[0].provider;
  }
  throw codedError("provider-selection-required");
}

async function resolveSessionConfig(
  client: AhpClient,
  provider: string,
  workspaceUri: string,
  config: Record<string, unknown>,
): Promise<{
  readonly values: Record<string, unknown>;
  readonly model?: SupportedSessionField;
  readonly approval?: SupportedSessionField;
}> {
  const result = await client.request("resolveSessionConfig", {
    channel: "ahp-root://",
    provider,
    workingDirectory: workspaceUri,
    config,
  });
  const schema = asSchema(result.schema);
  const values = asRecord(result.values);
  const model = await supportedField(
    client,
    provider,
    workspaceUri,
    schema,
    values,
    /model/iu,
  );
  const approval = await supportedField(
    client,
    provider,
    workspaceUri,
    schema,
    values,
    /approval|confirm|permission/iu,
  );
  const required = schema.required ?? [];
  for (const property of required) {
    if (
      !(property in values) &&
      property !== model?.property &&
      property !== approval?.property
    ) {
      throw codedError(
        `unsupported-required-config-${property}`,
        `unsupported-required-config:${property}`,
      );
    }
  }
  return {
    values,
    ...(model ? { model } : {}),
    ...(approval ? { approval } : {}),
  };
}

async function supportedField(
  client: AhpClient,
  provider: string,
  workspaceUri: string,
  schema: ConfigSchema,
  values: Record<string, unknown>,
  matcher: RegExp,
): Promise<SupportedSessionField | undefined> {
  for (const [property, descriptor] of Object.entries(schema.properties)) {
    if (
      !matcher.test(property) &&
      !matcher.test(descriptor.title) &&
      !(descriptor.description && matcher.test(descriptor.description))
    ) {
      continue;
    }
    const options = await propertyOptions(
      client,
      provider,
      workspaceUri,
      property,
      descriptor,
      values,
    );
    if (!options.length) {
      continue;
    }
    const fallbackValue =
      property in values
        ? values[property]
        : descriptor.default !== undefined
          ? structuredClone(descriptor.default)
          : options[0]?.value;
    if (fallbackValue === undefined) {
      continue;
    }
    return {
      property,
      options,
      selected: fallbackValue,
    };
  }
  return undefined;
}

function fieldNeedsExplicitSelection(
  field: SupportedSessionField,
  values: Record<string, unknown>,
): boolean {
  return !(field.property in values);
}

async function propertyOptions(
  client: AhpClient,
  provider: string,
  workspaceUri: string,
  property: string,
  descriptor: ConfigPropertySchema,
  values: Record<string, unknown>,
): Promise<readonly SessionConfigOption[]> {
  if (Array.isArray(descriptor.enum) && descriptor.enum.length > 0) {
    return descriptor.enum.map((value, index) => ({
      value,
      label:
        typeof descriptor.enumLabels?.[index] === "string"
          ? descriptor.enumLabels[index]
          : String(value),
      ...(typeof descriptor.enumDescriptions?.[index] === "string"
        ? { description: descriptor.enumDescriptions[index] }
        : {}),
    }));
  }
  if (!("enumDynamic" in descriptor) || descriptor.enumDynamic !== true) {
    return [];
  }
  const result = await client.request("sessionConfigCompletions", {
    channel: "ahp-root://",
    provider,
    workingDirectory: workspaceUri,
    config: values,
    property,
    query: "",
  });
  return result.items.map((item) => ({
    value: item.value,
    label: item.label,
    ...(item.description ? { description: item.description } : {}),
  }));
}

function mergeConfigValues(
  base: unknown,
  overrides: unknown,
): Record<string, unknown> {
  return {
    ...asRecord(base),
    ...asRecord(overrides),
  };
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("invalid-config-record");
  }
  return value as Record<string, unknown>;
}

function asSchema(value: unknown): ConfigSchema {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    (value as { type?: unknown }).type !== "object" ||
    typeof (value as { properties?: unknown }).properties !== "object" ||
    (value as { properties?: unknown }).properties === null
  ) {
    throw new Error("invalid-config-schema");
  }
  return value as ConfigSchema;
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
