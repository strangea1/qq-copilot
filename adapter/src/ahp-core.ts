import { randomUUID } from "node:crypto";

import {
  ActionType,
  ChatInputResponseKind,
  ConfirmationOptionKind,
  MessageKind,
  PendingMessageKind,
  SessionInputRequestKind,
  ToolCallCancellationReason,
  ToolCallConfirmationReason,
  ToolCallStatus,
  SUPPORTED_PROTOCOL_VERSIONS,
  type ActionEnvelope,
  type ChatInputAnswer,
  type ChatState,
  type ChatToolCallConfirmedAction,
  type ChatToolCallResultConfirmedAction,
  type InitializeParams,
  type Message,
  type SessionChatInputRequest,
  type SessionInputRequest,
  type SessionState,
  type SessionSummary,
  type SessionToolConfirmationRequest,
  type StateAction,
  type ToolCallPendingConfirmationState,
  type ToolCallPendingResultConfirmationState,
  type URI,
} from "@microsoft/agent-host-protocol";
import {
  AhpClient,
  ClientClosedError,
  RpcError,
  RpcTimeoutError,
  TransportError,
  type AhpClientConfig,
  type AhpTransport,
  type Subscription,
  type SubscriptionEvent,
} from "@microsoft/agent-host-protocol/client";

import {
  discoverEditorEndpoints,
  endpointPublicId,
  watchEditorEndpoints,
  type EndpointRegistryEntry,
  type WatchEditorEndpointsOptions,
} from "./endpoint-registry.js";
import { openNamedPipeTransport } from "./named-pipe-transport.js";
import {
  MirrorSnapshotError,
  ProviderSessionStateMirror,
  type MirrorApplyResult,
} from "./provider-state-mirror.js";
import { normalizeLegacyActionEnvelope } from "./protocol-compatibility.js";

const ROOT: "ahp-root://" = "ahp-root://";
const MAX_SESSION_PAGES = 100;
const SESSION_PAGE_SIZE = 100;
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const DEFAULT_SUBSCRIPTION_BUFFER = 16_384;
const DEFAULT_RETRY_BASE_MS = 250;
const MAX_RETRY_MS = 30_000;

export interface PublicEndpoint {
  readonly id: string;
  readonly pid: number;
  readonly instanceId: string;
  readonly advertisedProtocol: string;
}

export type EndpointConnectionStatus =
  | "connecting"
  | "connected"
  | "disconnected"
  | "incompatible";

export interface EndpointCatalogue {
  readonly endpoint: PublicEndpoint;
  readonly connection: EndpointConnectionStatus;
  readonly selectedProtocol?: string;
  readonly sessions: readonly SessionSummary[];
}

export interface CatalogueSnapshot {
  readonly revision: number;
  readonly endpoints: readonly EndpointCatalogue[];
}

export interface ConnectionEvent {
  readonly endpoint: PublicEndpoint;
  readonly status: EndpointConnectionStatus;
  readonly selectedProtocol?: string;
}

export type IncompatibilityReason =
  | "advertised-version"
  | "negotiated-version";

export interface IncompatibilityEvent {
  readonly endpoint: PublicEndpoint;
  readonly reason: IncompatibilityReason;
  readonly supportedProtocols: readonly string[];
  readonly selectedProtocol?: string;
}

export interface SessionSnapshotEvent {
  readonly endpointId: string;
  readonly sessionUri: URI;
  readonly provider: string;
  readonly serverSeq: number;
  readonly state: SessionState;
}

export interface ChatSnapshotEvent {
  readonly endpointId: string;
  readonly sessionUri: URI;
  readonly provider: string;
  readonly chatUri: URI;
  readonly serverSeq: number;
  readonly state: ChatState;
}

export type DomainActionEvent =
  | {
      readonly scope: "root";
      readonly endpointId: string;
      readonly envelope: ActionEnvelope;
    }
  | {
      readonly scope: "session";
      readonly endpointId: string;
      readonly sessionUri: URI;
      readonly provider: string;
      readonly envelope: ActionEnvelope;
    }
  | {
      readonly scope: "chat";
      readonly endpointId: string;
      readonly sessionUri: URI;
      readonly provider: string;
      readonly chatUri: URI;
      readonly envelope: ActionEnvelope;
    };

export type CoreErrorOperation =
  | "bind"
  | "callback"
  | "connect"
  | "endpoint-watch"
  | "list-sessions"
  | "session-stream"
  | "chat-stream";

export interface CoreErrorEvent {
  readonly operation: CoreErrorOperation;
  readonly code: string;
  readonly message: string;
  readonly endpointId?: string;
  readonly sessionUri?: URI;
  readonly chatUri?: URI;
}

export interface AhpCoreCallbacks {
  readonly onCatalogue?: (snapshot: CatalogueSnapshot) => void;
  readonly onConnection?: (event: ConnectionEvent) => void;
  readonly onSessionSnapshot?: (event: SessionSnapshotEvent) => void;
  readonly onChatSnapshot?: (event: ChatSnapshotEvent) => void;
  readonly onAction?: (event: DomainActionEvent) => void;
  readonly onIncompatibility?: (event: IncompatibilityEvent) => void;
  readonly onError?: (event: CoreErrorEvent) => void;
}

export interface AhpCoreDependencies {
  readonly discoverEndpoints?: (
    userDataDirectory: string,
  ) => Promise<readonly EndpointRegistryEntry[]>;
  readonly watchEndpoints?: (
    userDataDirectory: string,
    options?: WatchEditorEndpointsOptions,
  ) => AsyncIterable<readonly EndpointRegistryEntry[]>;
  readonly openTransport?: (
    endpoint: EndpointRegistryEntry,
  ) => Promise<AhpTransport>;
  readonly createClient?: (
    transport: AhpTransport,
    config: AhpClientConfig,
  ) => AhpClient;
  readonly createId?: () => string;
  readonly monotonicNow?: () => number;
}

export interface AhpCoreOptions {
  readonly userDataDirectory: string;
  readonly clientId: string;
  readonly locale?: string;
  readonly callbacks?: AhpCoreCallbacks;
  readonly watch?: boolean;
  readonly watchIntervalMs?: number;
  readonly requestTimeoutMs?: number;
  readonly subscriptionBuffer?: number;
  readonly dependencies?: AhpCoreDependencies;
}

export type AhpOperationErrorCode =
  | "already-closed"
  | "ambiguous-input"
  | "binding-unavailable"
  | "chat-unavailable"
  | "endpoint-not-found"
  | "invalid-command"
  | "invalid-confirmation-option"
  | "no-active-turn"
  | "pending-input-not-found"
  | "pending-tool-not-found"
  | "session-not-found";

export class AhpOperationError extends Error {
  readonly code: AhpOperationErrorCode;

  constructor(code: AhpOperationErrorCode, message: string) {
    super(message);
    this.name = "AhpOperationError";
    this.code = code;
  }
}

export interface QueueUserTextResult {
  readonly disposition: "started" | "queued";
  readonly id: string;
  readonly clientSeq: number;
}

export interface CancelTurnResult {
  readonly turnId: string;
  readonly clientSeq: number;
}

export type ReviewToolParametersCommand =
  | {
      readonly requestId: string;
      readonly decision: "approve";
      readonly confirmed?: ToolCallConfirmationReason.UserAction | ToolCallConfirmationReason.Setting;
      readonly editedToolInput?: string;
      readonly selectedOptionId?: string;
    }
  | {
      readonly requestId: string;
      readonly decision: "deny";
      readonly reason?: ToolCallCancellationReason.Denied | ToolCallCancellationReason.Skipped;
      readonly reasonMessage?: string;
      readonly userSuggestion?: string;
      readonly selectedOptionId?: string;
    };

export interface ReviewToolResultCommand {
  readonly requestId: string;
  readonly approved: boolean;
}

export interface CompleteCurrentInputCommand {
  readonly requestId?: string;
  readonly response: ChatInputResponseKind;
  readonly answers?: Record<string, ChatInputAnswer>;
}

export interface ActionDispatchResult {
  readonly clientSeq: number;
}

export interface BoundSessionSnapshot {
  readonly endpointId: string;
  readonly sessionUri: URI;
  readonly provider: string;
  readonly session?: SessionState;
  readonly defaultChat?: ChatState;
}

export interface AhpSessionBinding {
  readonly endpointId: string;
  readonly sessionUri: URI;
  readonly provider: string;
  snapshot(): BoundSessionSnapshot;
  queueUserText(
    text: string,
    clientSeq?: number,
  ): Promise<QueueUserTextResult>;
  cancelActiveTurn(clientSeq?: number): Promise<CancelTurnResult>;
  reviewToolParameters(
    command: ReviewToolParametersCommand,
    clientSeq?: number,
  ): Promise<ActionDispatchResult>;
  reviewToolResult(
    command: ReviewToolResultCommand,
    clientSeq?: number,
  ): Promise<ActionDispatchResult>;
  completeCurrentInput(
    command: CompleteCurrentInputCommand,
    clientSeq?: number,
  ): Promise<ActionDispatchResult>;
  close(): Promise<void>;
}

type DiscoverEndpoints = NonNullable<
  AhpCoreDependencies["discoverEndpoints"]
>;
type WatchEndpoints = NonNullable<AhpCoreDependencies["watchEndpoints"]>;
type OpenTransport = NonNullable<AhpCoreDependencies["openTransport"]>;
type CreateClient = NonNullable<AhpCoreDependencies["createClient"]>;

interface EndpointConnection {
  readonly client: AhpClient;
  readonly rootSubscription: Subscription;
  readonly catalogueQueue: SerialTaskQueue;
  readonly selectedProtocol: string;
  closed: boolean;
}

interface EndpointRecord {
  entry: EndpointRegistryEntry;
  endpoint: PublicEndpoint;
  readonly sessions: Map<URI, SessionSummary>;
  status: EndpointConnectionStatus;
  selectedProtocol: string | undefined;
  connection: EndpointConnection | undefined;
  connectTask: Promise<void> | undefined;
  retryAttempt: number;
  retryTimer: NodeJS.Timeout | undefined;
  present: boolean;
}

interface BindingRuntime {
  readonly clientId: string;
  readonly createId: () => string;
  readonly monotonicNow: () => number;
  readonly connection: () => EndpointConnection | undefined;
  readonly sanitize: <T>(value: T) => T;
  readonly emitSession: (state: SessionState, sequence: number) => void;
  readonly emitChat: (state: ChatState, sequence: number) => void;
  readonly emitAction: (
    scope: "session" | "chat",
    envelope: ActionEnvelope,
    chatUri?: URI,
  ) => void;
  readonly emitError: (
    operation: "bind" | "session-stream" | "chat-stream",
    error: unknown,
    chatUri?: URI,
  ) => void;
  readonly release: (binding: SessionBinding) => void;
}

type SessionParameterConfirmationRequest = Omit<
  SessionToolConfirmationRequest,
  "toolCall"
> & {
  readonly toolCall: ToolCallPendingConfirmationState;
};

type SessionResultConfirmationRequest = Omit<
  SessionToolConfirmationRequest,
  "toolCall"
> & {
  readonly toolCall: ToolCallPendingResultConfirmationState;
};

class ProtocolGateError extends Error {
  readonly selectedProtocol: string;

  constructor(selectedProtocol: string) {
    super("Agent Host selected an incompatible protocol");
    this.name = "ProtocolGateError";
    this.selectedProtocol = selectedProtocol;
  }
}

class SerialTaskQueue {
  #tail: Promise<void> = Promise.resolve();

  run<T>(task: () => Promise<T> | T): Promise<T> {
    const result = this.#tail.then(task, task);
    this.#tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

export class AhpCore {
  readonly #userDataDirectory: string;

  readonly #clientId: string;

  readonly #locale: string | undefined;

  readonly #callbacks: AhpCoreCallbacks;

  readonly #watchEnabled: boolean;

  readonly #watchIntervalMs: number;

  readonly #clientConfig: AhpClientConfig;

  readonly #discoverEndpoints: DiscoverEndpoints;

  readonly #watchEndpoints: WatchEndpoints;

  readonly #openTransport: OpenTransport;

  readonly #createClient: CreateClient;

  readonly #createId: () => string;

  readonly #monotonicNow: () => number;

  readonly #records = new Map<string, EndpointRecord>();

  readonly #bindings = new Map<string, SessionBinding>();

  #revision = 0;

  #lifecycle: "idle" | "running" | "stopped" = "idle";

  #watchAbort: AbortController | undefined;

  #watchTask: Promise<void> | undefined;

  constructor(options: AhpCoreOptions) {
    if (options.userDataDirectory.length === 0) {
      throw new TypeError("userDataDirectory must not be empty");
    }
    if (
      options.clientId.length < 8 ||
      options.clientId.length > 256 ||
      /[\u0000-\u001f\u007f]/u.test(options.clientId)
    ) {
      throw new TypeError(
        "clientId must be a stable 8-256 character identifier",
      );
    }
    const watchIntervalMs = options.watchIntervalMs ?? 1_000;
    if (!Number.isFinite(watchIntervalMs) || watchIntervalMs <= 0) {
      throw new RangeError("watchIntervalMs must be a positive finite number");
    }

    this.#userDataDirectory = options.userDataDirectory;
    this.#clientId = options.clientId;
    this.#locale = options.locale;
    this.#callbacks = options.callbacks ?? {};
    this.#watchEnabled = options.watch ?? true;
    this.#watchIntervalMs = watchIntervalMs;
    this.#clientConfig = {
      requestTimeoutMs:
        options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      subscriptionBuffer:
        options.subscriptionBuffer ?? DEFAULT_SUBSCRIPTION_BUFFER,
    };
    this.#discoverEndpoints =
      options.dependencies?.discoverEndpoints ?? discoverEditorEndpoints;
    this.#watchEndpoints =
      options.dependencies?.watchEndpoints ?? watchEditorEndpoints;
    this.#openTransport =
      options.dependencies?.openTransport ?? defaultOpenTransport;
    this.#createClient =
      options.dependencies?.createClient ??
      ((transport, config) => new AhpClient(transport, config));
    this.#createId = options.dependencies?.createId ?? randomUUID;
    this.#monotonicNow =
      options.dependencies?.monotonicNow ?? (() => performance.now());
  }

  get catalogue(): CatalogueSnapshot {
    return this.#catalogueSnapshot();
  }

  async start(): Promise<CatalogueSnapshot> {
    if (this.#lifecycle === "stopped") {
      throw new AhpOperationError(
        "already-closed",
        "This AHP core has already been stopped",
      );
    }
    if (this.#lifecycle === "running") {
      return this.catalogue;
    }
    this.#lifecycle = "running";
    await this.refreshEndpoints();

    if (this.#watchEnabled) {
      const abort = new AbortController();
      this.#watchAbort = abort;
      this.#watchTask = this.#runEndpointWatch(abort.signal);
    }
    return this.catalogue;
  }

  async refreshEndpoints(): Promise<CatalogueSnapshot> {
    if (this.#lifecycle === "stopped") {
      throw new AhpOperationError(
        "already-closed",
        "This AHP core has already been stopped",
      );
    }
    try {
      const entries = await this.#discoverEndpoints(this.#userDataDirectory);
      await this.#reconcileEndpoints(entries);
    } catch (error) {
      this.#emitError("endpoint-watch", error);
    }
    return this.catalogue;
  }

  async listSessions(): Promise<CatalogueSnapshot> {
    const refreshes: Promise<void>[] = [];
    for (const record of this.#records.values()) {
      const connection = record.connection;
      if (connection && !connection.closed) {
        refreshes.push(this.#refreshSessions(record, connection));
      }
    }
    await Promise.all(refreshes);
    this.#emitCatalogue();
    return this.catalogue;
  }

  async bindSession(
    endpointId: string,
    sessionUri: URI,
  ): Promise<AhpSessionBinding> {
    const key = bindingKey(endpointId, sessionUri);
    const existing = this.#bindings.get(key);
    if (existing) {
      return existing;
    }

    const record = this.#records.get(endpointId);
    if (!record || !record.present) {
      throw new AhpOperationError(
        "endpoint-not-found",
        "The requested Agent Host endpoint is not available",
      );
    }
    const connection = record.connection;
    if (!connection || connection.closed) {
      throw new AhpOperationError(
        "binding-unavailable",
        "The requested Agent Host endpoint is not connected",
      );
    }

    let summary = record.sessions.get(sessionUri);
    if (!summary) {
      await this.#refreshSessions(record, connection);
      summary = record.sessions.get(sessionUri);
    }
    if (!summary || summary.resource !== sessionUri) {
      throw new AhpOperationError(
        "session-not-found",
        "The exact session URI is not present on this endpoint",
      );
    }

    const racedBinding = this.#bindings.get(key);
    if (racedBinding) {
      return racedBinding;
    }

    const binding = new SessionBinding(
      endpointId,
      sessionUri,
      summary.provider,
      this.#bindingRuntime(record, sessionUri, summary.provider),
    );
    this.#bindings.set(key, binding);
    try {
      await binding.hydrate(connection);
      return binding;
    } catch (error) {
      this.#bindings.delete(key);
      await binding.close();
      this.#emitError("bind", error, record, sessionUri);
      if (error instanceof AhpOperationError) {
        throw error;
      }
      throw new AhpOperationError(
        "binding-unavailable",
        "The session could not be hydrated",
      );
    }
  }

  async stop(): Promise<void> {
    if (this.#lifecycle === "stopped") {
      return;
    }
    this.#lifecycle = "stopped";
    this.#watchAbort?.abort();
    const records = [...this.#records.values()];
    const connectTasks = records.flatMap((record) =>
      record.connectTask ? [record.connectTask] : [],
    );
    for (const record of records) {
      record.present = false;
      if (record.retryTimer) {
        clearTimeout(record.retryTimer);
        record.retryTimer = undefined;
      }
    }
    await Promise.all(
      records.map((record) => this.#closeConnection(record, false)),
    );
    await Promise.all(connectTasks.map((task) => task.catch(() => undefined)));
    await this.#watchTask?.catch(() => undefined);
    await Promise.all(
      [...this.#bindings.values()].map((binding) => binding.close()),
    );
    this.#bindings.clear();
  }

  async #runEndpointWatch(signal: AbortSignal): Promise<void> {
    while (!signal.aborted && this.#lifecycle === "running") {
      try {
        const snapshots = this.#watchEndpoints(this.#userDataDirectory, {
          signal,
          pollIntervalMs: this.#watchIntervalMs,
        });
        for await (const entries of snapshots) {
          if (signal.aborted || this.#lifecycle !== "running") {
            return;
          }
          await this.#reconcileEndpoints(entries);
        }
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        this.#emitError("endpoint-watch", error);
      }
      if (
        !(await delayUnlessAborted(this.#watchIntervalMs, signal)) ||
        this.#lifecycle !== "running"
      ) {
        return;
      }
    }
  }

  async #reconcileEndpoints(
    entries: readonly EndpointRegistryEntry[],
  ): Promise<void> {
    const next = new Map<string, EndpointRegistryEntry>();
    for (const entry of entries) {
      const id = endpointPublicId(entry);
      if (next.has(id)) {
        this.#emitError("endpoint-watch", new Error("endpoint id collision"));
        continue;
      }
      next.set(id, entry);
    }

    const closing: Promise<void>[] = [];
    for (const [id, record] of this.#records) {
      const replacement = next.get(id);
      if (!replacement) {
        record.present = false;
        if (!this.#hasBindingsForEndpoint(id)) {
          this.#records.delete(id);
        }
        if (record.retryTimer) {
          clearTimeout(record.retryTimer);
          record.retryTimer = undefined;
        }
        closing.push(this.#closeConnection(record, false));
        continue;
      }
      next.delete(id);
      if (!sameRegistryEntry(record.entry, replacement)) {
        record.present = false;
        if (record.retryTimer) {
          clearTimeout(record.retryTimer);
          record.retryTimer = undefined;
        }
        const oldConnectTask = record.connectTask;
        closing.push(
          (async () => {
            await this.#closeConnection(record, false);
            await oldConnectTask?.catch(() => undefined);
            record.entry = replacement;
            record.endpoint = publicEndpoint(replacement);
            record.present = true;
            record.status = "disconnected";
            record.selectedProtocol = undefined;
            record.sessions.clear();
            record.retryAttempt = 0;
          })(),
        );
      } else {
        record.present = true;
      }
    }
    await Promise.all(closing);

    for (const [id, entry] of next) {
      this.#records.set(id, {
        entry,
        endpoint: publicEndpoint(entry),
        sessions: new Map(),
        status: "disconnected",
        selectedProtocol: undefined,
        connection: undefined,
        connectTask: undefined,
        retryAttempt: 0,
        retryTimer: undefined,
        present: true,
      });
    }

    this.#emitCatalogue();
    await Promise.all(
      [...this.#records.values()].map((record) => this.#connect(record)),
    );
  }

  async #connect(record: EndpointRecord): Promise<void> {
    if (
      this.#lifecycle !== "running" ||
      !record.present ||
      record.connection ||
      record.status === "incompatible"
    ) {
      return;
    }
    if (record.connectTask) {
      return record.connectTask;
    }
    const task = this.#connectOnce(record);
    record.connectTask = task;
    try {
      await task;
    } finally {
      if (record.connectTask === task) {
        record.connectTask = undefined;
      }
    }
  }

  async #connectOnce(record: EndpointRecord): Promise<void> {
    const entry = record.entry;
    if (!SUPPORTED_PROTOCOL_VERSIONS.includes(entry.protocolVersion)) {
      record.status = "incompatible";
      record.selectedProtocol = undefined;
      this.#emitConnection(record);
      this.#emitIncompatibility(record, "advertised-version");
      this.#emitCatalogue();
      return;
    }

    if (record.retryTimer) {
      clearTimeout(record.retryTimer);
      record.retryTimer = undefined;
    }
    record.status = "connecting";
    this.#emitConnection(record);
    this.#emitCatalogue();

    let transport: AhpTransport | undefined;
    let client: AhpClient | undefined;
    try {
      transport = await this.#openTransport(entry);
      client = this.#createClient(transport, this.#clientConfig);
      const rootSubscription = client.attachSubscription(ROOT);
      client.connect();
      const initializeParams: InitializeParams = {
        channel: ROOT,
        clientId: this.#clientId,
        clientInfo: {
          name: "qq-copilot-ahp-adapter",
          version: "0.1.0",
          title: "QQ Copilot AHP Adapter",
        },
        protocolVersions: [...SUPPORTED_PROTOCOL_VERSIONS],
        initialSubscriptions: [ROOT],
        ...(this.#locale ? { locale: this.#locale } : {}),
      };
      const initialized = await client.request(
        "initialize",
        initializeParams,
      );
      if (
        initialized.protocolVersion !== entry.protocolVersion ||
        !SUPPORTED_PROTOCOL_VERSIONS.includes(initialized.protocolVersion)
      ) {
        throw new ProtocolGateError(initialized.protocolVersion);
      }

      const sessions = await listAllSessions(client);
      if (
        this.#lifecycle !== "running" ||
        !record.present ||
        record.entry !== entry
      ) {
        await client.shutdown();
        return;
      }

      record.sessions.clear();
      for (const summary of sessions) {
        record.sessions.set(summary.resource, summary);
      }
      const connection: EndpointConnection = {
        client,
        rootSubscription,
        catalogueQueue: new SerialTaskQueue(),
        selectedProtocol: initialized.protocolVersion,
        closed: false,
      };
      record.connection = connection;
      record.status = "connected";
      record.selectedProtocol = initialized.protocolVersion;
      record.retryAttempt = 0;
      this.#emitConnection(record);
      this.#emitCatalogue();

      void this.#pumpRoot(record, connection);
      const hydrations = [...this.#bindings.values()]
        .filter((binding) => binding.endpointId === record.endpoint.id)
        .map((binding) =>
          binding.hydrate(connection).catch((error: unknown) => {
            this.#emitError(
              "bind",
              error,
              record,
              binding.sessionUri,
            );
          }),
        );
      await Promise.all(hydrations);
    } catch (error) {
      if (client) {
        await client.shutdown().catch(() => undefined);
      } else {
        await Promise.resolve(transport?.close()).catch(() => undefined);
      }
      if (
        this.#lifecycle !== "running" ||
        !record.present ||
        record.entry !== entry
      ) {
        return;
      }
      record.connection = undefined;
      if (error instanceof ProtocolGateError) {
        record.status = "incompatible";
        record.selectedProtocol = error.selectedProtocol;
        this.#emitConnection(record);
        this.#emitIncompatibility(
          record,
          "negotiated-version",
          error.selectedProtocol,
        );
      } else {
        record.status = "disconnected";
        record.selectedProtocol = undefined;
        this.#emitConnection(record);
        this.#emitError("connect", error, record);
        this.#scheduleRetry(record);
      }
      this.#emitCatalogue();
    }
  }

  async #pumpRoot(
    record: EndpointRecord,
    connection: EndpointConnection,
  ): Promise<void> {
    try {
      for await (const event of connection.rootSubscription) {
        if (
          connection.closed ||
          record.connection !== connection ||
          this.#lifecycle !== "running"
        ) {
          return;
        }
        await connection.catalogueQueue.run(() => {
          this.#applyRootEvent(record, event);
        });
      }
    } catch (error) {
      this.#emitError("connect", error, record);
    } finally {
      if (
        !connection.closed &&
        record.connection === connection &&
        this.#lifecycle === "running"
      ) {
        await this.#connectionLost(record, connection);
      }
    }
  }

  #applyRootEvent(
    record: EndpointRecord,
    event: SubscriptionEvent,
  ): void {
    switch (event.type) {
      case "sessionAdded":
        record.sessions.set(
          event.params.summary.resource,
          event.params.summary,
        );
        this.#emitCatalogue();
        break;
      case "sessionRemoved":
        record.sessions.delete(event.params.session);
        this.#emitCatalogue();
        break;
      case "sessionSummaryChanged": {
        const current = record.sessions.get(event.params.session);
        if (current) {
          record.sessions.set(event.params.session, {
            ...current,
            ...event.params.changes,
            resource: current.resource,
            provider: current.provider,
            createdAt: current.createdAt,
          });
          this.#emitCatalogue();
        }
        break;
      }
      case "action":
        this.#emitAction(record, {
          scope: "root",
          endpointId: record.endpoint.id,
          envelope: event.params,
        });
        break;
      case "authRequired":
        break;
    }
  }

  async #connectionLost(
    record: EndpointRecord,
    connection: EndpointConnection,
  ): Promise<void> {
    if (record.connection !== connection) {
      return;
    }
    connection.closed = true;
    record.connection = undefined;
    record.status = "disconnected";
    record.selectedProtocol = undefined;
    await Promise.all(
      [...this.#bindings.values()]
        .filter((binding) => binding.endpointId === record.endpoint.id)
        .map((binding) => binding.detach(connection)),
    );
    await connection.client.shutdown().catch(() => undefined);
    this.#emitConnection(record);
    this.#emitCatalogue();
    this.#scheduleRetry(record);
  }

  async #closeConnection(
    record: EndpointRecord,
    scheduleRetry: boolean,
  ): Promise<void> {
    const connection = record.connection;
    if (!connection) {
      return;
    }
    connection.closed = true;
    record.connection = undefined;
    await connection.rootSubscription.close().catch(() => undefined);
    await Promise.all(
      [...this.#bindings.values()]
        .filter((binding) => binding.endpointId === record.endpoint.id)
        .map((binding) => binding.detach(connection)),
    );
    await connection.client.shutdown().catch(() => undefined);
    if (record.status !== "incompatible") {
      record.status = "disconnected";
      record.selectedProtocol = undefined;
    }
    this.#emitConnection(record);
    this.#emitCatalogue();
    if (scheduleRetry) {
      this.#scheduleRetry(record);
    }
  }

  #scheduleRetry(record: EndpointRecord): void {
    if (
      this.#lifecycle !== "running" ||
      !record.present ||
      record.status === "incompatible" ||
      record.retryTimer
    ) {
      return;
    }
    const exponent = Math.min(record.retryAttempt, 16);
    const delayMs = Math.min(
      DEFAULT_RETRY_BASE_MS * 2 ** exponent,
      MAX_RETRY_MS,
    );
    record.retryAttempt += 1;
    const timer = setTimeout(() => {
      if (record.retryTimer === timer) {
        record.retryTimer = undefined;
      }
      void this.#connect(record);
    }, delayMs);
    timer.unref();
    record.retryTimer = timer;
  }

  async #refreshSessions(
    record: EndpointRecord,
    connection: EndpointConnection,
  ): Promise<void> {
    try {
      await connection.catalogueQueue.run(async () => {
        if (connection.closed || record.connection !== connection) {
          return;
        }
        const sessions = await listAllSessions(connection.client);
        if (connection.closed || record.connection !== connection) {
          return;
        }
        record.sessions.clear();
        for (const summary of sessions) {
          record.sessions.set(summary.resource, summary);
        }
      });
    } catch (error) {
      this.#emitError("list-sessions", error, record);
    }
  }

#hasBindingsForEndpoint(endpointId: string): boolean {
    return [...this.#bindings.values()].some(
      (binding) => binding.endpointId === endpointId,
    );
  }

  #bindingRuntime(
    record: EndpointRecord,
    sessionUri: URI,
    provider: string,
  ): BindingRuntime {
    return {
      clientId: this.#clientId,
      createId: this.#createId,
      monotonicNow: this.#monotonicNow,
      connection: () => record.connection,
      sanitize: <T>(value: T): T =>
        sanitizeClone(value, endpointSecrets(record.entry)),
      emitSession: (state, sequence) => {
        const event: SessionSnapshotEvent = {
          endpointId: record.endpoint.id,
          sessionUri,
          provider,
          serverSeq: sequence,
          state,
        };
        this.#invokeCallback(
          this.#callbacks.onSessionSnapshot,
          event,
          endpointSecrets(record.entry),
        );
      },
      emitChat: (state, sequence) => {
        const event: ChatSnapshotEvent = {
          endpointId: record.endpoint.id,
          sessionUri,
          provider,
          chatUri: state.resource,
          serverSeq: sequence,
          state,
        };
        this.#invokeCallback(
          this.#callbacks.onChatSnapshot,
          event,
          endpointSecrets(record.entry),
        );
      },
      emitAction: (scope, envelope, chatUri) => {
        if (scope === "session") {
          this.#emitAction(record, {
            scope,
            endpointId: record.endpoint.id,
            sessionUri,
            provider,
            envelope,
          });
          return;
        }
        if (chatUri !== undefined) {
          this.#emitAction(record, {
            scope,
            endpointId: record.endpoint.id,
            sessionUri,
            provider,
            chatUri,
            envelope,
          });
        }
      },
      emitError: (operation, error, chatUri) => {
        this.#emitError(
          operation,
          error,
          record,
          sessionUri,
          chatUri,
        );
      },
      release: (binding) => {
        const key = bindingKey(binding.endpointId, binding.sessionUri);
        if (this.#bindings.get(key) === binding) {
          this.#bindings.delete(key);
        }
        if (!record.present && !this.#hasBindingsForEndpoint(record.endpoint.id)) {
          this.#records.delete(record.endpoint.id);
        }
      },
    };
  }

  #emitCatalogue(): void {
    this.#revision += 1;
    this.#invokeCallback(
      this.#callbacks.onCatalogue,
      this.#catalogueSnapshot(),
      [],
    );
  }

  #catalogueSnapshot(): CatalogueSnapshot {
    const endpoints = [...this.#records.values()]
      .filter((record) => record.present)
      .sort((left, right) =>
        left.endpoint.id.localeCompare(right.endpoint.id),
      )
      .map((record): EndpointCatalogue => ({
        endpoint: record.endpoint,
        connection: record.status,
        ...(record.selectedProtocol
          ? { selectedProtocol: record.selectedProtocol }
          : {}),
        sessions: [...record.sessions.values()].map((summary) =>
          sanitizeClone(summary, endpointSecrets(record.entry)),
        ),
      }));
    return structuredClone({
      revision: this.#revision,
      endpoints,
    });
  }

  #emitConnection(record: EndpointRecord): void {
    const event: ConnectionEvent = {
      endpoint: record.endpoint,
      status: record.status,
      ...(record.selectedProtocol
        ? { selectedProtocol: record.selectedProtocol }
        : {}),
    };
    this.#invokeCallback(
      this.#callbacks.onConnection,
      event,
      endpointSecrets(record.entry),
    );
  }

  #emitIncompatibility(
    record: EndpointRecord,
    reason: IncompatibilityReason,
    selectedProtocol?: string,
  ): void {
    const event: IncompatibilityEvent = {
      endpoint: record.endpoint,
      reason,
      supportedProtocols: [...SUPPORTED_PROTOCOL_VERSIONS],
      ...(selectedProtocol ? { selectedProtocol } : {}),
    };
    this.#invokeCallback(
      this.#callbacks.onIncompatibility,
      event,
      endpointSecrets(record.entry),
    );
  }

  #emitAction(record: EndpointRecord, event: DomainActionEvent): void {
    this.#invokeCallback(
      this.#callbacks.onAction,
      event,
      endpointSecrets(record.entry),
    );
  }

  #emitError(
    operation: CoreErrorOperation,
    error: unknown,
    record?: EndpointRecord,
    sessionUri?: URI,
    chatUri?: URI,
  ): void {
    const event: CoreErrorEvent = {
      operation,
      code: classifyError(error),
      message: errorMessageFor(operation),
      ...(record ? { endpointId: record.endpoint.id } : {}),
      ...(sessionUri ? { sessionUri } : {}),
      ...(chatUri ? { chatUri } : {}),
    };
    this.#invokeCallback(
      this.#callbacks.onError,
      event,
      record ? endpointSecrets(record.entry) : [],
      true,
    );
  }

  #invokeCallback<T>(
    callback: ((event: T) => void) | undefined,
    event: T,
    secrets: readonly string[],
    isErrorCallback = false,
  ): void {
    if (!callback) {
      return;
    }
    try {
      callback(sanitizeClone(event, secrets));
    } catch {
      if (!isErrorCallback) {
        this.#emitError("callback", new Error("domain callback failed"));
      }
    }
  }
}

class SessionBinding implements AhpSessionBinding {
  readonly endpointId: string;

  readonly sessionUri: URI;

  readonly provider: string;

  readonly #runtime: BindingRuntime;

  readonly #mirror: ProviderSessionStateMirror;

  readonly #queue = new SerialTaskQueue();

  #connection: EndpointConnection | undefined;

  #sessionSubscription: Subscription | undefined;

  #chatSubscription: Subscription | undefined;

  #chatUri: URI | undefined;

  #pendingTurnStartClientSeq: number | undefined;

  #observedTurnId: string | undefined;

  #observedTurnAt = 0;

  #closed = false;

  constructor(
    endpointId: string,
    sessionUri: URI,
    provider: string,
    runtime: BindingRuntime,
  ) {
    this.endpointId = endpointId;
    this.sessionUri = sessionUri;
    this.provider = provider;
    this.#runtime = runtime;
    this.#mirror = new ProviderSessionStateMirror(sessionUri, provider);
  }

  get defaultChatUri(): URI | undefined {
    return this.#mirror.defaultChatUri;
  }

  snapshot(): BoundSessionSnapshot {
    return this.#runtime.sanitize({
      endpointId: this.endpointId,
      sessionUri: this.sessionUri,
      provider: this.provider,
      ...(this.#mirror.session ? { session: this.#mirror.session } : {}),
      ...(this.#mirror.chat ? { defaultChat: this.#mirror.chat } : {}),
    });
  }

  hydrate(connection: EndpointConnection): Promise<void> {
    return this.#queue.run(async () => {
      this.#assertOpen();
      await this.#detachCurrent(false);
      this.#connection = connection;
      const subscribed = await connection.client.subscribe(this.sessionUri, {
        delivery: { maxLatencyMs: 0 },
      });
      if (
        this.#closed ||
        connection.closed ||
        this.#connection !== connection
      ) {
        await subscribed.subscription.close();
        return;
      }
      if (!subscribed.result.snapshot) {
        await subscribed.subscription.close();
        throw new MirrorSnapshotError(
          "session subscription returned no snapshot",
        );
      }
      const state = this.#mirror.hydrateSession(
        subscribed.result.snapshot,
      );
      this.#sessionSubscription = subscribed.subscription;
      this.#updateTurnObservation();
      this.#runtime.emitSession(state, this.#mirror.sessionSeq);
      void this.#pumpSession(connection, subscribed.subscription);
      await this.#reconcileDefaultChat(connection);
    });
  }

  detach(connection: EndpointConnection): Promise<void> {
    return this.#queue.run(async () => {
      if (this.#connection !== connection) {
        return;
      }
      await this.#detachCurrent(false);
    });
  }

  queueUserText(
    text: string,
    clientSeq?: number,
  ): Promise<QueueUserTextResult> {
    return this.#queue.run(() => {
      this.#assertOpen();
      if (text.length === 0) {
        throw new AhpOperationError(
          "invalid-command",
          "User text must not be empty",
        );
      }
      const { connection, chat } = this.#requireChat();
      const message: Message = {
        text,
        origin: { kind: MessageKind.User },
      };
      if (
        chat.activeTurn === undefined &&
        this.#pendingTurnStartClientSeq === undefined
      ) {
        const turnId = this.#runtime.createId();
        const action: StateAction = {
          type: ActionType.ChatTurnStarted,
          turnId,
          startedAt: new Date().toISOString(),
          message,
        };
        const dispatched = connection.client.dispatch(
          chat.resource,
          action,
          clientSeq,
        );
        this.#pendingTurnStartClientSeq = dispatched.clientSeq;
        return {
          disposition: "started",
          id: turnId,
          clientSeq: dispatched.clientSeq,
        };
      }

      const pendingId = this.#runtime.createId();
      const action: StateAction = {
        type: ActionType.ChatPendingMessageSet,
        kind: PendingMessageKind.Queued,
        id: pendingId,
        message,
      };
      const dispatched = connection.client.dispatch(
        chat.resource,
        action,
        clientSeq,
      );
      return {
        disposition: "queued",
        id: pendingId,
        clientSeq: dispatched.clientSeq,
      };
    });
  }

  cancelActiveTurn(clientSeq?: number): Promise<CancelTurnResult> {
    return this.#queue.run(() => {
      this.#assertOpen();
      const { connection, chat } = this.#requireChat();
      const activeTurn = chat.activeTurn;
      if (!activeTurn) {
        throw new AhpOperationError(
          "no-active-turn",
          "There is no active turn to cancel",
        );
      }
      const observedDuration =
        this.#observedTurnId === activeTurn.id
          ? this.#runtime.monotonicNow() - this.#observedTurnAt
          : 0;
      const action: StateAction = {
        type: ActionType.ChatTurnCancelled,
        turnId: activeTurn.id,
        duration: Math.max(0, Math.floor(observedDuration)),
      };
      const dispatched = connection.client.dispatch(
        chat.resource,
        action,
        clientSeq,
      );
      return {
        turnId: activeTurn.id,
        clientSeq: dispatched.clientSeq,
      };
    });
  }

  reviewToolParameters(
    command: ReviewToolParametersCommand,
    clientSeq?: number,
  ): Promise<ActionDispatchResult> {
    return this.#queue.run(() => {
      this.#assertOpen();
      const connection = this.#requireConnection();
      const request = this.#findParameterToolRequest(command.requestId);
      validateConfirmationOption(request, command);

      let action: ChatToolCallConfirmedAction;
      if (command.decision === "approve") {
        if (
          command.editedToolInput !== undefined &&
          (!request.toolCall.editable ||
            typeof request.toolCall.toolInput !== "string")
        ) {
          throw new AhpOperationError(
            "invalid-command",
            "This tool input cannot be edited",
          );
        }
        action = {
          type: ActionType.ChatToolCallConfirmed,
          turnId: request.turnId,
          toolCallId: request.toolCall.toolCallId,
          approved: true,
          confirmed:
            command.confirmed ?? ToolCallConfirmationReason.UserAction,
          ...(command.editedToolInput !== undefined
            ? { editedToolInput: command.editedToolInput }
            : {}),
          ...(command.selectedOptionId
            ? { selectedOptionId: command.selectedOptionId }
            : {}),
        };
      } else {
        action = {
          type: ActionType.ChatToolCallConfirmed,
          turnId: request.turnId,
          toolCallId: request.toolCall.toolCallId,
          approved: false,
          reason: command.reason ?? ToolCallCancellationReason.Denied,
          ...(command.reasonMessage
            ? { reasonMessage: command.reasonMessage }
            : {}),
          ...(command.userSuggestion
            ? {
                userSuggestion: {
                  text: command.userSuggestion,
                  origin: { kind: MessageKind.User },
                },
              }
            : {}),
          ...(command.selectedOptionId
            ? { selectedOptionId: command.selectedOptionId }
            : {}),
        };
      }
      const dispatched = connection.client.dispatch(
        request.chat,
        action,
        clientSeq,
      );
      return { clientSeq: dispatched.clientSeq };
    });
  }

  reviewToolResult(
    command: ReviewToolResultCommand,
    clientSeq?: number,
  ): Promise<ActionDispatchResult> {
    return this.#queue.run(() => {
      this.#assertOpen();
      const connection = this.#requireConnection();
      const request = this.#findResultToolRequest(command.requestId);
      const action: ChatToolCallResultConfirmedAction = {
        type: ActionType.ChatToolCallResultConfirmed,
        turnId: request.turnId,
        toolCallId: request.toolCall.toolCallId,
        approved: command.approved,
      };
      const dispatched = connection.client.dispatch(
        request.chat,
        action,
        clientSeq,
      );
      return { clientSeq: dispatched.clientSeq };
    });
  }

  completeCurrentInput(
    command: CompleteCurrentInputCommand,
    clientSeq?: number,
  ): Promise<ActionDispatchResult> {
    return this.#queue.run(() => {
      this.#assertOpen();
      const connection = this.#requireConnection();
      const request = this.#findInputRequest(command.requestId);
      const action: StateAction = {
        type: ActionType.ChatInputCompleted,
        requestId: request.request.id,
        response: command.response,
        ...(command.answers
          ? { answers: structuredClone(command.answers) }
          : {}),
      };
      const dispatched = connection.client.dispatch(
        request.chat,
        action,
        clientSeq,
      );
      return { clientSeq: dispatched.clientSeq };
    });
  }

  close(): Promise<void> {
    return this.#queue.run(async () => {
      if (this.#closed) {
        return;
      }
      this.#closed = true;
      await this.#detachCurrent(true);
      this.#runtime.release(this);
    });
  }

  async #pumpSession(
    connection: EndpointConnection,
    subscription: Subscription,
  ): Promise<void> {
    try {
      for await (const event of subscription) {
        if (
          this.#closed ||
          connection.closed ||
          this.#connection !== connection ||
          this.#sessionSubscription !== subscription
        ) {
          return;
        }
        if (event.type !== "action") {
          continue;
        }
        await this.#queue.run(async () => {
          if (
            this.#closed ||
            this.#connection !== connection ||
            this.#sessionSubscription !== subscription
          ) {
            return;
          }
          const result = this.#mirror.applySession(event.params);
          this.#handleMirrorResult(
            result,
            "session-stream",
            event.params,
          );
          if (result === "applied") {
            const state = this.#mirror.session;
            if (state) {
              this.#runtime.emitSession(state, this.#mirror.sessionSeq);
            }
            try {
              await this.#reconcileDefaultChat(connection);
            } catch (error) {
              this.#runtime.emitError(
                "chat-stream",
                error,
                this.#mirror.defaultChatUri,
              );
            }
          }
        });
      }
    } catch (error) {
      this.#runtime.emitError("session-stream", error);
    }
  }

  async #pumpChat(
    connection: EndpointConnection,
    subscription: Subscription,
    chatUri: URI,
  ): Promise<void> {
    try {
      for await (const event of subscription) {
        if (
          this.#closed ||
          connection.closed ||
          this.#connection !== connection ||
          this.#chatSubscription !== subscription ||
          this.#chatUri !== chatUri
        ) {
          return;
        }
        if (event.type !== "action") {
          continue;
        }
        await this.#queue.run(() => {
          if (
            this.#closed ||
            this.#connection !== connection ||
            this.#chatSubscription !== subscription ||
            this.#chatUri !== chatUri
          ) {
            return;
          }
          const envelope = normalizeLegacyActionEnvelope(event.params);
          const result = this.#mirror.applyDefaultChat(envelope);
          this.#handleMirrorResult(
            result,
            "chat-stream",
            envelope,
            chatUri,
          );
          if (result === "applied") {
            this.#updateTurnObservation();
            const state = this.#mirror.chat;
            if (state) {
              this.#runtime.emitChat(state, this.#mirror.chatSeq);
            }
          }
        });
      }
    } catch (error) {
      this.#runtime.emitError("chat-stream", error, chatUri);
    }
  }

  async #reconcileDefaultChat(
    connection: EndpointConnection,
  ): Promise<void> {
    const desired = this.#mirror.defaultChatUri;
    if (
      desired === this.#chatUri &&
      this.#chatSubscription !== undefined
    ) {
      return;
    }

    const oldSubscription = this.#chatSubscription;
    const oldChatUri = this.#chatUri;
    this.#chatSubscription = undefined;
    this.#chatUri = undefined;
    this.#mirror.clearDefaultChat(oldChatUri);
    if (oldSubscription) {
      await oldSubscription.close();
      if (!connection.closed && oldChatUri !== undefined) {
        await connection.client
          .unsubscribe(oldChatUri)
          .catch(() => undefined);
      }
    }
    if (
      desired === undefined ||
      connection.closed ||
      this.#connection !== connection
    ) {
      return;
    }

    const subscribed = await connection.client.subscribe(desired, {
      delivery: { maxLatencyMs: 0 },
    });
    if (
      this.#closed ||
      connection.closed ||
      this.#connection !== connection
    ) {
      await subscribed.subscription.close();
      return;
    }
    if (!subscribed.result.snapshot) {
      await subscribed.subscription.close();
      throw new MirrorSnapshotError(
        "default chat subscription returned no snapshot",
      );
    }
    const state = this.#mirror.hydrateDefaultChat(
      subscribed.result.snapshot,
      desired,
    );
    this.#chatUri = desired;
    this.#chatSubscription = subscribed.subscription;
    this.#updateTurnObservation();
    this.#runtime.emitChat(state, this.#mirror.chatSeq);
    void this.#pumpChat(connection, subscribed.subscription, desired);
  }

  #handleMirrorResult(
    result: MirrorApplyResult,
    operation: "session-stream" | "chat-stream",
    envelope: ActionEnvelope,
    chatUri?: URI,
  ): void {
    if (result === "invalid-action" || result === "wrong-channel") {
      this.#runtime.emitError(
        operation,
        new Error("invalid action envelope"),
        chatUri,
      );
      return;
    }
    if (result === "stale" || result === "unhydrated") {
      return;
    }
    this.#runtime.emitAction(
      operation === "session-stream" ? "session" : "chat",
      envelope,
      chatUri,
    );
    if (
      envelope.origin?.clientId === this.#runtime.clientId &&
      envelope.origin.clientSeq === this.#pendingTurnStartClientSeq
    ) {
      this.#pendingTurnStartClientSeq = undefined;
    }
    if (
      envelope.action.type === ActionType.ChatTurnStarted &&
      operation === "chat-stream"
    ) {
      this.#pendingTurnStartClientSeq = undefined;
    }
  }

  async #detachCurrent(unsubscribe: boolean): Promise<void> {
    const connection = this.#connection;
    const sessionSubscription = this.#sessionSubscription;
    const chatSubscription = this.#chatSubscription;
    const chatUri = this.#chatUri;
    this.#connection = undefined;
    this.#sessionSubscription = undefined;
    this.#chatSubscription = undefined;
    this.#chatUri = undefined;
    this.#pendingTurnStartClientSeq = undefined;
    this.#observedTurnId = undefined;
    this.#mirror.clearDefaultChat();

    await Promise.all([
      sessionSubscription?.close() ?? Promise.resolve(),
      chatSubscription?.close() ?? Promise.resolve(),
    ]);
    if (unsubscribe && connection && !connection.closed) {
      const targets = chatUri
        ? [this.sessionUri, chatUri]
        : [this.sessionUri];
      await Promise.all(
        targets.map((uri) =>
          connection.client.unsubscribe(uri).catch(() => undefined),
        ),
      );
    }
  }

  #requireConnection(): EndpointConnection {
    const connection = this.#runtime.connection();
    if (
      !connection ||
      connection.closed ||
      this.#connection !== connection
    ) {
      throw new AhpOperationError(
        "binding-unavailable",
        "The bound Agent Host connection is unavailable",
      );
    }
    return connection;
  }

  #requireChat(): {
    readonly connection: EndpointConnection;
    readonly chat: ChatState;
  } {
    const connection = this.#requireConnection();
    const chat = this.#mirror.chat;
    if (!chat || chat.resource !== this.#mirror.defaultChatUri) {
      throw new AhpOperationError(
        "chat-unavailable",
        "The session has no hydrated default chat",
      );
    }
    return { connection, chat };
  }

  #findParameterToolRequest(
    requestId: string,
  ): SessionParameterConfirmationRequest {
    const state = this.#mirror.session;
    const request = state?.inputNeeded?.find((candidate) =>
      isParameterConfirmationRequest(candidate, requestId),
    );
    if (!request) {
      throw new AhpOperationError(
        "pending-tool-not-found",
        "The requested pending tool confirmation is no longer current",
      );
    }
    return request;
  }

  #findResultToolRequest(
    requestId: string,
  ): SessionResultConfirmationRequest {
    const state = this.#mirror.session;
    const request = state?.inputNeeded?.find((candidate) =>
      isResultConfirmationRequest(candidate, requestId),
    );
    if (!request) {
      throw new AhpOperationError(
        "pending-tool-not-found",
        "The requested pending tool confirmation is no longer current",
      );
    }
    return request;
  }

  #findInputRequest(
    requestId: string | undefined,
  ): SessionChatInputRequest {
    const state = this.#mirror.session;
    const candidates =
      state?.inputNeeded?.filter(
        (candidate) =>
          candidate.kind === SessionInputRequestKind.ChatInput,
      ) ?? [];
    if (requestId !== undefined) {
      const exact = candidates.find(
        (candidate) =>
          candidate.id === requestId ||
          candidate.request.id === requestId,
      );
      if (exact) {
        return exact;
      }
      throw new AhpOperationError(
        "pending-input-not-found",
        "The requested input is no longer current",
      );
    }
    const defaultChat = this.#mirror.defaultChatUri;
    const inDefaultChat = candidates.filter(
      (candidate) => candidate.chat === defaultChat,
    );
    if (inDefaultChat.length === 1) {
      const current = inDefaultChat[0];
      if (current) {
        return current;
      }
    }
    if (candidates.length === 1) {
      const current = candidates[0];
      if (current) {
        return current;
      }
    }
    if (candidates.length > 1) {
      throw new AhpOperationError(
        "ambiguous-input",
        "More than one input request is pending; provide requestId",
      );
    }
    throw new AhpOperationError(
      "pending-input-not-found",
      "There is no pending input to complete",
    );
  }

  #updateTurnObservation(): void {
    const activeTurnId = this.#mirror.chat?.activeTurn?.id;
    if (activeTurnId === this.#observedTurnId) {
      return;
    }
    this.#observedTurnId = activeTurnId;
    this.#observedTurnAt =
      activeTurnId === undefined ? 0 : this.#runtime.monotonicNow();
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new AhpOperationError(
        "already-closed",
        "This session binding has been closed",
      );
    }
  }
}

async function listAllSessions(client: AhpClient): Promise<SessionSummary[]> {
  const sessions = new Map<URI, SessionSummary>();
  const cursors = new Set<string>();
  let cursor: string | undefined;

  for (let pageNumber = 0; pageNumber < MAX_SESSION_PAGES; pageNumber += 1) {
    const page = await client.request("listSessions", {
      channel: ROOT,
      limit: SESSION_PAGE_SIZE,
      ...(cursor ? { cursor } : {}),
    });
    for (const summary of page.items) {
      sessions.set(summary.resource, summary);
    }
    if (!page.nextCursor) {
      return [...sessions.values()];
    }
    if (cursors.has(page.nextCursor)) {
      throw new Error("Agent Host repeated a session cursor");
    }
    cursors.add(page.nextCursor);
    cursor = page.nextCursor;
  }
  throw new Error("Agent Host session pagination limit exceeded");
}

async function defaultOpenTransport(
  entry: EndpointRegistryEntry,
): Promise<AhpTransport> {
  if (entry.endpoint.type !== "socket") {
    throw new TransportError(
      "protocol",
      "editor Agent Host did not advertise a named pipe",
    );
  }
  return openNamedPipeTransport(
    entry.endpoint.path,
    entry.connectionToken,
  );
}

function publicEndpoint(entry: EndpointRegistryEntry): PublicEndpoint {
  return {
    id: endpointPublicId(entry),
    pid: entry.pid,
    instanceId: entry.instanceId,
    advertisedProtocol: entry.protocolVersion,
  };
}

function sameRegistryEntry(
  left: EndpointRegistryEntry,
  right: EndpointRegistryEntry,
): boolean {
  if (
    left.type !== right.type ||
    left.pid !== right.pid ||
    left.instanceId !== right.instanceId ||
    left.protocolVersion !== right.protocolVersion ||
    left.connectionToken !== right.connectionToken ||
    left.endpoint.type !== right.endpoint.type
  ) {
    return false;
  }
  if (left.endpoint.type === "socket") {
    return (
      right.endpoint.type === "socket" &&
      left.endpoint.path === right.endpoint.path
    );
  }
  return (
    right.endpoint.type === "tcp" &&
    left.endpoint.host === right.endpoint.host &&
    left.endpoint.port === right.endpoint.port
  );
}

function endpointSecrets(entry: EndpointRegistryEntry): readonly string[] {
  return entry.endpoint.type === "socket"
    ? [entry.connectionToken, entry.endpoint.path]
    : [entry.connectionToken];
}

function sanitizeClone<T>(value: T, secrets: readonly string[]): T {
  const clone = structuredClone(value);
  redactStrings(clone, secrets.filter((secret) => secret.length > 0));
  return clone;
}

function redactText(value: string, secrets: readonly string[]): string {
  let redacted = value;
  for (const secret of secrets) {
    redacted = redacted.split(secret).join("[redacted]");
  }
  return redacted;
}

function redactStrings(value: unknown, secrets: readonly string[]): void {
  if (Array.isArray(value)) {
    for (const item of value) {
      redactStrings(item, secrets);
    }
    return;
  }
  if (typeof value !== "object" || value === null) {
    return;
  }
  for (const [key, item] of Object.entries(value)) {
    const redactedKey = redactText(key, secrets);
    const redactedItem =
      typeof item === "string" ? redactText(item, secrets) : item;
    if (redactedKey !== key) {
      Reflect.deleteProperty(value, key);
      Reflect.set(value, redactedKey, redactedItem);
    } else if (redactedItem !== item) {
      Reflect.set(value, key, redactedItem);
    }
    if (typeof redactedItem !== "string") {
      redactStrings(redactedItem, secrets);
    }
  }
}

function classifyError(error: unknown): string {
  if (error instanceof RpcError) {
    return `rpc:${error.code}`;
  }
  if (error instanceof RpcTimeoutError) {
    return `timeout:${safeIdentifier(error.method)}`;
  }
  if (error instanceof TransportError) {
    return `transport:${error.kind}`;
  }
  if (error instanceof ClientClosedError) {
    return "client-closed";
  }
  if (error instanceof MirrorSnapshotError) {
    return "invalid-snapshot";
  }
  if (error instanceof AhpOperationError) {
    return error.code;
  }
  if (error instanceof Error) {
    return `internal:${safeIdentifier(error.name)}`;
  }
  return "internal:unknown";
}

function safeIdentifier(value: string): string {
  const safe = value.replace(/[^A-Za-z0-9_.-]/gu, "-").slice(0, 64);
  return safe.length > 0 ? safe : "unknown";
}

function errorMessageFor(operation: CoreErrorOperation): string {
  switch (operation) {
    case "bind":
      return "Failed to bind and hydrate an Agent Host session";
    case "callback":
      return "A domain callback failed";
    case "connect":
      return "Failed to connect to an editor Agent Host";
    case "endpoint-watch":
      return "Failed to watch the editor Agent Host catalogue";
    case "list-sessions":
      return "Failed to refresh the Agent Host session catalogue";
    case "session-stream":
      return "The bound session stream failed";
    case "chat-stream":
      return "The bound chat stream failed";
  }
}

function isParameterConfirmationRequest(
  request: SessionInputRequest,
  requestId: string,
): request is SessionParameterConfirmationRequest {
  return (
    request.id === requestId &&
    request.kind === SessionInputRequestKind.ToolConfirmation &&
    request.toolCall.status === ToolCallStatus.PendingConfirmation
  );
}

function isResultConfirmationRequest(
  request: SessionInputRequest,
  requestId: string,
): request is SessionResultConfirmationRequest {
  return (
    request.id === requestId &&
    request.kind === SessionInputRequestKind.ToolConfirmation &&
    request.toolCall.status === ToolCallStatus.PendingResultConfirmation
  );
}

function validateConfirmationOption(
  request: SessionParameterConfirmationRequest,
  command: ReviewToolParametersCommand,
): void {
  if (!command.selectedOptionId) {
    return;
  }
  const option = request.toolCall.options?.find(
    (candidate) => candidate.id === command.selectedOptionId,
  );
  const expected =
    command.decision === "approve"
      ? ConfirmationOptionKind.Approve
      : ConfirmationOptionKind.Deny;
  if (!option || option.kind !== expected) {
    throw new AhpOperationError(
      "invalid-confirmation-option",
      "The selected confirmation option is not valid for this decision",
    );
  }
}

function bindingKey(endpointId: string, sessionUri: URI): string {
  return `${endpointId.length}:${endpointId}${sessionUri}`;
}

function delayUnlessAborted(
  delayMs: number,
  signal: AbortSignal,
): Promise<boolean> {
  if (signal.aborted) {
    return Promise.resolve(false);
  }
  return new Promise((resolve) => {
    let settled = false;
    const finish = (continued: boolean): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", aborted);
      resolve(continued);
    };
    const aborted = (): void => finish(false);
    const timer = setTimeout(() => finish(true), delayMs);
    timer.unref();
    signal.addEventListener("abort", aborted, { once: true });
    if (signal.aborted) {
      finish(false);
    }
  });
}
