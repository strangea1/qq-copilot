import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";

import { SUPPORTED_PROTOCOL_VERSIONS } from "@microsoft/agent-host-protocol";

import {
  AhpCore,
  AhpOperationError,
  type AhpCoreOptions,
  type AhpSessionBinding,
  type CatalogueSnapshot,
  type ChatSnapshotEvent,
  type ConnectionEvent,
  type DomainActionEvent,
  type IncompatibilityEvent,
  type SessionSnapshotEvent,
} from "./ahp-core.js";
import {
  BridgeClient,
  BridgeRpcError,
  type BridgeRequest,
} from "./bridge-client.js";
import {
  loadAdapterConfig,
  parseAdapterArguments,
  type AdapterConfig,
} from "./config.js";
import {
  AhpEventNormalizer,
  type PublishedEvent,
} from "./event-normalizer.js";
import { buildInputCompletion } from "./input-completion.js";

const ADAPTER_VERSION = "0.1.0";
const EVENT_BATCH_SIZE = 64;
const RETRY_DELAY_MS = 1_000;

export interface BridgeBinding {
  readonly binding_id: string;
  readonly generation: number;
  readonly endpoint_id: string;
  readonly host_instance_id?: string;
  readonly session_uri: string;
  readonly chat_uri?: string;
  readonly state: string;
  readonly last_server_sequence: number;
}

export interface RegisterResult {
  readonly bindings: readonly BridgeBinding[];
  readonly foreground_binding_id?: string;
}

export interface AdapterCommand {
  readonly command_id: number;
  readonly command_key: string;
  readonly binding_id: string;
  readonly binding_generation: number;
  readonly kind:
    | "bind_session"
    | "unbind_session"
    | "send_message"
    | "cancel_turn"
    | "approve_tool"
    | "review_tool_result"
    | "complete_input";
  readonly data: unknown;
}

interface PollResult {
  readonly commands: readonly AdapterCommand[];
}

interface PendingBinding {
  readonly bindingId: string;
  readonly generation: number;
  readonly endpointId: string;
  readonly hostInstanceId: string;
  readonly sessionUri: string;
  chatUri?: string;
  normalizer?: AhpEventNormalizer;
  lastServerSequence: number;
}

interface ActiveBinding {
  readonly bindingId: string;
  readonly generation: number;
  readonly endpointId: string;
  readonly hostInstanceId: string;
  readonly sessionUri: string;
  readonly chatUri: string;
  readonly binding: AhpSessionBinding;
  readonly normalizer: AhpEventNormalizer;
}

export interface BridgeClientLike {
  call<T>(request: BridgeRequest, timeoutMs?: number): Promise<T>;
}

export interface AhpCoreLike {
  readonly catalogue: CatalogueSnapshot;
  start(): Promise<CatalogueSnapshot | void>;
  stop(): Promise<void>;
  bindSession(endpointId: string, sessionUri: string): Promise<AhpSessionBinding>;
}

export interface RuntimeDependencies {
  readonly createBridgeClient?: (config: AdapterConfig) => BridgeClientLike;
  readonly createCore?: (options: AhpCoreOptions) => AhpCoreLike;
}

class SerialQueue {
  #tail: Promise<void> = Promise.resolve();

  run(task: () => Promise<void>): void {
    const next = this.#tail.then(task, task);
    this.#tail = next.catch(() => undefined);
  }

  async drain(): Promise<void> {
    await this.#tail;
  }
}

export class AdapterRuntime {
  readonly #config: AdapterConfig;

  readonly #bridge: BridgeClientLike;

  readonly #adapterInstanceId = randomUUID();

  readonly #core: AhpCoreLike;

  readonly #callbackQueue = new SerialQueue();

  readonly #events = new Map<string, Map<string, PublishedEvent>>();

  readonly #bindings = new Map<string, ActiveBinding>();

  readonly #pendingBindings = new Map<string, PendingBinding>();

  readonly #eventFlushes = new Map<string, Promise<void>>();

  readonly #readOnlyEndpoints = new Set<string>();

  #stopping = false;

  constructor(
    config: AdapterConfig,
    dependencies: RuntimeDependencies = {},
  ) {
    this.#config = config;
    this.#bridge =
      dependencies.createBridgeClient?.(config) ??
      new BridgeClient(config.bridgePipePath, config.bridgeToken);
    const coreOptions: AhpCoreOptions = {
      userDataDirectory: config.userDataDirectory,
      clientId: config.adapterId,
      locale: "zh-CN",
      watch: true,
      callbacks: {
        onCatalogue: (snapshot) => {
          this.#callbackQueue.run(() => this.#publishCatalogue(snapshot));
        },
        onConnection: (event) => this.#onConnection(event),
        onSessionSnapshot: (event) => this.#onSessionSnapshot(event),
        onChatSnapshot: (event) => this.#onChatSnapshot(event),
        onAction: (event) => this.#onAction(event),
        onIncompatibility: (event) => this.#onIncompatibility(event),
        onError: (event) => {
          if (
            event.code === "invalid-snapshot" ||
            event.code.startsWith("internal:")
          ) {
            if (event.endpointId) {
              this.#readOnlyEndpoints.add(event.endpointId);
            }
          }
          safeLog("warn", "AHP core operation failed", {
            operation: event.operation,
            code: event.code,
            endpointId: event.endpointId,
          });
        },
      },
    };
    this.#core = dependencies.createCore?.(coreOptions) ?? new AhpCore(coreOptions);
  }

  async run(signal: AbortSignal): Promise<void> {
    const registration = parseRegisterResult(
      await this.#bridge.call<unknown>({
        operation: "ahp_adapter_register",
        registration: {
          adapter_id: this.#config.adapterId,
          adapter_instance_id: this.#adapterInstanceId,
          version: ADAPTER_VERSION,
          supported_protocols: [...SUPPORTED_PROTOCOL_VERSIONS],
        },
      }),
    );
    await this.#core.start();
    await this.#publishCatalogue(this.#core.catalogue);
    for (const binding of registration.bindings) {
      if (binding.state !== "binding" && binding.state !== "bound") {
        continue;
      }
      await this.#activateBinding(binding).catch((error) => {
        safeLog("warn", "Failed to restore AHP binding", {
          bindingId: binding.binding_id,
          code: errorCode(error),
        });
      });
    }

    while (!signal.aborted) {
      try {
        const poll = await this.#bridge.call<PollResult>(
          {
            operation: "ahp_poll_commands",
            adapter_id: this.#config.adapterId,
            adapter_instance_id: this.#adapterInstanceId,
            timeout_seconds: this.#config.pollSeconds,
          },
          (this.#config.pollSeconds + 5) * 1_000,
        );
        for (const command of poll.commands) {
          if (signal.aborted) {
            break;
          }
          await this.#executeCommand(command);
        }
      } catch (error) {
        if (!signal.aborted) {
          safeLog("warn", "Bridge command poll failed", {
            code: errorCode(error),
          });
          await delay(RETRY_DELAY_MS, signal);
        }
      }
    }
    await this.stop();
  }

  async stop(): Promise<void> {
    if (this.#stopping) {
      return;
    }
    this.#stopping = true;
    await this.#callbackQueue.drain();

    let failure: unknown;
    try {
      await Promise.all([...this.#eventFlushes.values()]);
    } catch (error) {
      failure = error;
    }

    const bindings = [...this.#bindings.values()];
    this.#bindings.clear();
    this.#pendingBindings.clear();
    this.#events.clear();
    this.#eventFlushes.clear();

    try {
      await Promise.all(bindings.map((binding) => binding.binding.close()));
    } catch (error) {
      failure ??= error;
    }

    try {
      await this.#core.stop();
    } catch (error) {
      failure ??= error;
    }

    if (failure !== undefined) {
      throw failure;
    }
  }

  async #executeCommand(command: AdapterCommand): Promise<void> {
    if (
      !Number.isSafeInteger(command.command_id) ||
      command.command_id <= 0 ||
      typeof command.binding_id !== "string" ||
      command.binding_id.length === 0 ||
      !Number.isSafeInteger(command.binding_generation)
    ) {
      await this.#ack(command.command_id, "rejected", "invalid-command");
      return;
    }
    try {
      if (command.kind !== "bind_session" && command.kind !== "unbind_session") {
        const active = this.#requireBinding(command);
        if (this.#readOnlyEndpoints.has(active.endpointId)) {
          throw new AhpOperationError(
            "binding-unavailable",
            "AHP compatibility gate is read-only",
          );
        }
      }
      switch (command.kind) {
        case "bind_session":
          await this.#activateBinding(parseBindingCommand(command));
          break;
        case "unbind_session":
          await this.#unbindBinding(command);
          break;
        case "send_message": {
          const data = requireRecord(command.data);
          const content = requireString(data.content, "content");
          await this.#requireBinding(command).binding.queueUserText(
            content,
            command.command_id,
          );
          break;
        }
        case "cancel_turn":
          await this.#requireBinding(command).binding.cancelActiveTurn(
            command.command_id,
          );
          break;
        case "approve_tool": {
          const data = requireRecord(command.data);
          await this.#requireBinding(command).binding.reviewToolParameters(
            {
              requestId: requireString(data.approval_key, "approval_key"),
              decision: requireBoolean(data.approved, "approved")
                ? "approve"
                : "deny",
            },
            command.command_id,
          );
          break;
        }
        case "review_tool_result": {
          const data = requireRecord(command.data);
          await this.#requireBinding(command).binding.reviewToolResult(
            {
              requestId: requireString(data.approval_key, "approval_key"),
              approved: requireBoolean(data.approved, "approved"),
            },
            command.command_id,
          );
          break;
        }
        case "complete_input": {
          const data = requireRecord(command.data);
          const active = this.#requireBinding(command).binding;
          const inputKey = requireString(data.input_key, "input_key");
          const answer = requireString(data.answer, "answer");
          await active.completeCurrentInput(
            buildInputCompletion(active, inputKey, answer),
            command.command_id,
          );
          break;
        }
        default:
          throw new AhpOperationError(
            "invalid-command",
            `Unsupported command kind ${String(command.kind)}`,
          );
      }
      await this.#ack(command.command_id, "applied");
    } catch (error) {
      const rejected = error instanceof AhpOperationError;
      await this.#ack(
        command.command_id,
        rejected ? "rejected" : "failed",
        errorCode(error),
      );
      safeLog("warn", "AHP command failed", {
        commandId: command.command_id,
        bindingId: command.binding_id,
        kind: command.kind,
        code: errorCode(error),
      });
    }
  }

  #requireBinding(command: AdapterCommand): ActiveBinding {
    const active = this.#bindings.get(command.binding_id);
    if (!active || active.generation !== command.binding_generation) {
      throw new AhpOperationError(
        "binding-unavailable",
        "Command targets a stale binding",
      );
    }
    return active;
  }

  async #unbindBinding(command: AdapterCommand): Promise<void> {
    const active = this.#bindings.get(command.binding_id);
    const pending = this.#pendingBindings.get(command.binding_id);
    if (
      active?.generation !== command.binding_generation &&
      pending?.generation !== command.binding_generation
    ) {
      throw new AhpOperationError(
        "binding-unavailable",
        "Command targets a stale binding",
      );
    }
    if (active) {
      await this.#flushEvents(command.binding_id);
    }
    this.#pendingBindings.delete(command.binding_id);
    this.#events.delete(command.binding_id);
    if (!active) {
      return;
    }
    this.#bindings.delete(command.binding_id);
    await active.binding.close();
  }

  async #activateBinding(binding: BridgeBinding): Promise<void> {
    const endpoint = this.#core.catalogue.endpoints.find(
      (entry) => entry.endpoint.id === binding.endpoint_id,
    );
    if (
      !endpoint ||
      endpoint.connection !== "connected" ||
      endpoint.endpoint.instanceId !== binding.host_instance_id
    ) {
      throw new AhpOperationError(
        "binding-unavailable",
        "Bound Agent Host is not connected",
      );
    }

    const existing = this.#bindings.get(binding.binding_id);
    if (
      existing &&
      existing.generation === binding.generation &&
      existing.endpointId === binding.endpoint_id &&
      existing.hostInstanceId === binding.host_instance_id &&
      existing.sessionUri === binding.session_uri
    ) {
      await this.#bridge.call({
        operation: "ahp_binding_ready",
        adapter_id: this.#config.adapterId,
        adapter_instance_id: this.#adapterInstanceId,
        binding_id: existing.bindingId,
        endpoint_id: existing.endpointId,
        host_instance_id: existing.hostInstanceId,
        binding_generation: existing.generation,
        session_uri: existing.sessionUri,
        chat_uri: existing.chatUri,
        last_server_sequence: binding.last_server_sequence,
      });
      await this.#flushEvents(binding.binding_id);
      return;
    }

    if (existing) {
      this.#bindings.delete(binding.binding_id);
      this.#events.delete(binding.binding_id);
      await existing.binding.close();
    } else {
      this.#events.delete(binding.binding_id);
    }

    const pending: PendingBinding = {
      bindingId: binding.binding_id,
      generation: binding.generation,
      endpointId: binding.endpoint_id,
      hostInstanceId: endpoint.endpoint.instanceId,
      sessionUri: binding.session_uri,
      lastServerSequence: binding.last_server_sequence,
    };
    this.#pendingBindings.set(binding.binding_id, pending);

    let sessionBinding: AhpSessionBinding | undefined;
    try {
      sessionBinding = await this.#core.bindSession(
        binding.endpoint_id,
        binding.session_uri,
      );
      const snapshot = sessionBinding.snapshot();
      const chatUri = snapshot.defaultChat?.resource;
      if (!chatUri) {
        throw new AhpOperationError(
          "chat-unavailable",
          "Bound session has no default chat",
        );
      }
      const normalizer = this.#ensurePendingNormalizer(pending, chatUri);
      const active: ActiveBinding = {
        bindingId: binding.binding_id,
        generation: binding.generation,
        endpointId: binding.endpoint_id,
        hostInstanceId: endpoint.endpoint.instanceId,
        sessionUri: binding.session_uri,
        chatUri,
        binding: sessionBinding,
        normalizer,
      };
      this.#bindings.set(binding.binding_id, active);
      await this.#bridge.call({
        operation: "ahp_binding_ready",
        adapter_id: this.#config.adapterId,
        adapter_instance_id: this.#adapterInstanceId,
        binding_id: active.bindingId,
        endpoint_id: active.endpointId,
        host_instance_id: active.hostInstanceId,
        binding_generation: active.generation,
        session_uri: active.sessionUri,
        chat_uri: active.chatUri,
        last_server_sequence: pending.lastServerSequence,
      });
      this.#pendingBindings.delete(binding.binding_id);
      await this.#flushEvents(binding.binding_id);
    } catch (error) {
      this.#pendingBindings.delete(binding.binding_id);
      this.#events.delete(binding.binding_id);
      this.#bindings.delete(binding.binding_id);
      await sessionBinding?.close().catch((closeError: unknown) => {
        safeLog("warn", "Failed to close rejected AHP binding", {
          bindingId: binding.binding_id,
          code: errorCode(closeError),
        });
      });
      try {
        await this.#bridge.call({
          operation: "ahp_binding_failed",
          adapter_id: this.#config.adapterId,
          adapter_instance_id: this.#adapterInstanceId,
          binding_id: binding.binding_id,
          binding_generation: binding.generation,
          reason_code: errorCode(error),
        });
      } catch (reportError) {
        safeLog("warn", "Failed to report rejected AHP binding", {
          bindingId: binding.binding_id,
          code: errorCode(reportError),
        });
      }
      throw error;
    }
  }

  #ensurePendingNormalizer(
    pending: PendingBinding,
    chatUri: string,
  ): AhpEventNormalizer {
    if (!pending.normalizer || pending.chatUri !== chatUri) {
      pending.chatUri = chatUri;
      pending.normalizer = this.#createNormalizer(pending, chatUri);
    }
    return pending.normalizer;
  }

  #createNormalizer(
    binding: Pick<
      PendingBinding | ActiveBinding,
      "endpointId" | "hostInstanceId" | "generation" | "sessionUri"
    >,
    chatUri: string,
  ): AhpEventNormalizer {
    return new AhpEventNormalizer({
      adapterId: this.#config.adapterId,
      endpointId: binding.endpointId,
      hostInstanceId: binding.hostInstanceId,
      generation: binding.generation,
      sessionUri: binding.sessionUri,
      chatUri,
    });
  }

  #onSessionSnapshot(event: SessionSnapshotEvent): void {
    const binding = this.#matchSessionBinding(event.endpointId, event.sessionUri);
    if (!binding) {
      return;
    }
    if ("binding" in binding) {
      this.#queueEvents(
        binding.bindingId,
        binding.normalizer.sessionSnapshot(event),
      );
      return;
    }
    binding.lastServerSequence = Math.max(
      binding.lastServerSequence,
      event.serverSeq,
    );
    if (!event.state.defaultChat) {
      return;
    }
    const normalizer = this.#ensurePendingNormalizer(
      binding,
      event.state.defaultChat,
    );
    this.#queueEvents(binding.bindingId, normalizer.sessionSnapshot(event));
  }

  #onChatSnapshot(event: ChatSnapshotEvent): void {
    const binding = this.#matchChatBinding(
      event.endpointId,
      event.sessionUri,
      event.chatUri,
    );
    if (!binding) {
      return;
    }
    if ("binding" in binding) {
      this.#queueEvents(binding.bindingId, binding.normalizer.chatSnapshot(event));
      return;
    }
    binding.lastServerSequence = Math.max(
      binding.lastServerSequence,
      event.serverSeq,
    );
    if (!binding.normalizer) {
      return;
    }
    this.#queueEvents(binding.bindingId, binding.normalizer.chatSnapshot(event));
  }

  #onAction(event: DomainActionEvent): void {
    if (event.scope === "root") {
      return;
    }
    const binding =
      event.scope === "chat"
        ? this.#matchChatBinding(
            event.endpointId,
            event.sessionUri,
            event.chatUri,
          )
        : this.#matchSessionBinding(event.endpointId, event.sessionUri);
    if (!binding) {
      return;
    }
    if ("binding" in binding) {
      if (event.envelope.rejectionReason !== undefined) {
        return;
      }
      this.#queueEvents(binding.bindingId, binding.normalizer.action(event));
      return;
    }
    binding.lastServerSequence = Math.max(
      binding.lastServerSequence,
      event.envelope.serverSeq,
    );
    if (event.envelope.rejectionReason !== undefined || !binding.normalizer) {
      return;
    }
    this.#queueEvents(binding.bindingId, binding.normalizer.action(event));
  }

  #onConnection(event: ConnectionEvent): void {
    if (
      event.status === "connected" &&
      event.selectedProtocol &&
      SUPPORTED_PROTOCOL_VERSIONS.includes(event.selectedProtocol)
    ) {
      this.#readOnlyEndpoints.delete(event.endpoint.id);
    }
    if (event.status !== "disconnected") {
      return;
    }
    for (const binding of this.#bindings.values()) {
      if (binding.endpointId !== event.endpoint.id) {
        continue;
      }
      this.#queueEvents(binding.bindingId, [
        binding.normalizer.hostDisconnected(
          "VS Code Agent Host 连接已中断，Adapter 正在重连。",
        ),
      ]);
    }
  }

  #onIncompatibility(event: IncompatibilityEvent): void {
    this.#readOnlyEndpoints.add(event.endpoint.id);
    safeLog("warn", "AHP protocol is incompatible; entering read-only mode", {
      endpointId: event.endpoint.id,
      reason: event.reason,
    });
  }

  #matchSessionBinding(
    endpointId: string,
    sessionUri: string,
  ): PendingBinding | ActiveBinding | undefined {
    return this.#resolveCallbackBinding(
      [...this.#pendingBindings.values()].filter(
        (binding) =>
          binding.endpointId === endpointId && binding.sessionUri === sessionUri,
      ),
      [...this.#bindings.values()].filter(
        (binding) =>
          binding.endpointId === endpointId && binding.sessionUri === sessionUri,
      ),
      { endpointId, sessionUri },
    );
  }

  #matchChatBinding(
    endpointId: string,
    sessionUri: string,
    chatUri: string,
  ): PendingBinding | ActiveBinding | undefined {
    return this.#resolveCallbackBinding(
      [...this.#pendingBindings.values()].filter(
        (binding) =>
          binding.endpointId === endpointId &&
          binding.sessionUri === sessionUri &&
          binding.chatUri === chatUri,
      ),
      [...this.#bindings.values()].filter(
        (binding) =>
          binding.endpointId === endpointId &&
          binding.sessionUri === sessionUri &&
          binding.chatUri === chatUri,
      ),
      { endpointId, sessionUri, chatUri },
    );
  }

  #resolveCallbackBinding(
    pendingMatches: readonly PendingBinding[],
    activeMatches: readonly ActiveBinding[],
    route: Readonly<Record<string, string>>,
  ): PendingBinding | ActiveBinding | undefined {
    const matches = [...pendingMatches, ...activeMatches];
    if (matches.length === 0) {
      return undefined;
    }
    if (matches.length > 1) {
      safeLog("warn", "AHP callback routing was ambiguous", {
        ...route,
        bindingIds: matches.map((binding) => binding.bindingId).join(","),
      });
      return undefined;
    }
    return matches[0];
  }

  #queueEvents(bindingId: string, events: readonly PublishedEvent[]): void {
    if (events.length === 0) {
      return;
    }
    let pending = this.#events.get(bindingId);
    if (!pending) {
      pending = new Map<string, PublishedEvent>();
      this.#events.set(bindingId, pending);
    }
    for (const event of events) {
      pending.set(event.event_id, event);
    }
    if (this.#bindings.has(bindingId)) {
      void this.#flushEvents(bindingId);
    }
  }

  #flushEvents(bindingId: string): Promise<void> {
    const existing = this.#eventFlushes.get(bindingId);
    if (existing) {
      return existing;
    }
    const flush = this.#flushEventsInner(bindingId).finally(() => {
      if (this.#eventFlushes.get(bindingId) === flush) {
        this.#eventFlushes.delete(bindingId);
      }
      const pending = this.#events.get(bindingId);
      if (
        pending &&
        pending.size > 0 &&
        this.#bindings.has(bindingId) &&
        !this.#stopping
      ) {
        setTimeout(() => void this.#flushEvents(bindingId), RETRY_DELAY_MS).unref();
      }
    });
    this.#eventFlushes.set(bindingId, flush);
    return flush;
  }

  async #flushEventsInner(bindingId: string): Promise<void> {
    for (;;) {
      const active = this.#bindings.get(bindingId);
      const pending = this.#events.get(bindingId);
      if (!active || !pending || pending.size === 0) {
        return;
      }
      const batch = [...pending.values()].slice(0, EVENT_BATCH_SIZE);
      await this.#bridge.call({
        operation: "ahp_publish_events",
        adapter_id: this.#config.adapterId,
        adapter_instance_id: this.#adapterInstanceId,
        binding_id: bindingId,
        binding_generation: active.generation,
        events: batch,
      });
      for (const event of batch) {
        pending.delete(event.event_id);
      }
      if (pending.size === 0) {
        this.#events.delete(bindingId);
      }
    }
  }

  async #publishCatalogue(snapshot: CatalogueSnapshot): Promise<void> {
    const hosts = snapshot.endpoints.map((entry) => ({
      endpoint_id: entry.endpoint.id,
      host_instance_id: entry.endpoint.instanceId,
      pid: entry.endpoint.pid,
      advertised_protocol: entry.endpoint.advertisedProtocol,
      selected_protocol: entry.selectedProtocol,
      state:
        entry.connection === "connected"
          ? !this.#readOnlyEndpoints.has(entry.endpoint.id)
            ? "connected"
            : "read_only"
          : entry.connection === "incompatible"
            ? "incompatible"
            : "unreachable",
    }));
    const sessions = snapshot.endpoints.flatMap((entry) =>
      entry.sessions.map((session) => ({
        endpoint_id: entry.endpoint.id,
        host_instance_id: entry.endpoint.instanceId,
        session_uri: session.resource,
        provider: session.provider,
        title: session.title,
        status: session.status,
        workspace_uris: session.workingDirectories ?? [],
        created_at: session.createdAt,
        modified_at: session.modifiedAt,
      })),
    );
    await this.#bridge.call({
      operation: "ahp_catalog_replace",
      adapter_id: this.#config.adapterId,
      adapter_instance_id: this.#adapterInstanceId,
      hosts,
      sessions,
    });
  }

  async #ack(
    commandId: number,
    outcome: "applied" | "rejected" | "failed",
    error?: string,
  ): Promise<void> {
    await this.#bridge.call({
      operation: "ahp_ack_command",
      adapter_id: this.#config.adapterId,
      adapter_instance_id: this.#adapterInstanceId,
      command_id: commandId,
      outcome,
      error_code: error,
    });
  }
}

function parseRegisterResult(value: unknown): RegisterResult {
  const data = requirePlainRecord(value, "Bridge registration result");
  const bindings = data.bindings;
  if (!Array.isArray(bindings)) {
    throw new Error("Bridge registration result bindings are invalid");
  }
  const foregroundBindingId = requireOptionalPlainString(
    data.foreground_binding_id,
    "foreground_binding_id",
  );
  return {
    bindings: bindings.map((binding) => parseBridgeBinding(binding)),
    ...(foregroundBindingId
      ? { foreground_binding_id: foregroundBindingId }
      : {}),
  };
}

function parseBridgeBinding(value: unknown): BridgeBinding {
  const data = requirePlainRecord(value, "Bridge binding");
  const hostInstanceId = requireOptionalPlainString(
    data.host_instance_id,
    "host_instance_id",
  );
  const chatUri = requireOptionalPlainString(data.chat_uri, "chat_uri");
  return {
    binding_id: requirePlainString(data.binding_id, "binding_id"),
    generation: requirePlainInteger(data.generation, "generation"),
    endpoint_id: requirePlainString(data.endpoint_id, "endpoint_id"),
    ...(hostInstanceId ? { host_instance_id: hostInstanceId } : {}),
    session_uri: requirePlainString(data.session_uri, "session_uri"),
    ...(chatUri ? { chat_uri: chatUri } : {}),
    state: requirePlainString(data.state, "state"),
    last_server_sequence: requirePlainInteger(
      data.last_server_sequence,
      "last_server_sequence",
    ),
  };
}

function parseBindingCommand(command: AdapterCommand): BridgeBinding {
  const data = requireRecord(command.data);
  return {
    binding_id: command.binding_id,
    generation: command.binding_generation,
    endpoint_id: requireString(data.endpoint_id, "endpoint_id"),
    host_instance_id: requireString(data.host_instance_id, "host_instance_id"),
    session_uri: requireString(data.session_uri, "session_uri"),
    state: "binding",
    last_server_sequence: 0,
  };
}

function requirePlainRecord(
  value: unknown,
  name: string,
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${name} is invalid`);
  }
  return value as Record<string, unknown>;
}

function requirePlainString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
  return value;
}

function requireOptionalPlainString(
  value: unknown,
  name: string,
): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  return requirePlainString(value, name);
}

function requirePlainInteger(value: unknown, name: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new Error(`${name} must be an integer`);
  }
  return value;
}

function requireRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new AhpOperationError("invalid-command", "Command data is invalid");
  }
  return value as Record<string, unknown>;
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new AhpOperationError(
      "invalid-command",
      `${name} must be a non-empty string`,
    );
  }
  return value;
}

function requireBoolean(value: unknown, name: string): boolean {
  if (typeof value !== "boolean") {
    throw new AhpOperationError(
      "invalid-command",
      `${name} must be a boolean`,
    );
  }
  return value;
}

function errorCode(error: unknown): string {
  if (error instanceof AhpOperationError || error instanceof BridgeRpcError) {
    return sanitizeCode(error.code);
  }
  if (error instanceof Error) {
    return `internal-${sanitizeCode(error.name)}`;
  }
  return "internal-unknown";
}

function sanitizeCode(value: string): string {
  const safe = value.replace(/[^A-Za-z0-9_.-]/gu, "-").slice(0, 100);
  return safe || "unknown";
}

function safeLog(
  level: "info" | "warn",
  message: string,
  fields: Readonly<Record<string, unknown>> = {},
): void {
  const output = JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    message,
    ...fields,
  });
  process.stderr.write(`${output}\n`);
}

function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, milliseconds);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

async function main(): Promise<void> {
  const args = parseAdapterArguments(process.argv.slice(2));
  const config = await loadAdapterConfig(
    args.configPath,
    args.userDataDirectory,
  );
  const abort = new AbortController();
  const stop = (): void => abort.abort();
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  const runtime = new AdapterRuntime(config);
  await runtime.run(abort.signal);
}

function isMainModule(): boolean {
  const entry = process.argv[1];
  return typeof entry === "string" && import.meta.url === pathToFileURL(entry).href;
}

if (isMainModule()) {
  void main().catch((error: unknown) => {
    safeLog("warn", "AHP Adapter stopped", { code: errorCode(error) });
    process.exitCode = 1;
  });
}
