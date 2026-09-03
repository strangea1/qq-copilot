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
  connectManagedTarget,
  createManagedSession,
  disposeManagedSession,
  managedTargetMatchesWorkspaceUri,
  prepareTargetResult,
  refreshManagedSessions,
  type ConnectedManagedTarget,
  type ManagedTarget,
  type PrepareTargetResult,
} from "./managed-target.js";
import {
  AhpEventNormalizer,
  type PublishedEvent,
} from "./event-normalizer.js";
import {
  discoverEditorEndpoints,
  endpointPublicId,
  watchEditorEndpoints,
  type WatchEditorEndpointsOptions,
} from "./endpoint-registry.js";
import { buildInputCompletion } from "./input-completion.js";
import type { EndpointRegistryEntry } from "./endpoint-registry.js";

const ADAPTER_VERSION = "0.1.0";
const EVENT_BATCH_SIZE = 64;
const RETRY_DELAY_MS = 1_000;
const ACK_RETRY_DELAY_MS = 1_000;
const MANAGED_BIND_GRACE_MS = 60_000;
const NEGOTIATED_PROTOCOL_VERSIONS = [...SUPPORTED_PROTOCOL_VERSIONS];

export interface BridgeBinding {
  readonly binding_id: string;
  readonly generation: number;
  readonly endpoint_id: string;
  readonly host_instance_id?: string;
  readonly session_uri: string;
  readonly chat_uri?: string;
  readonly state: string;
  readonly last_server_sequence: number;
  readonly host_label?: string;
  readonly ssh_alias?: string;
  readonly target_kind?: "local" | "ssh";
  readonly target_path?: string;
  readonly editor_client_tools_available?: boolean;
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
    | "complete_input"
    | "prepare_target"
    | "create_session"
    | "dispose_session";
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
  refreshEndpoints(): Promise<CatalogueSnapshot>;
  bindSession(endpointId: string, sessionUri: string): Promise<AhpSessionBinding>;
}

export interface RuntimeDependencies {
  readonly createBridgeClient?: (config: AdapterConfig) => BridgeClientLike;
  readonly createCore?: (options: AhpCoreOptions) => AhpCoreLike;
}

interface ManagedEntryState {
  readonly key: string;
  readonly target: ManagedTarget;
  readonly entry: EndpointRegistryEntry;
  readonly prepared: ConnectedManagedTarget["prepared"];
  readonly protectedUntil: number;
}

class SerialQueue {
  #tail: Promise<void> = Promise.resolve();

  run(task: () => Promise<void>): Promise<void> {
    const next = this.#tail.then(task, task);
    this.#tail = next.catch(() => undefined);
    return next;
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

  readonly #catalogueQueue = new SerialQueue();

  readonly #commandQueues = new Map<string, SerialQueue>();

  readonly #inFlightCommandIds = new Set<number>();

  readonly #removedSessionUris = new Map<string, string>();

  readonly #events = new Map<string, Map<string, PublishedEvent>>();

  readonly #managedEntries = new Map<string, ManagedEntryState>();

  readonly #bindings = new Map<string, ActiveBinding>();

  readonly #pendingBindings = new Map<string, PendingBinding>();

  readonly #eventFlushes = new Map<string, Promise<void>>();

  readonly #readOnlyEndpoints = new Set<string>();

  readonly #restoreBindings = new Map<string, BridgeBinding>();

  readonly #restoreManagedTargets = new Map<string, ManagedTarget>();

  readonly #restoringBindings = new Set<string>();

  #restoreRetryTimer: NodeJS.Timeout | undefined;

  #managedPruneTimer: NodeJS.Timeout | undefined;

  #stopping = false;

  constructor(
    config: AdapterConfig,
    dependencies: RuntimeDependencies = {},
  ) {
    const runtime = this;
    this.#config = config;
    this.#bridge =
      dependencies.createBridgeClient?.(config) ??
      new BridgeClient(config.bridgePipePath, config.bridgeToken);
    const coreOptions: AhpCoreOptions = {
      userDataDirectory: config.userDataDirectory,
      clientId: config.adapterId,
      locale: "zh-CN",
      watch: true,
      dependencies: {
        discoverEndpoints: async (userDataDirectory) => [
          ...(await discoverEditorEndpoints(userDataDirectory)),
          ...[...this.#managedEntries.values()].map((entry) => entry.entry),
        ],
        watchEndpoints: async function* (
          userDataDirectory: string,
          options?: WatchEditorEndpointsOptions,
        ) {
          for await (const entries of watchEditorEndpoints(
            userDataDirectory,
            options,
          )) {
            yield [
              ...entries,
              ...[...runtime.#managedEntries.values()].map((entry) => entry.entry),
            ];
          }
        },
      },
      callbacks: {
        onCatalogue: (snapshot) => {
          if (this.#stopping) {
            return;
          }
          void this.#publishCatalogue(snapshot).catch((error) => {
            safeLog("warn", "Failed to publish AHP catalogue", {
              code: errorCode(error),
            });
          });
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
    for (const binding of registration.bindings) {
      if (binding.state !== "binding" && binding.state !== "bound") {
        continue;
      }
      this.#restoreBindings.set(binding.binding_id, binding);
      const target = managedTargetFromBinding(
        binding,
        this.#config.authorizedTargets,
      );
      if (target) {
        this.#restoreManagedTargets.set(binding.binding_id, target);
      }
    }
    const preparedTargets = new Set<string>();
    for (const [bindingId, target] of this.#restoreManagedTargets) {
      const targetKey = managedTargetKey(target);
      if (preparedTargets.has(targetKey)) {
        continue;
      }
      try {
        await this.#prepareManagedBindingTarget(target);
        preparedTargets.add(targetKey);
      } catch (error) {
        safeLog("warn", "Failed to prepare managed AHP target during restore", {
          bindingId,
          code: errorCode(error),
        });
      }
    }
    await this.#core.start();
    await this.#publishCatalogue(this.#core.catalogue);
    await this.#tryRestoreBindings();

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
          this.#enqueueCommand(command);
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
    if (this.#restoreRetryTimer) {
      clearTimeout(this.#restoreRetryTimer);
      this.#restoreRetryTimer = undefined;
    }
    if (this.#managedPruneTimer) {
      clearTimeout(this.#managedPruneTimer);
      this.#managedPruneTimer = undefined;
    }
    await Promise.all(
      [...this.#commandQueues.values()].map((queue) => queue.drain()),
    );
    this.#commandQueues.clear();
    this.#inFlightCommandIds.clear();
    await this.#callbackQueue.drain();
    await this.#catalogueQueue.drain();

    let failure: unknown;
    try {
      await Promise.all([...this.#eventFlushes.values()]);
    } catch (error) {
      failure = error;
    }

    const bindings = [...this.#bindings.values()];
    this.#bindings.clear();
    this.#pendingBindings.clear();
    this.#restoreBindings.clear();
    this.#restoreManagedTargets.clear();
    this.#restoringBindings.clear();
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

    for (const entry of this.#managedEntries.values()) {
      if (entry.prepared.tunnel && !entry.prepared.tunnel.killed) {
        entry.prepared.tunnel.kill();
      }
    }
    this.#managedEntries.clear();

    if (failure !== undefined) {
      throw failure;
    }
  }

  #enqueueCommand(command: AdapterCommand): void {
    if (this.#inFlightCommandIds.has(command.command_id)) {
      return;
    }
    this.#inFlightCommandIds.add(command.command_id);
    let queue = this.#commandQueues.get(command.binding_id);
    if (!queue) {
      queue = new SerialQueue();
      this.#commandQueues.set(command.binding_id, queue);
    }
    queue.run(async () => {
      try {
        await this.#executeCommand(command);
      } catch (error) {
        safeLog("warn", "AHP command execution could not be reported", {
          commandId: command.command_id,
          bindingId: command.binding_id,
          kind: command.kind,
          code: errorCode(error),
        });
      } finally {
        this.#inFlightCommandIds.delete(command.command_id);
      }
    });
  }

  async #executeCommand(command: AdapterCommand): Promise<void> {
    if (
      !Number.isSafeInteger(command.command_id) ||
      command.command_id <= 0 ||
      typeof command.binding_id !== "string" ||
      command.binding_id.length === 0 ||
      !Number.isSafeInteger(command.binding_generation)
    ) {
      await this.#ackWithRetry(
        command.command_id,
        "rejected",
        "invalid-command",
      );
      return;
    }
    let result: unknown;
    let operationError: unknown;
    let operationFailed = false;
    try {
      if (
        command.kind === "send_message" ||
        command.kind === "cancel_turn" ||
        command.kind === "approve_tool" ||
        command.kind === "review_tool_result" ||
        command.kind === "complete_input"
      ) {
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
          this.#clearPendingRestore(command.binding_id);
          await this.#activateBinding(parseBindingCommand(command));
          break;
        case "unbind_session":
          this.#clearPendingRestore(command.binding_id);
          await this.#unbindBinding(command);
          await this.#pruneManagedEntries();
          break;
        case "send_message": {
          const data = requireRecord(command.data);
          const content = requireString(data.content, "content");
          result = await this.#requireBinding(command).binding.queueUserText(
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
        case "prepare_target": {
          result = await this.#prepareTarget(command);
          break;
        }
        case "create_session": {
          result = await this.#createSession(command);
          break;
        }
        case "dispose_session":
          await this.#disposeSession(command);
          break;
        default:
          throw new AhpOperationError(
            "invalid-command",
            `Unsupported command kind ${String(command.kind)}`,
          );
      }
    } catch (error) {
      operationFailed = true;
      operationError = error;
    }
    if (operationFailed) {
      const rejected = operationError instanceof AhpOperationError;
      safeLog("warn", "AHP command failed", {
        commandId: command.command_id,
        bindingId: command.binding_id,
        kind: command.kind,
        code: errorCode(operationError),
      });
      await this.#ackWithRetry(
        command.command_id,
        rejected ? "rejected" : "failed",
        errorCode(operationError),
      );
      return;
    }
    await this.#ackWithRetry(
      command.command_id,
      "applied",
      undefined,
      result,
    );
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

  async #tryRestoreBindings(): Promise<void> {
    if (this.#stopping) {
      return;
    }
    for (const [bindingId, binding] of [...this.#restoreBindings]) {
      if (
        this.#bindings.has(bindingId) ||
        this.#pendingBindings.has(bindingId) ||
        this.#restoringBindings.has(bindingId)
      ) {
        continue;
      }
      this.#restoringBindings.add(bindingId);
      try {
        const target = this.#restoreManagedTargets.get(bindingId);
        if (target) {
          if (!this.#managedEntries.has(managedTargetKey(target))) {
            await this.#prepareManagedBindingTarget(target);
          } else {
            await this.#core.refreshEndpoints();
          }
          this.#restoreManagedTargets.delete(bindingId);
        }
        const endpoint = this.#core.catalogue.endpoints.find(
          (entry) =>
            entry.endpoint.id === binding.endpoint_id &&
            entry.endpoint.instanceId === binding.host_instance_id &&
            entry.connection === "connected",
        );
        if (!endpoint) {
          continue;
        }
        await this.#activateBinding(binding);
        this.#restoreBindings.delete(bindingId);
        this.#restoreManagedTargets.delete(bindingId);
      } catch (error) {
        safeLog("warn", "Failed to prepare or restore AHP binding", {
          bindingId,
          code: errorCode(error),
        });
      } finally {
        this.#restoringBindings.delete(bindingId);
      }
    }
    this.#scheduleRestoreRetry();
  }

  #scheduleRestoreRetry(): void {
    if (
      this.#restoreRetryTimer ||
      this.#restoreBindings.size === 0 ||
      this.#stopping
    ) {
      return;
    }
    this.#restoreRetryTimer = setTimeout(() => {
      this.#restoreRetryTimer = undefined;
      this.#callbackQueue.run(() => this.#tryRestoreBindings());
    }, RETRY_DELAY_MS);
    this.#restoreRetryTimer.unref();
  }

  #clearPendingRestore(bindingId: string): void {
    this.#restoreBindings.delete(bindingId);
    this.#restoreManagedTargets.delete(bindingId);
    this.#restoringBindings.delete(bindingId);
    if (this.#restoreBindings.size === 0 && this.#restoreRetryTimer) {
      clearTimeout(this.#restoreRetryTimer);
      this.#restoreRetryTimer = undefined;
    }
  }

  async #prepareTarget(command: AdapterCommand): Promise<PrepareTargetResult> {
    const data = requireRecord(command.data);
    const target = parseManagedTarget(data.target);
    const advanced =
      "advanced" in data ? requireBoolean(data.advanced, "advanced") : false;
    const retainConnection =
      "retain_connection" in data
        ? requireBoolean(data.retain_connection, "retain_connection")
        : false;
    const currentConfig =
      "config" in data ? requireRecord(data.config, "config") : {};
    const connection = await connectManagedTarget(this.#config, target);
    const alreadyRetained = this.#managedEntries.has(managedTargetKey(target));
    let retained = false;
    try {
      const result = await prepareTargetResult(
        connection,
        advanced,
        currentConfig,
      );
      if (retainConnection) {
        await this.#publishManagedCatalogue(connection);
        await this.#retainManagedPrepared(target, connection.prepared);
        retained = true;
      }
      return result;
    } finally {
      if (retained) {
        await connection.client.shutdown().catch(() => undefined);
      } else {
        await connection.close().catch(() => undefined);
        if (!alreadyRetained) {
          await this.#publishManagedCatalogue(
            connection,
            connection.sessions,
            "unreachable",
          );
        }
      }
    }
  }

  async #prepareManagedBindingTarget(target: ManagedTarget): Promise<void> {
    const connection = await connectManagedTarget(this.#config, target);
    try {
      await this.#publishManagedCatalogue(connection);
      await this.#retainManagedPrepared(target, connection.prepared);
      await connection.client.shutdown().catch(() => undefined);
    } catch (error) {
      await connection.close().catch(() => undefined);
      throw error;
    }
  }

  async #createSession(command: AdapterCommand): Promise<Record<string, unknown>> {
    const data = requireRecord(command.data);
    const target = parseManagedTarget(data.target);
    const provider = requireString(data.provider, "provider");
    const sessionUri = requireString(data.session_uri, "session_uri");
    const workspaceUri = requireString(data.workspace_uri, "workspace_uri");
    const resolvedValues = requireRecord(data.resolved_values, "resolved_values");
    const overrides = requireRecord(data.overrides, "overrides");
    const connection = await connectManagedTarget(
      this.#config,
      target,
      async (progress) => {
        await this.#bridge.call({
          operation: "ahp_command_progress",
          adapter_id: this.#config.adapterId,
          adapter_instance_id: this.#adapterInstanceId,
          command_id: command.command_id,
          progress: progress.progress,
          ...(progress.total !== undefined ? { total: progress.total } : {}),
          ...(progress.message ? { message: progress.message } : {}),
        });
      },
    );
    let createdSessionUri: string | undefined;
    try {
      const result = await createManagedSession(connection, {
        provider,
        sessionUri,
        workspaceUri,
        resolvedValues,
        overrides,
      });
      createdSessionUri = result.session.resource;
      this.#removedSessionUris.delete(createdSessionUri);
      const session = toBridgeSession(
        connection.prepared,
        target,
        result.session,
      );
      const sessions = await refreshManagedSessions(connection.client);
      await this.#publishManagedCatalogue(connection, sessions);
      await this.#retainManagedPrepared(target, connection.prepared);
      await connection.client.shutdown().catch(() => undefined);
      return {
        endpoint_id: result.endpoint_id,
        host_instance_id: result.host_instance_id,
        workspace_uri: result.workspace_uri,
        host_label: result.host_label,
        editor_client_tools_available: result.editor_client_tools_available,
        session,
      };
    } catch (error) {
      let cleanupError: unknown;
      if (createdSessionUri) {
        try {
          await disposeManagedSession(connection, createdSessionUri);
          this.#removedSessionUris.set(
            createdSessionUri,
            managedTargetKey(target),
          );
          const sessions = await refreshManagedSessions(connection.client);
          await this.#publishManagedCatalogue(
            connection,
            sessions,
            this.#managedEntries.has(managedTargetKey(target))
              ? "connected"
              : "unreachable",
            [createdSessionUri],
          );
        } catch (disposeError) {
          cleanupError = disposeError;
        }
      }
      await connection.close().catch(() => undefined);
      if (cleanupError !== undefined) {
        throw new AggregateError(
          [error, cleanupError],
          "created-session-cleanup-failed",
        );
      }
      throw error;
    }
  }

  async #disposeSession(command: AdapterCommand): Promise<void> {
    const data = requireRecord(command.data);
    const target = parseManagedTarget(data.target);
    const sessionUri = requireString(data.session_uri, "session_uri");
    const alreadyRetained = this.#managedEntries.has(managedTargetKey(target));
    const connection = await connectManagedTarget(this.#config, target);
    try {
      await disposeManagedSession(connection, sessionUri);
      this.#removedSessionUris.set(sessionUri, managedTargetKey(target));
      await this.#forgetRemovedSessionBindings(sessionUri);
      const sessions = await refreshManagedSessions(connection.client);
      await this.#publishManagedCatalogue(
        connection,
        sessions,
        alreadyRetained ? "connected" : "unreachable",
        [sessionUri],
      );
    } finally {
      await connection.close().catch(() => undefined);
    }
  }

  async #forgetRemovedSessionBindings(sessionUri: string): Promise<void> {
    for (const [bindingId, binding] of [...this.#bindings]) {
      if (binding.sessionUri !== sessionUri) {
        continue;
      }
      this.#bindings.delete(bindingId);
      this.#events.delete(bindingId);
      this.#eventFlushes.delete(bindingId);
      try {
        await binding.binding.close();
      } catch (error) {
        safeLog("warn", "Failed to close a binding for a removed Session", {
          bindingId,
          code: errorCode(error),
        });
      }
    }
    for (const [bindingId, binding] of [...this.#pendingBindings]) {
      if (binding.sessionUri === sessionUri) {
        this.#pendingBindings.delete(bindingId);
        this.#events.delete(bindingId);
        this.#eventFlushes.delete(bindingId);
      }
    }
    for (const [bindingId, binding] of [...this.#restoreBindings]) {
      if (binding.session_uri === sessionUri) {
        this.#clearPendingRestore(bindingId);
      }
    }
  }

  async #retainManagedPrepared(
    target: ManagedTarget,
    prepared: ConnectedManagedTarget["prepared"],
  ): Promise<void> {
    const key = managedTargetKey(target);
    const previous = this.#managedEntries.get(key);
    this.#managedEntries.set(key, {
      key,
      target,
      entry: prepared.entry,
      prepared,
      protectedUntil: Date.now() + MANAGED_BIND_GRACE_MS,
    });
    try {
      await this.#core.refreshEndpoints();
    } catch (error) {
      if (previous) {
        this.#managedEntries.set(key, previous);
      } else {
        this.#managedEntries.delete(key);
      }
      if (prepared.tunnel && !prepared.tunnel.killed) {
        prepared.tunnel.kill();
      }
      await this.#core.refreshEndpoints().catch(() => undefined);
      throw error;
    }
    if (
      previous?.prepared !== prepared &&
      previous?.prepared.tunnel &&
      !previous.prepared.tunnel.killed
    ) {
      previous.prepared.tunnel.kill();
    }
    await this.#pruneManagedEntries();
  }

  async #pruneManagedEntries(): Promise<void> {
    if (this.#managedPruneTimer) {
      clearTimeout(this.#managedPruneTimer);
      this.#managedPruneTimer = undefined;
    }
    let changed = false;
    let nextPruneDelay: number | undefined;
    const now = Date.now();
    for (const [key, entry] of [...this.#managedEntries.entries()]) {
      const endpointId = endpointPublicId(entry.entry);
      if (
        [...this.#bindings.values(), ...this.#pendingBindings.values()].some(
          (binding) => binding.endpointId === endpointId,
        ) ||
        [...this.#restoreBindings.values()].some(
          (binding) => binding.endpoint_id === endpointId,
        )
      ) {
        continue;
      }
      if (entry.protectedUntil > now) {
        const delay = entry.protectedUntil - now;
        nextPruneDelay =
          nextPruneDelay === undefined ? delay : Math.min(nextPruneDelay, delay);
        continue;
      }
      if (entry.prepared.tunnel && !entry.prepared.tunnel.killed) {
        entry.prepared.tunnel.kill();
      }
      this.#managedEntries.delete(key);
      changed = true;
    }
    if (changed) {
      await this.#core.refreshEndpoints();
    }
    if (nextPruneDelay !== undefined && !this.#stopping) {
      this.#managedPruneTimer = setTimeout(() => {
        this.#managedPruneTimer = undefined;
        void this.#pruneManagedEntries().catch((error) => {
          safeLog("warn", "Failed to prune idle managed endpoint", {
            code: errorCode(error),
          });
        });
      }, Math.max(1, nextPruneDelay));
      this.#managedPruneTimer.unref();
    }
  }

  async #publishManagedCatalogue(
    connection: ConnectedManagedTarget,
    sessions = connection.sessions,
    hostState: "connected" | "unreachable" = "connected",
    removedSessionUris: readonly string[] = [],
  ): Promise<void> {
    const { prepared } = connection;
    const target = prepared.target;
    const targetKey = managedTargetKey(target);
    const pendingRemovals = [...this.#removedSessionUris]
      .filter(([, removedTargetKey]) => removedTargetKey === targetKey)
      .map(([sessionUri]) => sessionUri);
    const removals = [...new Set([...pendingRemovals, ...removedSessionUris])];
    await this.#catalogueQueue.run(async () => {
      await this.#bridge.call({
        operation: "ahp_catalog_replace",
        adapter_id: this.#config.adapterId,
        adapter_instance_id: this.#adapterInstanceId,
        hosts: [
          {
            endpoint_id: prepared.endpointId,
            host_instance_id: prepared.entry.instanceId,
            pid: prepared.entry.pid,
            advertised_protocol: prepared.entry.protocolVersion,
            selected_protocol: prepared.entry.protocolVersion,
            state: hostState,
            host_label: prepared.hostLabel,
            ...(target.kind === "ssh"
              ? {
                  ssh_alias: target.alias,
                  target_kind: "ssh",
                  target_path: target.path,
                }
              : {
                  target_kind: "local",
                  target_path: target.path,
                }),
            endpoint_type: prepared.entry.endpoint.type,
            editor_client_tools_available: prepared.editorClientToolsAvailable,
          },
        ],
        sessions: sessions
          .filter((session) => !removals.includes(session.resource))
          .map((session) => toBridgeSession(prepared, target, session)),
        removed_session_uris: removals,
        full_snapshot: false,
      });
      for (const sessionUri of removals) {
        if (this.#removedSessionUris.get(sessionUri) === targetKey) {
          this.#removedSessionUris.delete(sessionUri);
        }
      }
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
      NEGOTIATED_PROTOCOL_VERSIONS.includes(event.selectedProtocol)
    ) {
      this.#readOnlyEndpoints.delete(event.endpoint.id);
      if (this.#restoreBindings.size > 0) {
        this.#callbackQueue.run(() => this.#tryRestoreBindings());
      }
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
    const hosts = snapshot.endpoints.map((entry) => {
      const managed = this.#managedEntryByEndpointId(entry.endpoint.id);
      return {
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
        host_label: managed?.prepared.hostLabel ?? "local",
        ...(managed?.target.kind === "ssh"
          ? {
              ssh_alias: managed.target.alias,
              target_kind: "ssh",
              target_path: managed.target.path,
            }
          : managed
            ? {
                target_kind: "local",
                target_path: managed.target.path,
              }
            : {}),
        endpoint_type: managed?.entry.endpoint.type ?? "socket",
        editor_client_tools_available:
          managed?.prepared.editorClientToolsAvailable ?? true,
      };
    });
    const sessions = snapshot.endpoints.flatMap((entry) => {
      const managed = this.#managedEntryByEndpointId(entry.endpoint.id);
      return entry.sessions
        .filter(
          (session) => !this.#removedSessionUris.has(session.resource),
        )
        .map((session) =>
          managed
            ? toBridgeSession(managed.prepared, managed.target, session)
            : {
                endpoint_id: entry.endpoint.id,
                host_instance_id: entry.endpoint.instanceId,
                session_uri: session.resource,
                provider: session.provider,
                title: session.title,
                status: session.status,
                workspace_uris: session.workingDirectories ?? [],
                created_at: session.createdAt,
                modified_at: session.modifiedAt,
                host_label: "local",
                editor_client_tools_available: true,
              },
        );
    });
    const pendingRemovals = [...this.#removedSessionUris.keys()];
    await this.#catalogueQueue.run(async () => {
      await this.#bridge.call({
        operation: "ahp_catalog_replace",
        adapter_id: this.#config.adapterId,
        adapter_instance_id: this.#adapterInstanceId,
        hosts,
        sessions,
        removed_session_uris: pendingRemovals,
        full_snapshot: true,
      });
      for (const sessionUri of pendingRemovals) {
        this.#removedSessionUris.delete(sessionUri);
      }
    });
  }

  #managedEntryByEndpointId(endpointId: string): ManagedEntryState | undefined {
    return [...this.#managedEntries.values()].find(
      (entry) => endpointPublicId(entry.entry) === endpointId,
    );
  }

  async #ackWithRetry(
    commandId: number,
    outcome: "applied" | "rejected" | "failed",
    error?: string,
    result?: unknown,
  ): Promise<void> {
    let lastError: unknown;
    for (;;) {
      try {
        await this.#bridge.call({
          operation: "ahp_ack_command",
          adapter_id: this.#config.adapterId,
          adapter_instance_id: this.#adapterInstanceId,
          command_id: commandId,
          outcome,
          error_code: error,
          ...(result !== undefined ? { result } : {}),
        });
        return;
      } catch (ackError) {
        lastError = ackError;
        if (this.#stopping) {
          break;
        }
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, ACK_RETRY_DELAY_MS);
          timer.unref();
        });
      }
    }
    throw lastError ?? new Error("Adapter stopped before command acknowledgement");
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
  const hostLabel = requireOptionalPlainString(data.host_label, "host_label");
  const sshAlias = requireOptionalPlainString(data.ssh_alias, "ssh_alias");
  const rawTargetKind = requireOptionalPlainString(data.target_kind, "target_kind");
  let targetKind: "local" | "ssh" | undefined;
  if (rawTargetKind === "local" || rawTargetKind === "ssh") {
    targetKind = rawTargetKind;
  } else if (rawTargetKind !== undefined) {
    throw new Error("target_kind is invalid");
  }
  const targetPath = requireOptionalPlainString(data.target_path, "target_path");
  const editorClientToolsAvailable =
    data.editor_client_tools_available === undefined ||
    data.editor_client_tools_available === null
      ? undefined
      : requirePlainBoolean(
          data.editor_client_tools_available,
          "editor_client_tools_available",
        );
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
    ...(hostLabel ? { host_label: hostLabel } : {}),
    ...(sshAlias ? { ssh_alias: sshAlias } : {}),
    ...(targetKind ? { target_kind: targetKind } : {}),
    ...(targetPath ? { target_path: targetPath } : {}),
    ...(editorClientToolsAvailable !== undefined
      ? { editor_client_tools_available: editorClientToolsAvailable }
      : {}),
  };
}

function parseManagedTarget(value: unknown): ManagedTarget {
  const record = requireRecord(value);
  if (record.kind === "local") {
    return {
      kind: "local",
      path: requireString(record.path, "path"),
    };
  }
  if (record.kind === "ssh") {
    return {
      kind: "ssh",
      alias: requireString(record.alias, "alias"),
      path: requireString(record.path, "path"),
      user: requireString(record.user, "user"),
      host: requireString(record.host, "host"),
      port: requireInteger(record.port, "port"),
      hostKeyFingerprints: requireStringArray(
        record.host_key_fingerprints,
        "host_key_fingerprints",
      ),
    };
  }
  throw new AhpOperationError(
    "invalid-command",
    "Managed target kind is invalid",
  );
}

function managedTargetFromBinding(
  binding: BridgeBinding,
  targets: readonly ManagedTarget[],
): ManagedTarget | undefined {
  if (binding.target_kind === "local" && binding.target_path) {
    return targets.find(
      (target) =>
        target.kind === "local" &&
        target.path.toLocaleLowerCase("en-US") ===
          binding.target_path?.toLocaleLowerCase("en-US"),
    );
  }
  if (
    binding.target_kind === "ssh" &&
    binding.target_path &&
    binding.ssh_alias
  ) {
    return targets.find(
      (target) =>
        target.kind === "ssh" &&
        target.alias === binding.ssh_alias &&
        target.path === binding.target_path,
    );
  }
  return undefined;
}

function managedTargetKey(target: ManagedTarget): string {
  return target.kind === "local"
    ? `local:${target.path.toLowerCase()}`
    : `ssh:${target.alias}\0${target.path}`;
}

function toBridgeSession(
  prepared: ConnectedManagedTarget["prepared"],
  target: ManagedTarget,
  session: {
    readonly resource: string;
    readonly provider: string;
    readonly title: string;
    readonly status: number;
    readonly workingDirectories?: readonly string[];
    readonly createdAt: string;
    readonly modifiedAt: string;
  },
): Record<string, unknown> {
  const workingDirectories = session.workingDirectories ?? [];
  const matchesTarget = workingDirectories.some((workspaceUri) =>
    managedTargetMatchesWorkspaceUri(target, workspaceUri),
  );
  return {
    endpoint_id: prepared.endpointId,
    host_instance_id: prepared.entry.instanceId,
    session_uri: session.resource,
    provider: session.provider,
    title: session.title,
    status: session.status,
    workspace_uris: [...workingDirectories],
    created_at: session.createdAt,
    modified_at: session.modifiedAt,
    host_label: prepared.hostLabel,
    ...(target.kind === "ssh"
      ? {
          ssh_alias: target.alias,
          ...(matchesTarget
            ? {
                target_kind: "ssh",
                target_path: target.path,
              }
            : {}),
        }
      : matchesTarget
        ? {
            target_kind: "local",
            target_path: target.path,
          }
        : {}),
    editor_client_tools_available: prepared.editorClientToolsAvailable,
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

function requirePlainBoolean(value: unknown, name: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`${name} must be a boolean`);
  }
  return value;
}

function requireRecord(
  value: unknown,
  name = "command data",
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new AhpOperationError("invalid-command", `${name} is invalid`);
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

function requireStringArray(value: unknown, name: string): string[] {
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    value.some((item) => typeof item !== "string" || item.length === 0)
  ) {
    throw new AhpOperationError(
      "invalid-command",
      `${name} must be a non-empty string array`,
    );
  }
  return value;
}

function requireInteger(value: unknown, name: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new AhpOperationError(
      "invalid-command",
      `${name} must be an integer`,
    );
  }
  return value;
}

function errorCode(error: unknown): string {
  if (error instanceof AhpOperationError || error instanceof BridgeRpcError) {
    return sanitizeCode(error.code);
  }
  if (error instanceof Error) {
    if ("code" in error && typeof error.code === "string") {
      return sanitizeCode(error.code);
    }
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
