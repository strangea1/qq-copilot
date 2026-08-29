import { randomUUID } from "node:crypto";

import { SUPPORTED_PROTOCOL_VERSIONS } from "@microsoft/agent-host-protocol";

import {
  AhpCore,
  AhpOperationError,
  type AhpSessionBinding,
  type CatalogueSnapshot,
  type ChatSnapshotEvent,
  type ConnectionEvent,
  type DomainActionEvent,
  type IncompatibilityEvent,
  type SessionSnapshotEvent,
} from "./ahp-core.js";
import { BridgeClient, BridgeRpcError } from "./bridge-client.js";
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

interface BridgeBinding {
  readonly generation: number;
  readonly endpoint_id: string;
  readonly host_instance_id?: string;
  readonly session_uri: string;
  readonly chat_uri?: string;
  readonly state: string;
  readonly last_server_sequence: number;
}

interface RegisterResult {
  readonly binding?: BridgeBinding;
}

interface AdapterCommand {
  readonly command_id: number;
  readonly command_key: string;
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

interface ActiveBinding {
  readonly generation: number;
  readonly endpointId: string;
  readonly hostInstanceId: string;
  readonly sessionUri: string;
  readonly chatUri: string;
  readonly binding: AhpSessionBinding;
  readonly normalizer: AhpEventNormalizer;
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

class AdapterRuntime {
  readonly #config: AdapterConfig;

  readonly #bridge: BridgeClient;

  readonly #adapterInstanceId = randomUUID();

  readonly #core: AhpCore;

  readonly #callbackQueue = new SerialQueue();

  readonly #events = new Map<string, PublishedEvent>();

  #binding: ActiveBinding | undefined;

  #pendingBinding:
    | {
        readonly generation: number;
        readonly endpointId: string;
        readonly hostInstanceId: string;
        readonly sessionUri: string;
        normalizer?: AhpEventNormalizer;
        lastServerSequence: number;
      }
    | undefined;

  #eventFlush: Promise<void> | undefined;

  readonly #readOnlyEndpoints = new Set<string>();

  #stopping = false;

  constructor(config: AdapterConfig) {
    this.#config = config;
    this.#bridge = new BridgeClient(
      config.bridgePipePath,
      config.bridgeToken,
    );
    this.#core = new AhpCore({
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
    });
  }

  async run(signal: AbortSignal): Promise<void> {
    const registration = await this.#bridge.call<RegisterResult>({
      operation: "ahp_adapter_register",
      registration: {
        adapter_id: this.#config.adapterId,
        adapter_instance_id: this.#adapterInstanceId,
        version: ADAPTER_VERSION,
        supported_protocols: [...SUPPORTED_PROTOCOL_VERSIONS],
      },
    });
    await this.#core.start();
    await this.#publishCatalogue(this.#core.catalogue);
    if (
      registration.binding &&
      (registration.binding.state === "binding" ||
        registration.binding.state === "bound")
    ) {
      await this.#activateBinding(registration.binding).catch((error) => {
        safeLog("warn", "Failed to restore AHP binding", {
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
    await this.#eventFlush?.catch(() => undefined);
    await this.#binding?.binding.close().catch(() => undefined);
    await this.#core.stop();
  }

  async #executeCommand(command: AdapterCommand): Promise<void> {
    if (
      !Number.isSafeInteger(command.command_id) ||
      command.command_id <= 0 ||
      !Number.isSafeInteger(command.binding_generation)
    ) {
      await this.#ack(command.command_id, "rejected", "invalid-command");
      return;
    }
    try {
      if (
        this.#binding &&
        this.#readOnlyEndpoints.has(this.#binding.endpointId) &&
        command.kind !== "bind_session" &&
        command.kind !== "unbind_session"
      ) {
        throw new AhpOperationError(
          "binding-unavailable",
          "AHP compatibility gate is read-only",
        );
      }
      switch (command.kind) {
        case "bind_session":
          await this.#activateBinding(parseBindingCommand(command));
          break;
        case "unbind_session":
          await this.#binding?.binding.close();
          this.#binding = undefined;
          this.#pendingBinding = undefined;
          this.#events.clear();
          break;
        case "send_message": {
          const data = requireRecord(command.data);
          const content = requireString(data.content, "content");
          await this.#requireBinding(command).queueUserText(
            content,
            command.command_id,
          );
          break;
        }
        case "cancel_turn":
          await this.#requireBinding(command).cancelActiveTurn(
            command.command_id,
          );
          break;
        case "approve_tool": {
          const data = requireRecord(command.data);
          await this.#requireBinding(command).reviewToolParameters(
            {
              requestId: requireString(
                data.approval_key,
                "approval_key",
              ),
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
          await this.#requireBinding(command).reviewToolResult(
            {
              requestId: requireString(
                data.approval_key,
                "approval_key",
              ),
              approved: requireBoolean(data.approved, "approved"),
            },
            command.command_id,
          );
          break;
        }
        case "complete_input": {
          const data = requireRecord(command.data);
          const binding = this.#requireBinding(command);
          const inputKey = requireString(data.input_key, "input_key");
          const answer = requireString(data.answer, "answer");
          await binding.completeCurrentInput(
            buildInputCompletion(binding, inputKey, answer),
            command.command_id,
          );
          break;
        }
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
        kind: command.kind,
        code: errorCode(error),
      });
    }
  }

  #requireBinding(command: AdapterCommand): AhpSessionBinding {
    const active = this.#binding;
    if (
      !active ||
      active.generation !== command.binding_generation
    ) {
      throw new AhpOperationError(
        "binding-unavailable",
        "Command targets a stale binding",
      );
    }
    return active.binding;
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
    if (this.#binding) {
      const current = this.#binding.binding.snapshot().defaultChat;
      if (
        current?.activeTurn ||
        (current?.queuedMessages && current.queuedMessages.length > 0)
      ) {
        throw new AhpOperationError(
          "binding-unavailable",
          "Cannot switch Session while a Turn or queued message is active",
        );
      }
      await this.#binding.binding.close();
      this.#binding = undefined;
    }
    this.#events.clear();
    this.#pendingBinding = {
      generation: binding.generation,
      endpointId: binding.endpoint_id,
      hostInstanceId: endpoint.endpoint.instanceId,
      sessionUri: binding.session_uri,
      lastServerSequence: 0,
    };
    try {
      const sessionBinding = await this.#core.bindSession(
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
      const normalizer =
        this.#pendingBinding.normalizer ??
        new AhpEventNormalizer({
          adapterId: this.#config.adapterId,
          endpointId: binding.endpoint_id,
          hostInstanceId: endpoint.endpoint.instanceId,
          generation: binding.generation,
          sessionUri: binding.session_uri,
          chatUri,
        });
      const active: ActiveBinding = {
        generation: binding.generation,
        endpointId: binding.endpoint_id,
        hostInstanceId: endpoint.endpoint.instanceId,
        sessionUri: binding.session_uri,
        chatUri,
        binding: sessionBinding,
        normalizer,
      };
      this.#binding = active;
      const lastServerSequence =
        this.#pendingBinding.lastServerSequence;
      await this.#bridge.call({
        operation: "ahp_binding_ready",
        adapter_id: this.#config.adapterId,
        adapter_instance_id: this.#adapterInstanceId,
        endpoint_id: active.endpointId,
        host_instance_id: active.hostInstanceId,
        binding_generation: active.generation,
        session_uri: active.sessionUri,
        chat_uri: active.chatUri,
        last_server_sequence: lastServerSequence,
      });
      this.#pendingBinding = undefined;
      await this.#flushEvents();
    } catch (error) {
      this.#pendingBinding = undefined;
      await this.#bridge
        .call({
          operation: "ahp_binding_failed",
          adapter_id: this.#config.adapterId,
          adapter_instance_id: this.#adapterInstanceId,
          binding_generation: binding.generation,
          reason_code: errorCode(error),
        })
        .catch(() => undefined);
      throw error;
    }
  }

  #onSessionSnapshot(event: SessionSnapshotEvent): void {
    const pending = this.#pendingBinding;
    if (
      pending &&
      event.endpointId === pending.endpointId &&
      event.sessionUri === pending.sessionUri &&
      event.state.defaultChat
    ) {
      pending.lastServerSequence = Math.max(
        pending.lastServerSequence,
        event.serverSeq,
      );
      pending.normalizer ??= new AhpEventNormalizer({
        adapterId: this.#config.adapterId,
        endpointId: pending.endpointId,
        hostInstanceId: pending.hostInstanceId,
        generation: pending.generation,
        sessionUri: pending.sessionUri,
        chatUri: event.state.defaultChat,
      });
      this.#queueEvents(pending.normalizer.sessionSnapshot(event));
      return;
    }
    const active = this.#binding;
    if (active) {
      this.#queueEvents(active.normalizer.sessionSnapshot(event));
    }
  }

  #onChatSnapshot(event: ChatSnapshotEvent): void {
    const pending = this.#pendingBinding;
    if (
      pending?.normalizer &&
      event.endpointId === pending.endpointId &&
      event.sessionUri === pending.sessionUri
    ) {
      pending.lastServerSequence = Math.max(
        pending.lastServerSequence,
        event.serverSeq,
      );
      this.#queueEvents(pending.normalizer.chatSnapshot(event));
      return;
    }
    const active = this.#binding;
    if (active) {
      this.#queueEvents(active.normalizer.chatSnapshot(event));
    }
  }

  #onAction(event: DomainActionEvent): void {
    const sequence = event.envelope.serverSeq;
    if (this.#pendingBinding) {
      this.#pendingBinding.lastServerSequence = Math.max(
        this.#pendingBinding.lastServerSequence,
        sequence,
      );
      if (this.#pendingBinding.normalizer) {
        this.#queueEvents(this.#pendingBinding.normalizer.action(event));
      }
      return;
    }
    if (event.envelope.rejectionReason !== undefined) {
      return;
    }
    const active = this.#binding;
    if (active) {
      this.#queueEvents(active.normalizer.action(event));
    }
  }

  #onConnection(event: ConnectionEvent): void {
    if (
      event.status === "connected" &&
      event.selectedProtocol &&
      SUPPORTED_PROTOCOL_VERSIONS.includes(event.selectedProtocol)
    ) {
      this.#readOnlyEndpoints.delete(event.endpoint.id);
    }
    if (
      this.#binding?.endpointId === event.endpoint.id &&
      event.status === "disconnected"
    ) {
      this.#queueEvents([
        this.#binding.normalizer.hostDisconnected(
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

  #queueEvents(events: readonly PublishedEvent[]): void {
    for (const event of events) {
      this.#events.set(event.event_id, event);
    }
    if (this.#binding) {
      void this.#flushEvents();
    }
  }

  #flushEvents(): Promise<void> {
    if (this.#eventFlush) {
      return this.#eventFlush;
    }
    this.#eventFlush = this.#flushEventsInner().finally(() => {
      this.#eventFlush = undefined;
      if (this.#events.size > 0 && !this.#stopping) {
        setTimeout(() => void this.#flushEvents(), RETRY_DELAY_MS).unref();
      }
    });
    return this.#eventFlush;
  }

  async #flushEventsInner(): Promise<void> {
    while (this.#binding && this.#events.size > 0) {
      const batch = [...this.#events.values()].slice(0, EVENT_BATCH_SIZE);
      await this.#bridge.call({
        operation: "ahp_publish_events",
        adapter_id: this.#config.adapterId,
        adapter_instance_id: this.#adapterInstanceId,
        binding_generation: this.#binding.generation,
        events: batch,
      });
      for (const event of batch) {
        this.#events.delete(event.event_id);
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

function parseBindingCommand(command: AdapterCommand): BridgeBinding {
  const data = requireRecord(command.data);
  return {
    generation: command.binding_generation,
    endpoint_id: requireString(data.endpoint_id, "endpoint_id"),
    host_instance_id: requireString(
      data.host_instance_id,
      "host_instance_id",
    ),
    session_uri: requireString(data.session_uri, "session_uri"),
    state: "binding",
    last_server_sequence: 0,
  };
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

void main().catch((error: unknown) => {
  safeLog("warn", "AHP Adapter stopped", { code: errorCode(error) });
  process.exitCode = 1;
});
