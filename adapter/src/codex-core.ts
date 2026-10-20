import { createHash, randomUUID } from "node:crypto";
import { realpath } from "node:fs/promises";
import { win32 } from "node:path";
import { pathToFileURL } from "node:url";

import {
  ActionType,
  ChatInputAnswerState,
  ChatInputAnswerValueKind,
  ChatInputQuestionKind,
  ChatInputResponseKind,
  ConfirmationOptionKind,
  MessageKind,
  PendingMessageKind,
  ResponsePartKind,
  SessionInputRequestKind,
  SessionLifecycle,
  SessionStatus,
  ToolCallConfirmationReason,
  ToolCallCancellationReason,
  ToolCallStatus,
  chatReducer,
  type ChatAction,
  type ChatState,
  type ChatInputRequest,
  type SessionInputRequest,
  type SessionState,
  type SessionSummary,
} from "@microsoft/agent-host-protocol";

import {
  AhpOperationError,
  type AhpCoreCallbacks,
  type AhpSessionBinding,
  type CatalogueSnapshot,
  type CompleteCurrentInputCommand,
  type ReviewToolParametersCommand,
  type QueueUserTextResult,
} from "./ahp-core.js";
import {
  record, startCodexClient, text,
  type CodexClient, type CodexMessage, type CodexRequestId,
} from "./codex-rpc.js";
import type { AuthorizedTargetConfig } from "./config.js";
import type { ProviderCore } from "./provider-core.js";

export const CODEX_PROTOCOL = "codex-app-server/0.163.0";

interface CodexThread {
  readonly id: string;
  readonly cwd: string;
  readonly title: string;
  readonly createdAt: string;
  readonly modifiedAt: string;
  readonly active: boolean;
}

interface CodexCoreOptions {
  readonly executable: string;
  readonly clientId: string;
  readonly authorizedTargets: () => readonly AuthorizedTargetConfig[];
  readonly callbacks: AhpCoreCallbacks;
  readonly createClient?: typeof startCodexClient;
  readonly pollIntervalMs?: number;
}

export class CodexCore implements ProviderCore {
  readonly #options: CodexCoreOptions;
  readonly #endpointId: string;
  readonly #bindings = new Map<string, CodexBinding>();
  readonly #closingClients = new Set<Promise<void>>();
  #threads = new Map<string, CodexThread>();
  #client: CodexClient | undefined;
  #instanceId = randomUUID();
  #revision = 0;
  #sequence = 0;
  #timer: ReturnType<typeof setInterval> | undefined;
  #refreshTask: Promise<CatalogueSnapshot> | undefined;
  #stopping = false;
  #messageQueue = Promise.resolve();

  constructor(options: CodexCoreOptions) {
    this.#options = options;
    this.#endpointId = `codex-local-${createHash("sha256")
      .update(win32.normalize(options.executable).toLowerCase()).digest("hex").slice(0, 24)}`;
  }

  get catalogue(): CatalogueSnapshot {
    return {
      revision: this.#revision,
      endpoints: [{
        endpoint: {
          id: this.#endpointId,
          instanceId: this.#instanceId,
          pid: this.#client?.pid ?? process.pid,
          advertisedProtocol: CODEX_PROTOCOL,
        },
        connection: this.#client ? "connected" : "disconnected",
        ...(this.#client ? { selectedProtocol: CODEX_PROTOCOL } : {}),
        sessions: this.#client ? [...this.#threads.values()].map((thread) => this.#summary(thread)) : [],
      }],
    };
  }

  async start(): Promise<CatalogueSnapshot> {
    this.#stopping = false;
    const catalogue = await this.refreshEndpoints();
    this.#timer = setInterval(() => {
      void this.refreshEndpoints();
    }, this.#options.pollIntervalMs ?? 15_000);
    return catalogue;
  }

  async stop(): Promise<void> {
    this.#stopping = true;
    clearInterval(this.#timer);
    await this.#refreshTask;
    const client = this.#client;
    this.#client = undefined;
    for (const binding of this.#bindings.values()) {
      binding.disconnected();
    }
    this.#bindings.clear();
    await client?.close();
    await this.#messageQueue;
    await Promise.all(this.#closingClients);
  }

  refreshEndpoints(): Promise<CatalogueSnapshot> {
    if (!this.#refreshTask) {
      this.#refreshTask = this.#refresh().finally(() => {
        this.#refreshTask = undefined;
      });
    }
    return this.#refreshTask;
  }

  async #refresh(): Promise<CatalogueSnapshot> {
    try {
      if (!this.#client && !this.#stopping) {
        await Promise.all(this.#closingClients);
        if (this.#stopping) {
          return this.catalogue;
        }
        this.#instanceId = randomUUID();
        const instanceId = this.#instanceId;
        this.#client = await (this.#options.createClient ?? startCodexClient)(
          this.#options.executable,
          (message) => {
            this.#messageQueue = this.#messageQueue
              .then(() => {
                if (this.#instanceId === instanceId && !this.#stopping) {
                  return this.#message(message);
                }
              })
              .catch(() => {
                if (this.#instanceId === instanceId) {
                  this.#disconnect("codex-invalid-event");
                }
              });
          },
          () => {
            if (this.#instanceId === instanceId) {
              this.#disconnect("codex-disconnected");
            }
          },
        );
        await this.#client.request("initialize", {
          clientInfo: { name: "qq_copilot", title: "QQ Copilot Codex", version: "0.1.0" },
          capabilities: { experimentalApi: true },
        });
        await this.#client.notify("initialized");
        this.#options.callbacks.onConnection?.({
          endpoint: this.catalogue.endpoints[0]!.endpoint,
          status: "connected",
          selectedProtocol: CODEX_PROTOCOL,
        });
      }
      const client = this.#client;
      if (!client || this.#stopping) {
        return this.catalogue;
      }
      const threads = new Map<string, CodexThread>();
      const cursors = new Set<string>();
      let cursor: string | undefined;
      for (let page = 0; ; page++) {
        if (page >= 100) {
          throw new Error("codex-thread-pagination-limit");
        }
        const result = record(await client.request("thread/list", {
          limit: 100, sortKey: "updated_at", archived: false,
          ...(cursor ? { cursor } : {}),
        }));
        if (!Array.isArray(result.data)) {
          throw new Error("codex-invalid-thread-list");
        }
        for (const raw of result.data) {
          const thread = parseThread(raw);
          if (await this.#authorized(thread.cwd)) {
            threads.set(sessionUri(thread.id), thread);
            if (threads.size > 1_000) {
              throw new Error("codex-thread-catalogue-limit");
            }
          }
        }
        if (result.nextCursor === null || result.nextCursor === undefined) {
          break;
        }
        cursor = text(result.nextCursor);
        if (cursors.has(cursor)) {
          throw new Error("codex-repeated-thread-cursor");
        }
        cursors.add(cursor);
      }
      this.#threads = threads;
      this.#publishCatalogue();
    } catch (error) {
      this.#disconnect(error instanceof Error && /^codex-[a-z0-9:.-]+$/u.test(error.message)
        ? error.message : "codex-discovery-failed");
    }
    return this.catalogue;
  }

  async bindSession(endpointId: string, uri: string): Promise<AhpSessionBinding> {
    const client = this.#client;
    const known = this.#threads.get(uri);
    if (endpointId !== this.#endpointId || !client || !known ||
        !(await this.#authorized(known.cwd))) {
      throw new AhpOperationError("session-not-found", "Codex thread is not an authorized local session");
    }
    const existing = this.#bindings.get(uri);
    if (existing) {
      return existing;
    }
    const read = record(await client.request("thread/read", { threadId: known.id, includeTurns: false }));
    const current = parseThread(read.thread);
    if (current.id !== known.id || !(await this.#authorized(current.cwd))) {
      throw new AhpOperationError("session-not-found", "Codex thread workspace changed");
    }
    if (current.active) {
      throw new AhpOperationError("binding-unavailable", "Finish the active PC Codex turn before resuming from QQ");
    }
    // Never override the thread's sandbox, model, approval policy, or credentials.
    const result = record(await client.request("thread/resume", {
      threadId: known.id, excludeTurns: true,
    }));
    const resumed = parseThread(result.thread);
    if (resumed.id !== known.id || !(await this.#authorized(resumed.cwd)) ||
        localPathKey(text(result.cwd)) !== localPathKey(resumed.cwd) || resumed.active) {
      await client.request("thread/unsubscribe", { threadId: known.id });
      throw new AhpOperationError("binding-unavailable", "Codex resume returned unexpected thread state");
    }
    const binding = new CodexBinding({
      endpointId, thread: resumed, client,
      clientId: this.#options.clientId,
      callbacks: this.#options.callbacks,
      nextSequence: () => ++this.#sequence,
      authorize: () => this.#authorized(resumed.cwd),
      closed: () => this.#bindings.delete(uri),
      reportError: () => this.#options.callbacks.onError?.({
        operation: "chat-stream", code: "codex-operation-failed",
        message: "Codex operation failed; check Codex on the PC", endpointId, sessionUri: uri,
      }),
    });
    this.#bindings.set(uri, binding);
    binding.publish();
    return binding;
  }

  async #authorized(cwd: string): Promise<boolean> {
    if (!win32.isAbsolute(cwd) || cwd.includes("\0")) {
      return false;
    }
    const normalized = localPathKey(cwd);
    const targets = this.#options.authorizedTargets().filter((target) => target.kind === "local");
    if (!targets.some((target) =>
      localPathKey(target.path) === normalized)) {
      return false;
    }
    const canonical = await realpath(cwd);
    return localPathKey(canonical) === normalized;
  }

  async #message(message: CodexMessage): Promise<void> {
    const params = record(message.params ?? {});
    const binding = typeof params.threadId === "string"
      ? this.#bindings.get(sessionUri(params.threadId)) : undefined;
    if (binding) {
      await binding.receive(message);
    } else if (message.id !== undefined) {
      await this.#client?.reject(message.id, "No QQ binding owns this request; use Codex on the PC");
    }
  }

  #disconnect(code: string): void {
    const client = this.#client;
    this.#client = undefined;
    for (const binding of this.#bindings.values()) {
      binding.disconnected();
    }
    this.#bindings.clear();
    this.#threads.clear();
    if (!this.#stopping) {
      this.#options.callbacks.onError?.({
        operation: "connect", code, message: "Codex is unavailable; check the configured executable and PC login",
        endpointId: this.#endpointId,
      });
      this.#options.callbacks.onConnection?.({
        endpoint: this.catalogue.endpoints[0]!.endpoint, status: "disconnected",
      });
      this.#publishCatalogue();
    }
    if (client) {
      const closing = client.close().catch(() => this.#options.callbacks.onError?.({
        operation: "connect", code: "codex-close-failed", message: "Codex process cleanup failed",
        endpointId: this.#endpointId,
      })).finally(() => this.#closingClients.delete(closing));
      this.#closingClients.add(closing);
    }
  }

  #publishCatalogue(): void {
    this.#revision++;
    this.#options.callbacks.onCatalogue?.(this.catalogue);
  }

  #summary(thread: CodexThread): SessionSummary {
    return {
      resource: sessionUri(thread.id), provider: "codex", title: thread.title,
      status: thread.active ? SessionStatus.InProgress : SessionStatus.Idle,
      workingDirectories: [pathToFileURL(thread.cwd).href],
      createdAt: thread.createdAt, modifiedAt: thread.modifiedAt,
    };
  }
}

interface BindingOptions {
  readonly endpointId: string;
  readonly thread: CodexThread;
  readonly client: CodexClient;
  readonly clientId: string;
  readonly callbacks: AhpCoreCallbacks;
  readonly nextSequence: () => number;
  readonly authorize: () => Promise<boolean>;
  readonly closed: () => void;
  readonly reportError: () => void;
}

interface PendingRequest {
  readonly rpcId: CodexRequestId;
  readonly turnId: string;
  readonly itemId: string;
  readonly kind: "approval" | "input";
}

class CodexBinding implements AhpSessionBinding {
  readonly endpointId: string;
  readonly sessionUri: string;
  readonly provider = "codex";
  readonly #options: BindingOptions;
  readonly #chatUri: string;
  readonly #requests = new Map<string, PendingRequest>();
  readonly #tools = new Set<string>();
  readonly #parts = new Set<string>();
  readonly #completedTools = new Set<string>();
  readonly #finishedTurns = new Set<string>();
  readonly #commandResults = new Map<number, QueueUserTextResult>();
  #chat: ChatState;
  #inputs: SessionInputRequest[] = [];
  #closed = false;
  #startingText: string | undefined;
  #starting = false;
  #queue = Promise.resolve();

  constructor(options: BindingOptions) {
    this.#options = options;
    this.endpointId = options.endpointId;
    this.sessionUri = sessionUri(options.thread.id);
    this.#chatUri = `${this.sessionUri}/chat`;
    this.#chat = {
      resource: this.#chatUri, title: options.thread.title,
      status: SessionStatus.Idle, modifiedAt: options.thread.modifiedAt, turns: [],
    };
  }

  snapshot() {
    return {
      endpointId: this.endpointId, sessionUri: this.sessionUri, provider: this.provider,
      session: this.#session(), defaultChat: structuredClone(this.#chat),
    };
  }

  publish(completedAssistantPart?: { turnId: string; partId: string; finalResponse: boolean }): void {
    if (this.#closed) {
      return;
    }
    const serverSeq = this.#options.nextSequence();
    const common = { endpointId: this.endpointId, sessionUri: this.sessionUri, provider: this.provider, serverSeq };
    this.#options.callbacks.onSessionSnapshot?.({ ...common, state: this.#session() });
    this.#options.callbacks.onChatSnapshot?.({
      ...common, chatUri: this.#chatUri, state: structuredClone(this.#chat),
      ...(completedAssistantPart ? { completedAssistantPart } : {}),
    });
  }

  async queueUserText(content: string, clientSeq = 0) {
    return this.#serialized(async () => {
      await this.#assertAuthorized();
      const cached = clientSeq > 0 ? this.#commandResults.get(clientSeq) : undefined;
      if (cached) {
        return cached;
      }
      if (!content.trim()) {
        throw new AhpOperationError("invalid-command", "Codex message is empty");
      }
      const id = randomUUID();
      const busy = this.#starting || this.#chat.activeTurn !== undefined;
      if (busy) {
        if ((this.#chat.queuedMessages?.length ?? 0) >= 100) {
          throw new AhpOperationError("invalid-command", "Codex message queue is full");
        }
        this.#action({
          type: ActionType.ChatPendingMessageSet, kind: PendingMessageKind.Queued,
          id, message: { text: content, origin: { kind: MessageKind.User } },
        });
      } else {
        await this.#startTurn(content);
      }
      const result = { disposition: busy ? "queued" as const : "started" as const, id, clientSeq };
      if (clientSeq > 0) {
        this.#commandResults.set(clientSeq, result);
      }
      return result;
    });
  }

  async cancelActiveTurn(clientSeq = 0, expectedTurnId?: string) {
    await this.#assertAuthorized();
    const turnId = this.#chat.activeTurn?.id;
    if (!turnId) {
      throw new AhpOperationError("no-active-turn", "Codex has no active turn");
    }
    if (expectedTurnId !== undefined && expectedTurnId !== turnId) {
      throw new AhpOperationError("stale-command", "Codex active turn changed");
    }
    await this.#options.client.request("turn/interrupt", { threadId: this.#options.thread.id, turnId });
    return { turnId, clientSeq };
  }

  async reviewToolParameters(command: ReviewToolParametersCommand, clientSeq = 0) {
    await this.#assertAuthorized();
    const pending = this.#requests.get(command.requestId);
    if (!pending || pending.kind !== "approval" || pending.turnId !== this.#chat.activeTurn?.id) {
      throw new AhpOperationError("pending-tool-not-found", "Codex approval is no longer pending");
    }
    if ((command.decision === "approve" && command.editedToolInput !== undefined) ||
        command.selectedOptionId !== undefined) {
      throw new AhpOperationError("invalid-confirmation-option", "Codex supports only approve once or deny");
    }
    await this.#options.client.respond(pending.rpcId, {
      decision: command.decision === "approve" ? "accept" : "decline",
    });
    this.#requests.delete(command.requestId);
    this.#inputs = this.#inputs.filter((input) => input.id !== command.requestId);
    this.#action(command.decision === "approve"
      ? {
          type: ActionType.ChatToolCallConfirmed, turnId: pending.turnId,
          toolCallId: pending.itemId, approved: true, confirmed: ToolCallConfirmationReason.UserAction,
        }
      : {
          type: ActionType.ChatToolCallConfirmed, turnId: pending.turnId,
          toolCallId: pending.itemId, approved: false, reason: ToolCallCancellationReason.Denied,
        }, true, clientSeq);
    return { clientSeq };
  }

  async reviewToolResult(): Promise<{ clientSeq: number }> {
    throw new AhpOperationError("pending-tool-not-found", "Codex does not expose result confirmations");
  }

  async completeCurrentInput(command: CompleteCurrentInputCommand, clientSeq = 0) {
    await this.#assertAuthorized();
    const key = command.requestId;
    const pending = key ? this.#requests.get(key) : undefined;
    const input = this.#inputs.find((candidate) => candidate.id === key);
    if (!pending || pending.kind !== "input" ||
        input?.kind !== SessionInputRequestKind.ChatInput ||
        pending.turnId !== this.#chat.activeTurn?.id) {
      throw new AhpOperationError("pending-input-not-found", "Codex question is no longer pending");
    }
    if (command.response !== ChatInputResponseKind.Accept) {
      throw new AhpOperationError("invalid-command", "Answer the Codex question or cancel the turn");
    }
    const answers: Record<string, { answers: string[] }> = {};
    for (const question of input.request.questions ?? []) {
      const answer = command.answers?.[question.id];
      if (answer?.state !== ChatInputAnswerState.Submitted ||
          (answer.value.kind !== ChatInputAnswerValueKind.Text &&
           answer.value.kind !== ChatInputAnswerValueKind.Selected)) {
        throw new AhpOperationError("invalid-command", "Codex expects a text or selected answer");
      }
      answers[question.id] = { answers: [answer.value.value] };
    }
    await this.#options.client.respond(pending.rpcId, { answers });
    this.#requests.delete(key!);
    this.#inputs = this.#inputs.filter((candidate) => candidate.id !== key);
    this.#action({
      type: ActionType.ChatInputCompleted, requestId: key!,
      response: command.response, ...(command.answers ? { answers: command.answers } : {}),
    }, true, clientSeq);
    return { clientSeq };
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    if (this.#chat.activeTurn || this.#starting || this.#requests.size > 0 ||
        (this.#chat.queuedMessages?.length ?? 0) > 0) {
      throw new AhpOperationError("binding-unavailable", "Cannot detach a busy Codex thread");
    }
    await this.#options.client.request("thread/unsubscribe", { threadId: this.#options.thread.id });
    this.#closed = true;
    this.#options.closed();
  }

  disconnected(): void {
    this.#closed = true;
    this.#requests.clear();
    this.#inputs = [];
    this.#commandResults.clear();
  }

  async receive(message: CodexMessage): Promise<void> {
    if (this.#closed) {
      return;
    }
    const params = record(message.params);
    if (message.id !== undefined) {
      await this.#serverRequest(message, params);
      return;
    }
    switch (message.method) {
      case "turn/started":
        this.#turnStarted(record(params.turn));
        break;
      case "item/started":
      case "item/completed":
        this.#item(text(params.turnId), record(params.item), message.method === "item/completed");
        break;
      case "turn/completed": {
        const turn = record(params.turn);
        const turnId = text(turn.id);
        if (this.#chat.activeTurn?.id !== turnId) {
          return;
        }
        if (Array.isArray(turn.items)) {
          for (const item of turn.items) {
            this.#item(turnId, record(item), true);
          }
        }
        this.#finishedTurns.add(turnId);
        this.#inputs = [];
        this.#requests.clear();
        if (turn.status === "completed") {
          this.#action({ type: ActionType.ChatTurnComplete, turnId, duration: duration(turn) });
        } else if (turn.status === "interrupted") {
          this.#action({ type: ActionType.ChatTurnCancelled, turnId, duration: duration(turn) });
        } else {
          this.#action({
            type: ActionType.ChatError, turnId, duration: duration(turn),
            part: {
              kind: ResponsePartKind.Error,
              error: { errorType: "codex", message: "Codex Turn failed. Check Codex on the PC." },
            },
          });
        }
        if (turn.status === "completed") {
          const queued = this.#chat.queuedMessages?.[0];
          if (queued) {
            void this.#serialized(async () => {
              await this.#assertAuthorized();
              if (this.#chat.activeTurn) {
                return;
              }
              this.#action({ type: ActionType.ChatPendingMessageRemoved, kind: PendingMessageKind.Queued, id: queued.id });
              await this.#startTurn(queued.message.text);
            }).catch(() => this.#queueFailed());
          }
        } else {
          this.#clearQueue();
        }
        break;
      }
      case "serverRequest/resolved": {
        const key = requestKey(params.requestId);
        const pending = this.#requests.get(key);
        if (pending) {
          this.#requests.delete(key);
          this.#inputs = this.#inputs.filter((input) => input.id !== key);
          if (pending.kind === "input") {
            this.#action({
              type: ActionType.ChatInputCompleted, requestId: key,
              response: ChatInputResponseKind.Cancel,
            });
          } else {
            this.#options.reportError();
            this.publish();
          }
        }
        break;
      }
    }
  }

  async #startTurn(content: string): Promise<void> {
    await this.#assertAuthorized();
    this.#starting = true;
    this.#startingText = content;
    try {
      const result = record(await this.#options.client.request("turn/start", {
        threadId: this.#options.thread.id,
        input: [{ type: "text", text: content, text_elements: [] }],
      }));
      this.#turnStarted(record(result.turn));
    } finally {
      this.#starting = false;
      this.#startingText = undefined;
    }
  }

  #turnStarted(turn: Record<string, unknown>): void {
    const turnId = text(turn.id);
    if (this.#chat.activeTurn?.id === turnId || this.#finishedTurns.has(turnId)) {
      return;
    }
    if (this.#chat.activeTurn) {
      throw new Error("codex-concurrent-turn");
    }
    this.#tools.clear();
    this.#completedTools.clear();
    this.#parts.clear();
    this.#action({
      type: ActionType.ChatTurnStarted, turnId,
      startedAt: timestamp(turn.startedAt ?? Date.now() / 1000),
      message: { text: this.#startingText ?? "", origin: { kind: MessageKind.User } },
    });
  }

  #item(turnId: string, item: Record<string, unknown>, completed: boolean): void {
    if (this.#chat.activeTurn?.id !== turnId) {
      return;
    }
    const id = text(item.id);
    const kind = text(item.type);
    if (kind === "agentMessage") {
      // Only closed Codex items are publishable; deltas never become duplicate replies.
      if (completed && !this.#parts.has(id)) {
        this.#parts.add(id);
        this.#action({
          type: ActionType.ChatResponsePart, turnId,
          part: { kind: ResponsePartKind.Markdown, id, content: typeof item.text === "string" ? item.text : "" },
        }, false);
        this.publish(item.phase === "commentary" || item.phase === "final_answer"
          ? { turnId, partId: id, finalResponse: item.phase === "final_answer" }
          : undefined);
      }
    } else if (["commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall"].includes(kind)) {
      if (this.#completedTools.has(id)) {
        return;
      }
      this.#ensureTool(turnId, id, kind);
      const pending = [...this.#requests.values()].some((request) =>
        request.kind === "approval" && request.itemId === id);
      if (!pending) {
        this.#action({
          type: ActionType.ChatToolCallReady, turnId, toolCallId: id,
          invocationMessage: toolDescription(item), toolInput: toolInput(item),
          confirmed: ToolCallConfirmationReason.Setting,
        });
      }
      if (completed) {
        this.#completedTools.add(id);
        this.#action({
          type: ActionType.ChatToolCallComplete, turnId, toolCallId: id,
          result: {
            success: item.status !== "failed" && item.status !== "declined" && item.success !== false,
            pastTenseMessage: toolDescription(item),
          },
        });
      }
    }
  }

  async #serverRequest(message: CodexMessage, params: Record<string, unknown>): Promise<void> {
    const rpcId = message.id!;
    if (![
      "item/commandExecution/requestApproval", "item/fileChange/requestApproval",
      "item/tool/requestUserInput",
    ].includes(message.method)) {
      await this.#options.client.reject(rpcId, "Unsupported Codex request; use Codex on the PC");
      this.#options.reportError();
      return;
    }
    if (!(await this.#options.authorize())) {
      await this.#options.client.reject(rpcId, "Workspace is not authorized");
      return;
    }
    if (this.#closed) {
      return;
    }
    const turnId = text(params.turnId);
    if (turnId !== this.#chat.activeTurn?.id) {
      await this.#options.client.reject(rpcId, "Codex turn is not owned by this binding");
      return;
    }
    const key = requestKey(rpcId);
    const itemId = text(params.itemId);
    if (message.method === "item/commandExecution/requestApproval" ||
        message.method === "item/fileChange/requestApproval") {
      if ([...this.#requests.values()].some((pending) => pending.itemId === itemId)) {
        await this.#options.client.reject(rpcId, "Concurrent approval for this tool is unsupported; use Codex on the PC");
        this.#options.reportError();
        return;
      }
      this.#ensureTool(turnId, itemId, message.method.includes("commandExecution") ? "commandExecution" : "fileChange");
      const description = toolDescription(params);
      this.#action({
        type: ActionType.ChatToolCallReady, turnId, toolCallId: itemId,
        invocationMessage: description, toolInput: toolInput(params),
      }, false);
      this.#requests.set(key, { rpcId, turnId, itemId, kind: "approval" });
      this.#inputs.push({
        id: key, kind: SessionInputRequestKind.ToolConfirmation,
        chat: this.#chatUri, turnId,
        toolCall: {
          status: ToolCallStatus.PendingConfirmation, toolCallId: itemId,
          toolName: "codex", displayName: "Codex", invocationMessage: description,
          toolInput: toolInput(params), editable: false,
          options: [
            { id: "approve", label: "Approve once", kind: ConfirmationOptionKind.Approve },
            { id: "deny", label: "Deny", kind: ConfirmationOptionKind.Deny },
          ],
        },
      });
      this.publish();
    } else if (message.method === "item/tool/requestUserInput") {
      if (!Array.isArray(params.questions) || params.questions.length !== 1) {
        await this.#options.client.reject(rpcId, "QQ supports a single non-secret question; answer on PC");
        this.#options.reportError();
        return;
      }
      const question = record(params.questions[0]);
      if (question.isSecret !== false) {
        await this.#options.client.reject(rpcId, "Secret input is PC-only");
        this.#options.reportError();
        return;
      }
      const questionId = text(question.id);
      const options = Array.isArray(question.options)
        ? question.options.map((option) => {
            const value = record(option);
            return { id: text(value.label), label: text(value.label) };
          }) : [];
      const prompt = text(question.question);
      const request: ChatInputRequest = {
        id: key, message: prompt,
        questions: [options.length > 0
          ? {
              id: questionId, kind: ChatInputQuestionKind.SingleSelect, message: prompt,
              required: true, options, allowFreeformInput: question.isOther === true,
            }
          : { id: questionId, kind: ChatInputQuestionKind.Text, message: prompt, required: true }],
      };
      this.#requests.set(key, { rpcId, turnId, itemId, kind: "input" });
      this.#inputs.push({ id: key, kind: SessionInputRequestKind.ChatInput, chat: this.#chatUri, request });
      this.#action({ type: ActionType.ChatInputRequested, request });
    }
  }

  #ensureTool(turnId: string, itemId: string, kind: string): void {
    if (!this.#tools.has(itemId)) {
      this.#tools.add(itemId);
      this.#action({
        type: ActionType.ChatToolCallStart, turnId, toolCallId: itemId,
        toolName: kind, displayName: `Codex ${kind}`,
      });
    }
  }

  #action(action: ChatAction, publish = true, clientSeq?: number): void {
    if (this.#closed) {
      return;
    }
    this.#chat = chatReducer(this.#chat, action);
    this.#options.callbacks.onAction?.({
      scope: "chat", endpointId: this.endpointId, sessionUri: this.sessionUri,
      provider: this.provider, chatUri: this.#chatUri,
      envelope: {
        channel: this.#chatUri, serverSeq: this.#options.nextSequence(),
        origin: clientSeq === undefined ? undefined : { clientId: this.#options.clientId, clientSeq },
        action,
      },
    });
    if (publish) {
      this.publish();
    }
  }

  #clearQueue(): void {
    for (const queued of this.#chat.queuedMessages ?? []) {
      this.#action({ type: ActionType.ChatPendingMessageRemoved, kind: PendingMessageKind.Queued, id: queued.id });
    }
  }

  #queueFailed(): void {
    this.#clearQueue();
    this.#options.reportError();
  }

  #session(): SessionState {
    return {
      provider: this.provider, title: this.#options.thread.title,
      status: this.#inputs.length > 0 ? this.#chat.status | SessionStatus.InputNeeded : this.#chat.status,
      lifecycle: SessionLifecycle.Ready, activeClients: [],
      workingDirectories: [pathToFileURL(this.#options.thread.cwd).href],
      chats: [{
        resource: this.#chatUri, title: this.#chat.title, status: this.#chat.status,
        modifiedAt: this.#chat.modifiedAt,
      }],
      defaultChat: this.#chatUri, inputNeeded: structuredClone(this.#inputs),
    };
  }

  async #assertAuthorized(): Promise<void> {
    if (!(await this.#options.authorize()) || this.#closed) {
      throw new AhpOperationError("binding-unavailable", "Codex binding is closed or workspace is not trusted");
    }
  }

  #serialized<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#queue.then(operation);
    this.#queue = result.then(() => undefined, () => undefined);
    return result;
  }
}

function localPathKey(path: string): string {
  return win32.normalize(path).replace(/\\+$/u, "").toLowerCase();
}

function parseThread(value: unknown): CodexThread {
  const thread = record(value);
  return {
    id: text(thread.id), cwd: text(thread.cwd),
    title: `[Codex] ${(typeof thread.name === "string" && thread.name.trim()
      ? thread.name : typeof thread.preview === "string" ? thread.preview : text(thread.id)).slice(0, 240)}`,
    createdAt: timestamp(thread.createdAt), modifiedAt: timestamp(thread.updatedAt),
    active: record(thread.status).type === "active",
  };
}

function sessionUri(id: string): string {
  return `codex-thread://local/${encodeURIComponent(id)}`;
}

function requestKey(id: unknown): string {
  if (typeof id !== "string" && typeof id !== "number") {
    throw new Error("codex-invalid-request-id");
  }
  return `codex-request:${typeof id}:${String(id)}`;
}

function timestamp(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error("codex-invalid-timestamp");
  }
  return new Date(value * 1000).toISOString();
}

function duration(turn: Record<string, unknown>): number {
  return typeof turn.durationMs === "number" && turn.durationMs >= 0 ? turn.durationMs : 0;
}

function toolDescription(item: Record<string, unknown>): string {
  return (typeof item.command === "string" ? item.command :
    typeof item.reason === "string" ? item.reason :
    typeof item.tool === "string" ? item.tool : "Codex file change").slice(0, 16_000);
}

function toolInput(item: Record<string, unknown>): string {
  const serialized = JSON.stringify({
    command: item.command, cwd: item.cwd, reason: item.reason,
    changes: item.changes, tool: item.tool, arguments: item.arguments,
    grantRoot: item.grantRoot,
  });
  return serialized.length > 32_000 ? `${serialized.slice(0, 32_000)} [truncated]` : serialized;
}
