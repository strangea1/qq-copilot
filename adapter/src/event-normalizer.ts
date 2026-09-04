import { createHash } from "node:crypto";

import {
  ActionType,
  ChatInputQuestionKind,
  MessageKind,
  ResponsePartKind,
  SessionInputRequestKind,
  ToolCallStatus,
  type ChatInputQuestion,
  type ChatState,
  type ResponsePart,
  type SessionInputRequest,
  type SessionState,
  type StringOrMarkdown,
  type ToolCallState,
  type Turn,
} from "@microsoft/agent-host-protocol";

import {
  type ChatSnapshotEvent,
  type DomainActionEvent,
  type SessionSnapshotEvent,
} from "./ahp-core.js";

export type PublishedEventKind =
  | "session_snapshot"
  | "chat_snapshot"
  | "user_message"
  | "assistant_message"
  | "tool_status"
  | "approval_pending"
  | "approval_resolved"
  | "input_pending"
  | "input_resolved"
  | "turn_started"
  | "turn_completed"
  | "turn_cancelled"
  | "turn_failed"
  | "host_disconnected";

export interface PublishedEvent {
  readonly event_id: string;
  readonly host_instance_id: string;
  readonly server_sequence?: number;
  readonly session_uri: string;
  readonly chat_uri?: string;
  readonly turn_id?: string;
  readonly kind: PublishedEventKind;
  readonly origin_client_id?: string;
  readonly occurred_at: string;
  readonly data: unknown;
}

export interface NormalizerBinding {
  readonly adapterId: string;
  readonly endpointId: string;
  readonly hostInstanceId: string;
  readonly generation: number;
  readonly sessionUri: string;
  readonly chatUri: string;
}

interface OpenAssistantPart {
  readonly partId: string;
  readonly chunks: string[];
}

export class AhpEventNormalizer {
  readonly #binding: NormalizerBinding;

  readonly #seenUserTurns = new Set<string>();

  readonly #seenAssistantParts = new Set<string>();

  readonly #openAssistantParts = new Map<string, OpenAssistantPart>();

  readonly #seenStartedTurns = new Set<string>();

  readonly #seenFailedTurns = new Set<string>();

  readonly #toolStatuses = new Map<string, string>();

  readonly #approvalKeyByTool = new Map<string, string>();

  readonly #inputKeyByRequest = new Map<string, string>();

  readonly #inputTurnByKey = new Map<string, string>();

  readonly #pendingInputKeys = new Set<string>();

  #initialChatSnapshot = true;

  #activeTurnId: string | undefined;

  #redundantChatSnapshotSequence: number | undefined;

  constructor(binding: NormalizerBinding) {
    this.#binding = binding;
  }

  sessionSnapshot(event: SessionSnapshotEvent): PublishedEvent[] {
    if (!this.#matchesSession(event.endpointId, event.sessionUri)) {
      return [];
    }
    return [
      this.#event(
        "session_snapshot",
        event.serverSeq,
        undefined,
        undefined,
        {
          provider: event.provider,
          title: event.state.title,
          status: event.state.status,
          working_directories: event.state.workingDirectories ?? [],
        },
      ),
      ...this.#inputEvents(event.state, event.serverSeq),
    ];
  }

  chatSnapshot(event: ChatSnapshotEvent): PublishedEvent[] {
    if (
      !this.#matchesSession(event.endpointId, event.sessionUri) ||
      event.chatUri !== this.#binding.chatUri
    ) {
      return [];
    }
    if (this.#redundantChatSnapshotSequence === event.serverSeq) {
      this.#redundantChatSnapshotSequence = undefined;
      return [];
    }
    this.#redundantChatSnapshotSequence = undefined;
    const historical = this.#initialChatSnapshot;
    const events: PublishedEvent[] = [
      this.#event(
        "chat_snapshot",
        event.serverSeq,
        event.chatUri,
        event.state.activeTurn?.id,
        {
          title: event.state.title,
          status: event.state.status,
          activity: event.state.activity ?? null,
          active_turn_id: event.state.activeTurn?.id ?? null,
          queued_message_count: event.state.queuedMessages?.length ?? 0,
        },
      ),
    ];
    this.#activeTurnId = event.state.activeTurn?.id;
    if (
      event.state.activeTurn &&
      !this.#seenStartedTurns.has(event.state.activeTurn.id)
    ) {
      this.#seenStartedTurns.add(event.state.activeTurn.id);
      events.push(
        this.#event(
          "turn_started",
          event.serverSeq,
          event.chatUri,
          event.state.activeTurn.id,
          {},
          undefined,
          event.state.activeTurn.startedAt,
        ),
      );
    }
    for (const turn of event.state.turns) {
      events.push(
        ...this.#completedTurnEvents(
          turn,
          event.serverSeq,
          event.chatUri,
          historical,
        ),
      );
    }
    if (event.state.activeTurn) {
      events.push(
        ...this.#userMessageEvents(
          event.state.activeTurn.id,
          event.state.activeTurn.startedAt,
          event.state.activeTurn.message,
          event.serverSeq,
          event.chatUri,
          historical,
          undefined,
        ),
        ...this.#activeTurnAssistantEvents(
          event.state.activeTurn,
          event.serverSeq,
          event.chatUri,
          historical,
        ),
      );
    }
    events.push(
      ...this.#toolEvents(event.state, event.serverSeq, event.chatUri, historical),
    );
    this.#initialChatSnapshot = false;
    return events;
  }

  action(event: DomainActionEvent): PublishedEvent[] {
    if (
      event.scope !== "chat" ||
      !this.#matchesSession(event.endpointId, event.sessionUri) ||
      event.chatUri !== this.#binding.chatUri
    ) {
      return [];
    }
    const action = event.envelope.action;
    const sequence = event.envelope.serverSeq;
    const originClientId = event.envelope.origin?.clientId;
    switch (action.type) {
      case ActionType.ChatTurnStarted:
        this.#activeTurnId = action.turnId;
        this.#openAssistantParts.delete(action.turnId);
        this.#seenStartedTurns.add(action.turnId);
        return [
          this.#event(
            "turn_started",
            sequence,
            event.chatUri,
            action.turnId,
            {},
            originClientId,
            action.startedAt,
          ),
          ...this.#userMessageEvents(
            action.turnId,
            action.startedAt,
            action.message,
            sequence,
            event.chatUri,
            false,
            originClientId,
          ),
        ];
      case ActionType.ChatDelta: {
        const openPart = this.#openAssistantParts.get(action.turnId);
        if (
          openPart?.partId === action.partId &&
          !this.#seenAssistantParts.has(
            assistantPartKey(action.turnId, action.partId),
          )
        ) {
          openPart.chunks.push(action.content);
          this.#redundantChatSnapshotSequence = sequence;
        }
        return [];
      }
      case ActionType.ChatResponsePart: {
        const events = this.#flushAssistantPart(
          action.turnId,
          sequence,
          event.chatUri,
          false,
        );
        if (
          action.part.kind === ResponsePartKind.Markdown &&
          !this.#seenAssistantParts.has(
            assistantPartKey(action.turnId, action.part.id),
          )
        ) {
          this.#openAssistantParts.set(action.turnId, {
            partId: action.part.id,
            chunks: [action.part.content],
          });
        }
        return events;
      }
      case ActionType.ChatToolCallStart:
        return this.#flushAssistantPart(
          action.turnId,
          sequence,
          event.chatUri,
          false,
        );
      case ActionType.ChatInputRequested:
        return this.#activeTurnId
          ? this.#flushAssistantPart(
              this.#activeTurnId,
              sequence,
              event.chatUri,
              false,
            )
          : [];
      case ActionType.ChatToolCallConfirmed: {
        const approvalKey = this.#approvalKeyByTool.get(
          approvalMapKey("parameter", action.turnId, action.toolCallId),
        );
        return approvalKey
          ? [
              this.#event(
                "approval_resolved",
                sequence,
                event.chatUri,
                action.turnId,
                {
                  approval_key: approvalKey,
                  approved: action.approved,
                  client_id: originClientId ?? null,
                },
                originClientId,
              ),
            ]
          : [];
      }
      case ActionType.ChatToolCallResultConfirmed: {
        const approvalKey = this.#approvalKeyByTool.get(
          approvalMapKey("result", action.turnId, action.toolCallId),
        );
        return approvalKey
          ? [
              this.#event(
                "approval_resolved",
                sequence,
                event.chatUri,
                action.turnId,
                {
                  approval_key: approvalKey,
                  approved: action.approved,
                  client_id: originClientId ?? null,
                },
                originClientId,
              ),
            ]
          : [];
      }
      case ActionType.ChatInputCompleted: {
        const inputKey = this.#inputKeyByRequest.get(action.requestId);
        const turnId = inputKey
          ? this.#inputTurnByKey.get(inputKey)
          : undefined;
        return inputKey
          ? [
              this.#event(
                "input_resolved",
                sequence,
                event.chatUri,
                turnId,
                {
                  input_key: inputKey,
                  outcome: inputOutcome(action.response),
                  client_id: originClientId ?? null,
                },
                originClientId,
              ),
            ]
          : [];
      }
      case ActionType.ChatTurnComplete: {
        const assistantEvents = this.#flushAssistantPart(
          action.turnId,
          sequence,
          event.chatUri,
          true,
        );
        if (this.#activeTurnId === action.turnId) {
          this.#activeTurnId = undefined;
        }
        return [
          ...assistantEvents,
          this.#event(
            "turn_completed",
            sequence,
            event.chatUri,
            action.turnId,
            {},
            originClientId,
          ),
        ];
      }
      case ActionType.ChatTurnCancelled: {
        const assistantEvents = this.#flushAssistantPart(
          action.turnId,
          sequence,
          event.chatUri,
          false,
        );
        if (this.#activeTurnId === action.turnId) {
          this.#activeTurnId = undefined;
        }
        return [
          ...assistantEvents,
          this.#event(
            "turn_cancelled",
            sequence,
            event.chatUri,
            action.turnId,
            { summary: "当前 Turn 已取消" },
            originClientId,
          ),
        ];
      }
      case ActionType.ChatError: {
        const assistantEvents = this.#flushAssistantPart(
          action.turnId,
          sequence,
          event.chatUri,
          false,
        );
        if (this.#activeTurnId === action.turnId) {
          this.#activeTurnId = undefined;
        }
        if (this.#seenFailedTurns.has(action.turnId)) {
          return assistantEvents;
        }
        this.#seenFailedTurns.add(action.turnId);
        return [
          ...assistantEvents,
          this.#event(
            "turn_failed",
            sequence,
            event.chatUri,
            action.turnId,
            {
              summary: action.part.error.message || "当前 Turn 执行失败",
            },
            originClientId,
          ),
        ];
      }
      case ActionType.ChatTurnResume:
        this.#activeTurnId = action.turnId;
        return [];
      case ActionType.ChatTruncated:
        if (this.#activeTurnId) {
          this.#openAssistantParts.delete(this.#activeTurnId);
          this.#activeTurnId = undefined;
        }
        return [];
      default:
        return [];
    }
  }

  hostDisconnected(summary: string): PublishedEvent {
    return this.#event(
      "host_disconnected",
      undefined,
      this.#binding.chatUri,
      undefined,
      { summary },
    );
  }

  #inputEvents(state: SessionState, sequence: number): PublishedEvent[] {
    const events: PublishedEvent[] = [];
    const current = new Set<string>();
    for (const request of state.inputNeeded ?? []) {
      current.add(request.id);
      if (this.#pendingInputKeys.has(request.id)) {
        continue;
      }
      this.#pendingInputKeys.add(request.id);
      if (request.kind === SessionInputRequestKind.ToolConfirmation) {
        const stage =
          request.toolCall.status === ToolCallStatus.PendingResultConfirmation
            ? "result"
            : "parameter";
        this.#approvalKeyByTool.set(
          approvalMapKey(stage, request.turnId, request.toolCall.toolCallId),
          request.id,
        );
        events.push(
          this.#event(
            "approval_pending",
            sequence,
            request.chat,
            request.turnId,
            {
              approval_key: request.id,
              stage,
              tool_call_id: request.toolCall.toolCallId,
              tool_name: request.toolCall.displayName,
              summary: toolSummary(request.toolCall),
            },
          ),
        );
      } else if (request.kind === SessionInputRequestKind.ChatInput) {
        this.#inputKeyByRequest.set(request.request.id, request.id);
        if (this.#activeTurnId) {
          this.#inputTurnByKey.set(request.id, this.#activeTurnId);
        }
        const presentation = inputPresentation(request);
        events.push(
          this.#event(
            "input_pending",
            sequence,
            request.chat,
            this.#activeTurnId,
            {
              input_key: request.id,
              request_id: request.request.id,
              prompt: presentation.prompt,
              choices: presentation.choices,
              allow_freeform: presentation.allowFreeform,
              selection_mode: presentation.selectionMode,
            },
          ),
        );
      }
    }
    for (const previous of this.#pendingInputKeys) {
      if (!current.has(previous)) {
        this.#pendingInputKeys.delete(previous);
      }
    }
    return events;
  }

  #completedTurnEvents(
    turn: Turn,
    sequence: number,
    chatUri: string,
    historical: boolean,
  ): PublishedEvent[] {
    const events = this.#userMessageEvents(
      turn.id,
      turn.startedAt ?? new Date().toISOString(),
      turn.message,
      sequence,
      chatUri,
      historical,
      undefined,
    );
    this.#openAssistantParts.delete(turn.id);
    const markdownParts = turn.responseParts.filter(
      (part) => part.kind === ResponsePartKind.Markdown,
    );
    if (historical) {
      const unseenParts = markdownParts.filter(
        (part) =>
          !this.#seenAssistantParts.has(assistantPartKey(turn.id, part.id)),
      );
      for (const part of unseenParts) {
        this.#seenAssistantParts.add(assistantPartKey(turn.id, part.id));
      }
      const content = assistantText(turn.responseParts);
      if (unseenParts.length > 0 && content.length > 0) {
        events.push(
          this.#event(
            "assistant_message",
            sequence,
            chatUri,
            turn.id,
            {
              message_id: `turn:${turn.id}:assistant`,
              content,
              complete: true,
              historical,
              final_response: true,
            },
            undefined,
            turn.startedAt,
          ),
        );
      }
      return events;
    }

    const finalPart = [...markdownParts]
      .reverse()
      .find((part) => part.content.trim().length > 0);
    for (const part of markdownParts) {
      events.push(
        ...this.#assistantPartEvent(
          turn.id,
          part.id,
          part.content,
          sequence,
          chatUri,
          part === finalPart,
          false,
          turn.startedAt,
        ),
      );
    }
    return events;
  }

  #activeTurnAssistantEvents(
    turn: Pick<Turn, "id" | "startedAt" | "responseParts">,
    sequence: number,
    chatUri: string,
    historical: boolean,
  ): PublishedEvent[] {
    const events: PublishedEvent[] = [];
    const finalPartIndex = turn.responseParts.length - 1;
    for (const [index, part] of turn.responseParts.entries()) {
      if (part.kind !== ResponsePartKind.Markdown) {
        continue;
      }
      const key = assistantPartKey(turn.id, part.id);
      if (this.#seenAssistantParts.has(key)) {
        continue;
      }
      if (index < finalPartIndex) {
        if (historical) {
          this.#seenAssistantParts.add(key);
        } else {
          events.push(
            ...this.#assistantPartEvent(
              turn.id,
              part.id,
              part.content,
              sequence,
              chatUri,
              false,
              false,
              turn.startedAt,
            ),
          );
        }
        continue;
      }
      this.#openAssistantParts.set(turn.id, {
        partId: part.id,
        chunks: [part.content],
      });
    }
    return events;
  }

  #flushAssistantPart(
    turnId: string,
    sequence: number,
    chatUri: string,
    finalResponse: boolean,
  ): PublishedEvent[] {
    const part = this.#openAssistantParts.get(turnId);
    if (!part) {
      return [];
    }
    this.#openAssistantParts.delete(turnId);
    return this.#assistantPartEvent(
      turnId,
      part.partId,
      part.chunks.join(""),
      sequence,
      chatUri,
      finalResponse,
      false,
    );
  }

  #assistantPartEvent(
    turnId: string,
    partId: string,
    rawContent: string,
    sequence: number,
    chatUri: string,
    finalResponse: boolean,
    historical: boolean,
    occurredAt?: string,
  ): PublishedEvent[] {
    const key = assistantPartKey(turnId, partId);
    if (this.#seenAssistantParts.has(key)) {
      return [];
    }
    this.#seenAssistantParts.add(key);
    const content = rawContent.trim();
    if (content.length === 0) {
      return [];
    }
    return [
      this.#event(
        "assistant_message",
        sequence,
        chatUri,
        turnId,
        {
          message_id: `turn:${turnId}:assistant:${partId}`,
          content,
          complete: true,
          historical,
          final_response: finalResponse,
        },
        undefined,
        occurredAt,
      ),
    ];
  }

  #userMessageEvents(
    turnId: string,
    occurredAt: string,
    message: { readonly text: string; readonly origin: { readonly kind: MessageKind } },
    sequence: number,
    chatUri: string,
    historical: boolean,
    originClientId: string | undefined,
  ): PublishedEvent[] {
    if (
      message.origin.kind !== MessageKind.User ||
      this.#seenUserTurns.has(turnId)
    ) {
      return [];
    }
    this.#seenUserTurns.add(turnId);
    return [
      this.#event(
        "user_message",
        sequence,
        chatUri,
        turnId,
        {
          message_id: `turn:${turnId}:user`,
          content: message.text,
          complete: true,
          historical,
        },
        originClientId,
        occurredAt,
      ),
    ];
  }

  #toolEvents(
    state: ChatState,
    sequence: number,
    chatUri: string,
    historical: boolean,
  ): PublishedEvent[] {
    const events: PublishedEvent[] = [];
    for (const [turnId, tool] of toolsInChat(state)) {
      const previous = this.#toolStatuses.get(tool.toolCallId);
      this.#toolStatuses.set(tool.toolCallId, tool.status);
      if (historical || previous === tool.status) {
        continue;
      }
      events.push(
        this.#event(
          "tool_status",
          sequence,
          chatUri,
          turnId,
          {
            tool_call_id: tool.toolCallId,
            tool_name: tool.displayName,
            status: tool.status,
            summary: toolSummary(tool),
          },
        ),
      );
    }
    return events;
  }

  #event(
    kind: PublishedEventKind,
    serverSequence: number | undefined,
    chatUri: string | undefined,
    turnId: string | undefined,
    data: unknown,
    originClientId?: string,
    occurredAt?: string,
  ): PublishedEvent {
    const identity = [
      this.#binding.hostInstanceId,
      this.#binding.sessionUri,
      chatUri ?? "",
      kind,
      turnId ?? "",
      serverSequence ?? "",
      stableDataIdentity(data),
    ].join("\0");
    return {
      event_id: createHash("sha256").update(identity, "utf8").digest("hex"),
      host_instance_id: this.#binding.hostInstanceId,
      ...(serverSequence === undefined
        ? {}
        : { server_sequence: serverSequence }),
      session_uri: this.#binding.sessionUri,
      ...(chatUri ? { chat_uri: chatUri } : {}),
      ...(turnId ? { turn_id: turnId } : {}),
      kind,
      ...(originClientId ? { origin_client_id: originClientId } : {}),
      occurred_at: occurredAt ?? new Date().toISOString(),
      data,
    };
  }

  #matchesSession(endpointId: string, sessionUri: string): boolean {
    return (
      endpointId === this.#binding.endpointId &&
      sessionUri === this.#binding.sessionUri
    );
  }
}

function toolsInChat(state: ChatState): Array<readonly [string, ToolCallState]> {
  const tools: Array<readonly [string, ToolCallState]> = [];
  for (const turn of state.turns) {
    collectTools(turn.id, turn.responseParts, tools);
  }
  if (state.activeTurn) {
    collectTools(state.activeTurn.id, state.activeTurn.responseParts, tools);
  }
  return tools;
}

function collectTools(
  turnId: string,
  parts: readonly ResponsePart[],
  output: Array<readonly [string, ToolCallState]>,
): void {
  for (const part of parts) {
    if (part.kind === ResponsePartKind.ToolCall) {
      output.push([turnId, part.toolCall]);
    }
  }
}

function assistantText(parts: readonly ResponsePart[]): string {
  return parts
    .filter((part) => part.kind === ResponsePartKind.Markdown)
    .map((part) => part.content)
    .join("")
    .trim();
}

function assistantPartKey(turnId: string, partId: string): string {
  return `${turnId}\0${partId}`;
}

function toolSummary(tool: ToolCallState): string {
  if ("pastTenseMessage" in tool) {
    return stringOrMarkdown(tool.pastTenseMessage);
  }
  if ("invocationMessage" in tool && tool.invocationMessage) {
    return stringOrMarkdown(tool.invocationMessage);
  }
  return tool.intention ?? tool.displayName;
}

function stringOrMarkdown(value: StringOrMarkdown): string {
  return typeof value === "string" ? value : value.markdown;
}

function approvalMapKey(
  stage: "parameter" | "result",
  turnId: string,
  toolCallId: string,
): string {
  return `${stage}\0${turnId}\0${toolCallId}`;
}

function inputPresentation(request: Extract<
  SessionInputRequest,
  { kind: SessionInputRequestKind.ChatInput }
>): {
  readonly prompt: string;
  readonly choices: readonly string[];
  readonly allowFreeform: boolean;
  readonly selectionMode: "none" | "single" | "multi";
} {
  const questions = request.request.questions ?? [];
  if (questions.length !== 1 || !questions[0]) {
    return {
      prompt: `${request.request.message ?? "Agent 请求输入"}（该请求包含多个字段，请在 VS Code 中回答。）`,
      choices: [],
      allowFreeform: false,
      selectionMode: "none",
    };
  }
  const question = questions[0];
  const prompt = [request.request.message, question.message]
    .filter((item): item is string => Boolean(item))
    .join("\n");
  return {
    prompt,
    choices: questionChoices(question),
    allowFreeform: questionAllowsFreeform(question),
    selectionMode: questionSelectionMode(question),
  };
}

function questionSelectionMode(
  question: ChatInputQuestion,
): "none" | "single" | "multi" {
  switch (question.kind) {
    case ChatInputQuestionKind.Boolean:
    case ChatInputQuestionKind.SingleSelect:
      return "single";
    case ChatInputQuestionKind.MultiSelect:
      return "multi";
    default:
      return "none";
  }
}

function questionChoices(question: ChatInputQuestion): string[] {
  switch (question.kind) {
    case ChatInputQuestionKind.Boolean:
      return ["是", "否"];
    case ChatInputQuestionKind.SingleSelect:
    case ChatInputQuestionKind.MultiSelect:
      return question.options.map((option) => option.label);
    default:
      return [];
  }
}

function questionAllowsFreeform(question: ChatInputQuestion): boolean {
  switch (question.kind) {
    case ChatInputQuestionKind.Text:
    case ChatInputQuestionKind.Number:
    case ChatInputQuestionKind.Integer:
      return true;
    case ChatInputQuestionKind.SingleSelect:
    case ChatInputQuestionKind.MultiSelect:
      return question.allowFreeformInput === true;
    case ChatInputQuestionKind.Boolean:
      return false;
  }
}

function inputOutcome(response: string): "answered" | "declined" | "cancelled" {
  switch (response) {
    case "accept":
      return "answered";
    case "decline":
      return "declined";
    default:
      return "cancelled";
  }
}

function stableDataIdentity(data: unknown): string {
  return createHash("sha256")
    .update(JSON.stringify(sortJson(data)), "utf8")
    .digest("hex");
}

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortJson);
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => [key, sortJson(item)]),
  );
}
