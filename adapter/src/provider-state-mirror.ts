import {
  ActionType,
  chatReducer,
  sessionReducer,
  type ActionEnvelope,
  type ChatAction,
  type ChatState,
  type SessionAction,
  type SessionState,
  type Snapshot,
  type URI,
} from "@microsoft/agent-host-protocol";

import { normalizeLegacyChatStateErrors } from "./protocol-compatibility.js";

export type MirrorApplyResult =
  | "applied"
  | "invalid-action"
  | "rejected"
  | "stale"
  | "unhydrated"
  | "wrong-channel";

export class MirrorSnapshotError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MirrorSnapshotError";
  }
}

/**
 * Reducer-backed state for one provider session and its current default chat.
 * Resource kinds are assigned by the binding flow; URI text is never parsed.
 */
export class ProviderSessionStateMirror {
  readonly sessionUri: URI;

  readonly provider: string;

  #sessionState: SessionState | undefined;

  #sessionSeq = -1;

  #chatState: ChatState | undefined;

  #chatSeq = -1;

  constructor(sessionUri: URI, provider: string) {
    if (sessionUri.length === 0) {
      throw new TypeError("sessionUri must not be empty");
    }
    if (provider.length === 0) {
      throw new TypeError("provider must not be empty");
    }
    this.sessionUri = sessionUri;
    this.provider = provider;
  }

  get session(): SessionState | undefined {
    return this.#sessionState
      ? structuredClone(this.#sessionState)
      : undefined;
  }

  get sessionSeq(): number {
    return this.#sessionSeq;
  }

  get defaultChatUri(): URI | undefined {
    return this.#sessionState?.defaultChat;
  }

  get chat(): ChatState | undefined {
    return this.#chatState ? structuredClone(this.#chatState) : undefined;
  }

  get chatSeq(): number {
    return this.#chatSeq;
  }

  hydrateSession(snapshot: Snapshot): SessionState {
    if (snapshot.resource !== this.sessionUri || !isSessionState(snapshot.state)) {
      throw new MirrorSnapshotError(
        "subscription did not return the requested session state",
      );
    }
    if (snapshot.state.provider !== this.provider) {
      throw new MirrorSnapshotError(
        "session snapshot provider does not match the catalogue",
      );
    }
    assertSequence(snapshot.fromSeq);
    this.#sessionState = structuredClone(snapshot.state);
    this.#sessionSeq = snapshot.fromSeq;
    return structuredClone(this.#sessionState);
  }

  hydrateDefaultChat(snapshot: Snapshot, chatUri: URI): ChatState {
    if (snapshot.resource !== chatUri || !isChatState(snapshot.state)) {
      throw new MirrorSnapshotError(
        "subscription did not return the requested chat state",
      );
    }
    if (snapshot.state.resource !== chatUri) {
      throw new MirrorSnapshotError(
        "chat snapshot identity does not match the subscription",
      );
    }
    assertSequence(snapshot.fromSeq);
    this.#chatState = structuredClone(
      normalizeLegacyChatStateErrors(snapshot.state),
    );
    this.#chatSeq = snapshot.fromSeq;
    return structuredClone(this.#chatState);
  }

  clearDefaultChat(chatUri?: URI): void {
    if (chatUri !== undefined && this.#chatState?.resource !== chatUri) {
      return;
    }
    this.#chatState = undefined;
    this.#chatSeq = -1;
  }

  applySession(envelope: ActionEnvelope): MirrorApplyResult {
    if (envelope.channel !== this.sessionUri) {
      return "wrong-channel";
    }
    if (envelope.serverSeq <= this.#sessionSeq) {
      return "stale";
    }
    this.#sessionSeq = envelope.serverSeq;
    if (envelope.rejectionReason !== undefined) {
      return "rejected";
    }
    if (!this.#sessionState) {
      return "unhydrated";
    }
    if (!isSessionAction(envelope.action)) {
      return "invalid-action";
    }
    this.#sessionState = sessionReducer(
      this.#sessionState,
      envelope.action,
    );
    return "applied";
  }

  applyDefaultChat(envelope: ActionEnvelope): MirrorApplyResult {
    const chatState = this.#chatState;
    if (!chatState || envelope.channel !== chatState.resource) {
      return "wrong-channel";
    }
    if (envelope.serverSeq <= this.#chatSeq) {
      return "stale";
    }
    this.#chatSeq = envelope.serverSeq;
    if (envelope.rejectionReason !== undefined) {
      return "rejected";
    }
    if (!isChatAction(envelope.action)) {
      return "invalid-action";
    }
    this.#chatState = chatReducer(chatState, envelope.action);
    return "applied";
  }
}

function assertSequence(sequence: number): void {
  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw new MirrorSnapshotError("snapshot sequence is invalid");
  }
}

function isSessionState(state: Snapshot["state"]): state is SessionState {
  return (
    "provider" in state &&
    "lifecycle" in state &&
    "activeClients" in state &&
    "chats" in state
  );
}

function isChatState(state: Snapshot["state"]): state is ChatState {
  return "turns" in state && "modifiedAt" in state && "title" in state;
}

function isSessionAction(action: ActionEnvelope["action"]): action is SessionAction {
  switch (action.type) {
    case ActionType.SessionReady:
    case ActionType.SessionCreationFailed:
    case ActionType.SessionChatAdded:
    case ActionType.SessionChatRemoved:
    case ActionType.SessionChatUpdated:
    case ActionType.SessionDefaultChatChanged:
    case ActionType.SessionTitleChanged:
    case ActionType.SessionServerToolsChanged:
    case ActionType.SessionActiveClientSet:
    case ActionType.SessionActiveClientRemoved:
    case ActionType.SessionWorkingDirectorySet:
    case ActionType.SessionWorkingDirectoryRemoved:
    case ActionType.SessionWorkingDirectoryReplaced:
    case ActionType.SessionInputNeededSet:
    case ActionType.SessionInputNeededRemoved:
    case ActionType.SessionCustomizationsChanged:
    case ActionType.SessionCustomizationToggled:
    case ActionType.SessionCustomizationUpdated:
    case ActionType.SessionCustomizationRemoved:
    case ActionType.SessionMcpServerStateChanged:
    case ActionType.SessionMcpServerStartRequested:
    case ActionType.SessionMcpServerStopRequested:
    case ActionType.SessionIsReadChanged:
    case ActionType.SessionIsArchivedChanged:
    case ActionType.SessionActivityChanged:
    case ActionType.SessionChangesetsChanged:
    case ActionType.SessionConfigChanged:
    case ActionType.SessionMetaChanged:
      return true;
    default:
      return false;
  }
}

function isChatAction(action: ActionEnvelope["action"]): action is ChatAction {
  switch (action.type) {
    case ActionType.ChatTurnStarted:
    case ActionType.ChatDelta:
    case ActionType.ChatResponsePart:
    case ActionType.ChatToolCallStart:
    case ActionType.ChatToolCallDelta:
    case ActionType.ChatToolCallReady:
    case ActionType.ChatToolCallConfirmed:
    case ActionType.ChatToolCallComplete:
    case ActionType.ChatToolCallResultConfirmed:
    case ActionType.ChatToolCallContentChanged:
    case ActionType.ChatToolCallAuthRequired:
    case ActionType.ChatToolCallAuthResolved:
    case ActionType.ChatTurnComplete:
    case ActionType.ChatTurnCancelled:
    case ActionType.ChatError:
    case ActionType.ChatTurnResume:
    case ActionType.ChatActivityChanged:
    case ActionType.ChatWorkingDirectorySet:
    case ActionType.ChatWorkingDirectoryRemoved:
    case ActionType.ChatUsage:
    case ActionType.ChatReasoning:
    case ActionType.ChatPendingMessageSet:
    case ActionType.ChatPendingMessageRemoved:
    case ActionType.ChatQueuedMessagesReordered:
    case ActionType.ChatDraftChanged:
    case ActionType.ChatInputRequested:
    case ActionType.ChatInputAnswerChanged:
    case ActionType.ChatInputCompleted:
    case ActionType.ChatTruncated:
    case ActionType.ChatTurnsLoaded:
      return true;
    default:
      return false;
  }
}
