import {
  ActionType,
  ResponsePartKind,
  TurnState,
  type ActionEnvelope,
  type ChatErrorAction,
  type ChatState,
  type ErrorInfo,
  type StateAction,
  type Turn,
} from "@microsoft/agent-host-protocol";

type LegacyChatErrorAction = Omit<ChatErrorAction, "part"> & {
  readonly error: ErrorInfo;
};

type CompatibleActionEnvelope = Omit<ActionEnvelope, "action"> & {
  readonly action: StateAction | LegacyChatErrorAction;
};

function hasOwnError<T extends object>(
  value: T,
): value is T & { readonly error: ErrorInfo } {
  return Object.hasOwn(value, "error");
}

export function normalizeLegacyTurnError(turn: Turn): Turn {
  if (turn.state !== TurnState.Error || !hasOwnError(turn)) {
    return turn;
  }

  const { error, ...normalizedTurn } = turn;
  const finalPart = turn.responseParts.at(-1);
  return {
    ...normalizedTurn,
    responseParts:
      finalPart?.kind === ResponsePartKind.Error
        ? turn.responseParts
        : [...turn.responseParts, { kind: ResponsePartKind.Error, error }],
  };
}

export function normalizeLegacyChatStateErrors(state: ChatState): ChatState {
  const turns = state.turns.map(normalizeLegacyTurnError);
  return turns.some((turn, index) => turn !== state.turns[index])
    ? { ...state, turns }
    : state;
}

export function normalizeLegacyActionEnvelope(
  envelope: CompatibleActionEnvelope,
): ActionEnvelope {
  const action = envelope.action;
  switch (action.type) {
    case ActionType.ChatError:
      if (hasOwnError(action)) {
        const { error, ...normalizedAction } = action;
        return {
          ...envelope,
          action: {
            ...normalizedAction,
            part: { kind: ResponsePartKind.Error, error },
          },
        };
      }
      return { ...envelope, action };
    case ActionType.ChatTurnsLoaded: {
      const turns = action.turns.map(normalizeLegacyTurnError);
      return turns.some((turn, index) => turn !== action.turns[index])
        ? { ...envelope, action: { ...action, turns } }
        : { ...envelope, action };
    }
    default:
      return { ...envelope, action };
  }
}
