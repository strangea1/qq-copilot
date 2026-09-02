import assert from "node:assert/strict";
import test from "node:test";

import {
  ActionType,
  MessageKind,
  ResponsePartKind,
  SessionStatus,
  TurnState,
  type ChatState,
  type ErrorInfo,
  type Turn,
} from "@microsoft/agent-host-protocol";

import {
  normalizeLegacyActionEnvelope,
  normalizeLegacyChatStateErrors,
  normalizeLegacyTurnError,
} from "../src/protocol-compatibility.js";

const legacyError: ErrorInfo = {
  errorType: "legacy",
  message: "legacy failure",
};

function legacyTurn(
  responseParts: Turn["responseParts"] = [],
): Turn & { readonly error: ErrorInfo } {
  return {
    id: "turn-1",
    startedAt: "2026-09-02T00:00:00.000Z",
    message: {
      text: "run",
      origin: { kind: MessageKind.User },
    },
    responseParts,
    usage: undefined,
    state: TurnState.Error,
    error: legacyError,
  };
}

test("legacy completed errors become one durable response part", () => {
  const turn = legacyTurn();
  const normalized = normalizeLegacyTurnError(turn);
  assert.notEqual(normalized, turn);
  assert.equal("error" in normalized, false);
  assert.deepEqual(normalized.responseParts, [
    { kind: ResponsePartKind.Error, error: legacyError },
  ]);

  const alreadyDurable = legacyTurn([
    { kind: ResponsePartKind.Error, error: legacyError, resumable: true },
  ]);
  const normalizedDurable = normalizeLegacyTurnError(alreadyDurable);
  assert.equal(normalizedDurable.responseParts.length, 1);
  assert.equal(normalizedDurable.responseParts[0]?.kind, ResponsePartKind.Error);
});

test("legacy snapshot and loaded-turn errors are normalized idempotently", () => {
  const state: ChatState = {
    resource: "ahp-chat://default",
    title: "Default",
    status: SessionStatus.Idle,
    modifiedAt: "2026-09-02T00:00:00.000Z",
    turns: [legacyTurn()],
  };
  const normalizedState = normalizeLegacyChatStateErrors(state);
  assert.notEqual(normalizedState, state);
  assert.strictEqual(
    normalizeLegacyChatStateErrors(normalizedState),
    normalizedState,
  );

  const normalizedEnvelope = normalizeLegacyActionEnvelope({
    channel: state.resource,
    serverSeq: 2,
    origin: undefined,
    action: {
      type: ActionType.ChatTurnsLoaded,
      turns: [legacyTurn()],
    },
  });
  assert.equal(normalizedEnvelope.action.type, ActionType.ChatTurnsLoaded);
  if (normalizedEnvelope.action.type === ActionType.ChatTurnsLoaded) {
    assert.equal(
      normalizedEnvelope.action.turns[0]?.responseParts.at(-1)?.kind,
      ResponsePartKind.Error,
    );
  }
});

test("legacy live chat errors become canonical action parts", () => {
  const normalized = normalizeLegacyActionEnvelope({
    channel: "ahp-chat://default",
    serverSeq: 3,
    origin: undefined,
    action: {
      type: ActionType.ChatError,
      turnId: "turn-1",
      duration: 25,
      error: legacyError,
    },
  });
  assert.equal(normalized.action.type, ActionType.ChatError);
  if (normalized.action.type === ActionType.ChatError) {
    assert.deepEqual(normalized.action.part, {
      kind: ResponsePartKind.Error,
      error: legacyError,
    });
  }
  assert.deepEqual(normalizeLegacyActionEnvelope(normalized), normalized);
});
