import assert from "node:assert/strict";
import test from "node:test";

import {
  ActionType,
  MessageKind,
  ResponsePartKind,
  SessionInputRequestKind,
  SessionLifecycle,
  SessionStatus,
  ToolCallStatus,
  TurnState,
} from "@microsoft/agent-host-protocol";

import { AhpEventNormalizer } from "../src/event-normalizer.js";

const sessionUri = "copilot:/session-1";
const chatUri = "ahp-chat://default/session-1";

function normalizer(): AhpEventNormalizer {
  return new AhpEventNormalizer({
    adapterId: "qq-adapter",
    endpointId: "endpoint-1",
    hostInstanceId: "host-1",
    generation: 1,
    sessionUri,
    chatUri,
  });
}

test("normalizer marks hydrated history and emits live user/assistant text", () => {
  const events = normalizer();
  const historical = events.chatSnapshot({
    endpointId: "endpoint-1",
    sessionUri,
    provider: "copilot",
    chatUri,
    serverSeq: 10,
    state: {
      resource: chatUri,
      title: "Shared",
      status: SessionStatus.Idle,
      modifiedAt: "2026-08-27T00:00:00Z",
      turns: [
        {
          id: "old-turn",
          startedAt: "2026-08-27T00:00:00Z",
          message: {
            text: "old question",
            origin: { kind: MessageKind.User },
          },
          responseParts: [
            {
              kind: ResponsePartKind.Markdown,
              id: "old-answer",
              content: "old answer",
            },
          ],
          usage: undefined,
          state: TurnState.Complete,
        },
      ],
    },
  });
  const oldMessages = historical.filter(
    (event) =>
      event.kind === "user_message" ||
      event.kind === "assistant_message",
  );
  assert.equal(oldMessages.length, 2);
  assert.ok(
    oldMessages.every(
      (event) =>
        typeof event.data === "object" &&
        event.data !== null &&
        "historical" in event.data &&
        event.data.historical === true,
    ),
  );

  const started = events.action({
    scope: "chat",
    endpointId: "endpoint-1",
    sessionUri,
    provider: "copilot",
    chatUri,
    envelope: {
      channel: chatUri,
      serverSeq: 11,
      origin: { clientId: "vscode", clientSeq: 7 },
      action: {
        type: ActionType.ChatTurnStarted,
        turnId: "new-turn",
        startedAt: "2026-08-27T00:01:00Z",
        message: {
          text: "new question",
          origin: { kind: MessageKind.User },
        },
      },
    },
  });
  const user = started.find((event) => event.kind === "user_message");
  assert.equal(user?.origin_client_id, "vscode");
  assert.deepEqual(user?.data, {
    message_id: "turn:new-turn:user",
    content: "new question",
    complete: true,
    historical: false,
  });

  const completed = events.chatSnapshot({
    endpointId: "endpoint-1",
    sessionUri,
    provider: "copilot",
    chatUri,
    serverSeq: 12,
    state: {
      resource: chatUri,
      title: "Shared",
      status: SessionStatus.Idle,
      modifiedAt: "2026-08-27T00:02:00Z",
      turns: [
        {
          id: "new-turn",
          startedAt: "2026-08-27T00:01:00Z",
          message: {
            text: "new question",
            origin: { kind: MessageKind.User },
          },
          responseParts: [
            {
              kind: ResponsePartKind.Markdown,
              id: "answer",
              content: "new answer",
            },
          ],
          usage: undefined,
          state: TurnState.Complete,
        },
      ],
    },
  });
  const assistant = completed.find(
    (event) => event.kind === "assistant_message",
  );
  assert.deepEqual(assistant?.data, {
    message_id: "turn:new-turn:assistant",
    content: "new answer",
    complete: true,
    historical: false,
  });
});

test("normalizer publishes a pending tool confirmation from session state", () => {
  const events = normalizer().sessionSnapshot({
    endpointId: "endpoint-1",
    sessionUri,
    provider: "copilot",
    serverSeq: 20,
    state: {
      provider: "copilot",
      title: "Shared",
      status: SessionStatus.InputNeeded,
      lifecycle: SessionLifecycle.Ready,
      activeClients: [],
      chats: [
        {
          resource: chatUri,
          title: "Shared",
          status: SessionStatus.InputNeeded,
          modifiedAt: "2026-08-27T00:03:00Z",
        },
      ],
      defaultChat: chatUri,
      inputNeeded: [
        {
          id: "approval-request-1",
          kind: SessionInputRequestKind.ToolConfirmation,
          chat: chatUri,
          turnId: "turn-1",
          toolCall: {
            status: ToolCallStatus.PendingConfirmation,
            toolCallId: "tool-1",
            toolName: "terminal",
            displayName: "Run in terminal",
            invocationMessage: "Run cargo test",
          },
        },
      ],
    },
  });
  const approval = events.find(
    (event) => event.kind === "approval_pending",
  );
  assert.deepEqual(approval?.data, {
    approval_key: "approval-request-1",
    stage: "parameter",
    tool_call_id: "tool-1",
    tool_name: "Run in terminal",
    summary: "Run cargo test",
  });
});
