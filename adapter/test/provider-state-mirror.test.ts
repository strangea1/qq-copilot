import assert from "node:assert/strict";
import test from "node:test";

import {
  ActionType,
  BackgroundWorkKind,
  MessageKind,
  ResponsePartKind,
  SessionLifecycle,
  SessionStatus,
  TurnState,
  type BackgroundWork,
  type ChatAction,
} from "@microsoft/agent-host-protocol";

import {
  MirrorSnapshotError,
  ProviderSessionStateMirror,
} from "../src/provider-state-mirror.js";

test("VS Code 1.142 chat metadata and background work use audited reducers", () => {
  const chatUri = "opaque-chat";
  const mirror = new ProviderSessionStateMirror("opaque-session", "copilot");
  mirror.hydrateDefaultChat(
    {
      resource: chatUri,
      fromSeq: 1,
      state: {
        resource: chatUri,
        title: "Chat",
        status: SessionStatus.Idle,
        modifiedAt: "2026-10-20T00:00:00.000Z",
        turns: [],
      },
    },
    chatUri,
  );
  let serverSeq = 1;
  const apply = (action: ChatAction): void => {
    assert.equal(
      mirror.applyDefaultChat({
        channel: chatUri,
        serverSeq: ++serverSeq,
        origin: undefined,
        action,
      }),
      "applied",
    );
  };

  const work: BackgroundWork = {
    kind: BackgroundWorkKind.Shell,
    id: "background-1",
    label: "Build",
    startedAt: "2026-10-20T00:00:00.000Z",
    command: "npm run build",
  };
  apply({ type: ActionType.ChatBackgroundWorkSet, work });
  assert.deepEqual(mirror.chat?.backgroundWork, [work]);
  apply({ type: ActionType.ChatBackgroundWorkRemoved, id: work.id });
  assert.equal(mirror.chat?.backgroundWork?.length ?? 0, 0);

  apply({ type: ActionType.ChatMovableChanged, movable: true });
  assert.equal(mirror.chat?.movable, true);
  apply({ type: ActionType.ChatChangesetsChanged, changesets: [] });
  assert.deepEqual(mirror.chat?.changesets, []);
  const canvases = [{ resource: "opaque-canvas" }];
  apply({ type: ActionType.ChatCanvasesChanged, canvases });
  assert.deepEqual(mirror.chat?.canvases, canvases);
  apply({ type: ActionType.ChatCanvasesChanged, canvases: undefined });
  assert.equal(mirror.chat?.canvases, undefined);

  apply({ type: ActionType.ChatIsReadChanged, isRead: true });
  apply({ type: ActionType.ChatIsArchivedChanged, isArchived: true });
  assert.equal(
    mirror.chat!.status & (SessionStatus.IsRead | SessionStatus.IsArchived),
    SessionStatus.IsRead | SessionStatus.IsArchived,
  );
  apply({ type: ActionType.ChatIsReadChanged, isRead: false });
  apply({ type: ActionType.ChatIsArchivedChanged, isArchived: false });
  assert.equal(mirror.chat?.status, SessionStatus.Idle);
});

test("VS Code 1.142 session chat ordering and MCP background actions remain compatible", () => {
  const sessionUri = "opaque-session";
  const mirror = new ProviderSessionStateMirror(sessionUri, "copilot");
  const chats = ["opaque-chat-a", "opaque-chat-b"].map((resource) => ({
    resource,
    title: resource,
    status: SessionStatus.Idle,
    modifiedAt: "2026-10-20T00:00:00.000Z",
  }));
  mirror.hydrateSession({
    resource: sessionUri,
    fromSeq: 1,
    state: {
      provider: "copilot",
      title: "Session",
      status: SessionStatus.Idle,
      lifecycle: SessionLifecycle.Ready,
      activeClients: [],
      chats,
      defaultChat: chats[0]!.resource,
    },
  });
  assert.equal(
    mirror.applySession({
      channel: sessionUri,
      serverSeq: 2,
      origin: undefined,
      action: {
        type: ActionType.SessionChatsReordered,
        chats: chats.map((chat) => chat.resource).reverse(),
      },
    }),
    "applied",
  );
  assert.deepEqual(mirror.session?.chats, [...chats].reverse());
  assert.equal(mirror.session?.defaultChat, chats[0]!.resource);
  assert.equal(
    mirror.applySession({
      channel: sessionUri,
      serverSeq: 3,
      origin: undefined,
      action: {
        type: ActionType.SessionMcpServerBackgroundRequested,
        id: "already-removed-server",
      },
    }),
    "applied",
  );
});

test("provider mirror routes opaque URIs and rejects stale/rejected actions", () => {
  const sessionUri = "urn:vendor-owned:session/42?not=parsed";
  const chatUri = "custom+chat://authority/default";
  const mirror = new ProviderSessionStateMirror(sessionUri, "copilot");

  mirror.hydrateSession({
    resource: sessionUri,
    fromSeq: 10,
    state: {
      provider: "copilot",
      title: "Initial",
      status: SessionStatus.Idle,
      lifecycle: SessionLifecycle.Ready,
      activeClients: [],
      chats: [
        {
          resource: chatUri,
          title: "Chat",
          status: SessionStatus.Idle,
          modifiedAt: "2026-01-01T00:00:00.000Z",
        },
      ],
      defaultChat: chatUri,
    },
  });
  mirror.hydrateDefaultChat(
    {
      resource: chatUri,
      fromSeq: 20,
      state: {
        resource: chatUri,
        title: "Chat",
        status: SessionStatus.Idle,
        modifiedAt: "2026-01-01T00:00:00.000Z",
        turns: [],
      },
    },
    chatUri,
  );

  assert.equal(
    mirror.applySession({
      channel: sessionUri,
      serverSeq: 11,
      origin: undefined,
      action: {
        type: ActionType.SessionTitleChanged,
        title: "Reduced",
      },
    }),
    "applied",
  );
  assert.equal(mirror.session?.title, "Reduced");

  assert.equal(
    mirror.applySession({
      channel: sessionUri,
      serverSeq: 9,
      origin: undefined,
      action: {
        type: ActionType.SessionTitleChanged,
        title: "Stale",
      },
    }),
    "stale",
  );
  assert.equal(mirror.session?.title, "Reduced");

  assert.equal(
    mirror.applyDefaultChat({
      channel: chatUri,
      serverSeq: 21,
      origin: { clientId: "client-1", clientSeq: 1 },
      rejectionReason: "not accepted",
      action: {
        type: ActionType.ChatTurnStarted,
        turnId: "rejected",
        startedAt: "2026-01-01T00:00:00.000Z",
        message: { text: "no", origin: { kind: MessageKind.User } },
      },
    }),
    "rejected",
  );
  assert.equal(mirror.chat?.activeTurn, undefined);
});

test("provider mirror rejects a mismatched session provider", () => {
  const mirror = new ProviderSessionStateMirror("opaque-session", "copilot");
  assert.throws(
    () =>
      mirror.hydrateSession({
        resource: "opaque-session",
        fromSeq: 1,
        state: {
          provider: "other",
          title: "Wrong host",
          status: SessionStatus.Idle,
          lifecycle: SessionLifecycle.Ready,
          activeClients: [],
          chats: [],
        },
      }),
    MirrorSnapshotError,
  );
});

test("provider mirror applies durable errors and resumable turns", () => {
  const sessionUri = "opaque-session";
  const chatUri = "opaque-chat";
  const mirror = new ProviderSessionStateMirror(sessionUri, "copilot");
  mirror.hydrateDefaultChat(
    {
      resource: chatUri,
      fromSeq: 1,
      state: {
        resource: chatUri,
        title: "Chat",
        status: SessionStatus.InProgress,
        modifiedAt: "2026-09-02T00:00:00.000Z",
        turns: [],
        activeTurn: {
          id: "turn-1",
          startedAt: "2026-09-02T00:00:00.000Z",
          message: {
            text: "run",
            origin: { kind: MessageKind.User },
          },
          responseParts: [],
          usage: undefined,
        },
      },
    },
    chatUri,
  );

  assert.equal(
    mirror.applyDefaultChat({
      channel: chatUri,
      serverSeq: 2,
      origin: undefined,
      action: {
        type: ActionType.ChatResponsePart,
        turnId: "turn-1",
        part: {
          kind: ResponsePartKind.Error,
          error: {
            errorType: "test",
            message: "must be atomic",
          },
        },
      },
    }),
    "applied",
  );
  assert.deepEqual(mirror.chat?.activeTurn?.responseParts, []);

  assert.equal(
    mirror.applyDefaultChat({
      channel: chatUri,
      serverSeq: 3,
      origin: undefined,
      action: {
        type: ActionType.ChatError,
        turnId: "turn-1",
        duration: 50,
        part: {
          kind: ResponsePartKind.Error,
          error: {
            errorType: "test",
            message: "retryable",
          },
          resumable: true,
        },
      },
    }),
    "applied",
  );
  const failedState = mirror.chat;
  assert.equal(failedState?.activeTurn, undefined);
  assert.equal(failedState?.turns[0]?.state, TurnState.Error);
  assert.equal(
    failedState?.turns[0]?.responseParts.at(-1)?.kind,
    ResponsePartKind.Error,
  );

  assert.equal(
    mirror.applyDefaultChat({
      channel: chatUri,
      serverSeq: 4,
      origin: undefined,
      action: {
        type: ActionType.ChatTurnResume,
        turnId: "turn-1",
      },
    }),
    "applied",
  );
  const resumedState = mirror.chat;
  assert.equal(resumedState?.activeTurn?.id, "turn-1");
  assert.deepEqual(resumedState?.turns, []);
});
