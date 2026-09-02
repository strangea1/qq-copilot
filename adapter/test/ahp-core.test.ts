import assert from "node:assert/strict";
import test from "node:test";

import {
  ActionType,
  ChatInputAnswerState,
  ChatInputAnswerValueKind,
  ChatInputQuestionKind,
  ChatInputResponseKind,
  ConfirmationOptionKind,
  SessionInputRequestKind,
  SessionLifecycle,
  SessionStatus,
  ToolCallConfirmationReason,
  ToolCallStatus,
  type ChatState,
  type SessionState,
  type SessionSummary,
  type Snapshot,
  type URI,
} from "@microsoft/agent-host-protocol";
import {
  InMemoryTransport,
  type AhpTransport,
  type TransportFrame,
} from "@microsoft/agent-host-protocol/client";

import {
  AhpCore,
  AhpOperationError,
  type ChatSnapshotEvent,
  type ConnectionEvent,
  type CoreErrorEvent,
  type DomainActionEvent,
  type IncompatibilityEvent,
  type SessionSnapshotEvent,
} from "../src/ahp-core.js";
import type { EndpointRegistryEntry } from "../src/endpoint-registry.js";

interface ResourceSnapshot {
  readonly state: Snapshot["state"];
  readonly fromSeq: number;
}

interface FakeServerConfig {
  readonly summary: SessionSummary;
  readonly resources: ReadonlyMap<URI, ResourceSnapshot>;
  readonly protocolVersion?: string;
  readonly beforeSubscribe?: (
    channel: URI,
    server: FakeAhpServer,
  ) => Promise<void> | void;
  readonly echoStartedTurns?: boolean;
}

class FakeAhpServer {
  readonly initializeClientIds: string[] = [];

  readonly initializeProtocolVersions: string[][] = [];

  readonly dispatches: unknown[] = [];

  readonly #transport: AhpTransport;

  readonly #config: FakeServerConfig;

  readonly done: Promise<void>;

  #serverSeq = 100;

  constructor(transport: AhpTransport, config: FakeServerConfig) {
    this.#transport = transport;
    this.#config = config;
    this.done = this.#serve();
  }

  async emitAction(
    channel: URI,
    action: unknown,
    serverSeq: number,
    origin?: { readonly clientId: string; readonly clientSeq: number },
    rejectionReason?: string,
  ): Promise<void> {
    await this.#transport.send(
      JSON.stringify({
        jsonrpc: "2.0",
        method: "action",
        params: {
          channel,
          action,
          serverSeq,
          origin,
          ...(rejectionReason ? { rejectionReason } : {}),
        },
      }),
    );
  }

  async #serve(): Promise<void> {
    for (;;) {
      const frame = await this.#transport.recv();
      if (frame === null) {
        return;
      }
      const message = decodeFrame(frame);
      if (!isRecord(message) || typeof message.method !== "string") {
        continue;
      }
      const params = isRecord(message.params) ? message.params : {};
      switch (message.method) {
        case "initialize": {
          const clientId = stringProperty(params, "clientId");
          if (clientId) {
            this.initializeClientIds.push(clientId);
          }
          const protocolVersions = stringArrayProperty(
            params,
            "protocolVersions",
          );
          if (protocolVersions) {
            this.initializeProtocolVersions.push(protocolVersions);
          }
          await this.#respond(message.id, {
            protocolVersion: this.#config.protocolVersion ?? "0.9.0",
            serverSeq: 1,
            snapshots: [
              {
                resource: "ahp-root://",
                state: { agents: [] },
                fromSeq: 1,
              },
            ],
          });
          break;
        }
        case "listSessions":
          await this.#respond(message.id, {
            items: [this.#config.summary],
          });
          break;
        case "subscribe": {
          const channel = stringProperty(params, "channel");
          if (!channel) {
            await this.#respond(message.id, {});
            break;
          }
          await this.#config.beforeSubscribe?.(channel, this);
          const snapshot = this.#config.resources.get(channel);
          await this.#respond(
            message.id,
            snapshot
              ? {
                  snapshot: {
                    resource: channel,
                    state: snapshot.state,
                    fromSeq: snapshot.fromSeq,
                  },
                }
              : {},
          );
          break;
        }
        case "dispatchAction":
          this.dispatches.push(structuredClone(params));
          if (this.#config.echoStartedTurns) {
            await this.#echoStartedTurn(params);
          }
          break;
        case "unsubscribe":
          break;
      }
    }
  }

  async #echoStartedTurn(params: Record<string, unknown>): Promise<void> {
    const action = params.action;
    const channel = stringProperty(params, "channel");
    const clientSeq = numberProperty(params, "clientSeq");
    if (
      !channel ||
      clientSeq === undefined ||
      !isRecord(action) ||
      action.type !== ActionType.ChatTurnStarted
    ) {
      return;
    }
    this.#serverSeq += 1;
    await this.emitAction(
      channel,
      action,
      this.#serverSeq,
      {
        clientId: this.initializeClientIds[0] ?? "unknown-client",
        clientSeq,
      },
    );
  }

  async #respond(id: unknown, result: unknown): Promise<void> {
    if (typeof id !== "number" && typeof id !== "string") {
      return;
    }
    await this.#transport.send(
      JSON.stringify({ jsonrpc: "2.0", id, result }),
    );
  }
}

test("core gates protocols, lists all endpoints, keeps clientId stable, and redacts registry secrets", async () => {
  const tokenA = "token_A_123456789012345678901234";
  const tokenB = "token_B_123456789012345678901234";
  const pathA = "\\\\.\\pipe\\secret-ahp-a";
  const pathB = "\\\\.\\pipe\\secret-ahp-b";
  const entryA = fakeEndpoint("a", "endpoint_A_123456", tokenA, pathA);
  const entryB = fakeEndpoint("b", "endpoint_B_123456", tokenB, pathB);
  const incompatible = fakeEndpoint(
    "c",
    "endpoint_C_123456",
    "token_C_123456789012345678901234",
    "\\\\.\\pipe\\secret-ahp-c",
    "9.9.9",
  );
  const sessionA = summary(
    "opaque-session-a",
    `contains ${tokenA} and ${pathA}`,
  );
  const sessionB = summary("opaque-session-b", "Second");
  const [clientA, serverTransportA] = InMemoryTransport.pair();
  const [clientB, serverTransportB] = InMemoryTransport.pair();
  const serverA = new FakeAhpServer(serverTransportA, {
    summary: sessionA,
    resources: new Map(),
  });
  const serverB = new FakeAhpServer(serverTransportB, {
    summary: sessionB,
    resources: new Map(),
  });
  const transports = new Map<string, AhpTransport>([
    [entryA.instanceId, clientA],
    [entryB.instanceId, clientB],
  ]);
  const opened: string[] = [];
  const connections: ConnectionEvent[] = [];
  const incompatibilities: IncompatibilityEvent[] = [];
  const actions: DomainActionEvent[] = [];
  const core = new AhpCore({
    userDataDirectory: "unused",
    clientId: "stable-client-id",
    watch: false,
    callbacks: {
      onConnection: (event) => connections.push(event),
      onIncompatibility: (event) => incompatibilities.push(event),
      onAction: (event) => actions.push(event),
    },
    dependencies: {
      discoverEndpoints: async () => [entryA, entryB, incompatible],
      openTransport: async (entry) => {
        opened.push(entry.instanceId);
        const transport = transports.get(entry.instanceId);
        assert.ok(transport);
        return transport;
      },
    },
  });

  try {
    await core.start();
    const catalogue = await core.listSessions();
    assert.equal(catalogue.endpoints.length, 3);
    assert.equal(
      catalogue.endpoints.filter(
        (endpoint) => endpoint.connection === "connected",
      ).length,
      2,
    );
    assert.deepEqual(
      new Set(
        catalogue.endpoints.flatMap((endpoint) =>
          endpoint.sessions.map((session) => session.resource),
        ),
      ),
      new Set(["opaque-session-a", "opaque-session-b"]),
    );
    assert.deepEqual(opened.sort(), [
      entryA.instanceId,
      entryB.instanceId,
    ]);
    assert.deepEqual(serverA.initializeClientIds, ["stable-client-id"]);
    assert.deepEqual(serverB.initializeClientIds, ["stable-client-id"]);
    assert.equal(
      serverA.initializeProtocolVersions[0]?.includes("0.9.0"),
      true,
    );
    assert.equal(incompatibilities.length, 1);
    assert.equal(incompatibilities[0]?.reason, "advertised-version");

    const serializedCatalogue = JSON.stringify(catalogue);
    for (const secret of [tokenA, tokenB, pathA, pathB]) {
      assert.equal(serializedCatalogue.includes(secret), false);
    }

    await serverA.emitAction(
      "ahp-root://",
      {
        type: ActionType.RootActiveSessionsChanged,
        activeSessions: 2,
      },
      3,
      undefined,
      `${tokenA} ${pathA}`,
    );
    await waitFor(() => actions.length === 1);
    const serializedCallbacks = JSON.stringify({
      actions,
      connections,
      incompatibilities,
    });
    assert.equal(serializedCallbacks.includes(tokenA), false);
    assert.equal(serializedCallbacks.includes(pathA), false);
    assert.match(serializedCallbacks, /\[redacted\]/u);
  } finally {
    await core.stop();
    await Promise.all([serverA.done, serverB.done]);
  }
});

test("core rejects a negotiated protocol that differs from the endpoint advertisement", async () => {
  const entry = fakeEndpoint(
    "n",
    "endpoint_N_123456",
    "token_N_123456789012345678901234",
    "\\\\.\\pipe\\secret-ahp-n",
  );
  const [clientTransport, serverTransport] = InMemoryTransport.pair();
  const server = new FakeAhpServer(serverTransport, {
    summary: summary("opaque-session-n", "Negotiation mismatch"),
    resources: new Map(),
    protocolVersion: "1.0.0",
  });
  const incompatibilities: IncompatibilityEvent[] = [];
  const core = new AhpCore({
    userDataDirectory: "unused",
    clientId: "protocol-gate-client",
    watch: false,
    callbacks: {
      onIncompatibility: (event) => incompatibilities.push(event),
    },
    dependencies: {
      discoverEndpoints: async () => [entry],
      openTransport: async () => clientTransport,
    },
  });

  try {
    const catalogue = await core.start();
    assert.equal(catalogue.endpoints[0]?.connection, "incompatible");
    assert.equal(catalogue.endpoints[0]?.selectedProtocol, "1.0.0");
    assert.equal(incompatibilities[0]?.reason, "negotiated-version");
    assert.equal(incompatibilities[0]?.selectedProtocol, "1.0.0");
  } finally {
    await core.stop();
    await server.done;
  }
});

test("binding losslessly hydrates opaque session/default-chat URIs and dispatches typed operations", async () => {
  const sessionUri = "urn:provider:session:exact-value";
  const chatA = "chat+vendor://opaque/A";
  const chatB = "chat+vendor://opaque/B";
  const secondaryChat = "urn:provider:chat:tool-owner";
  const entry = fakeEndpoint(
    "d",
    "endpoint_D_123456",
    "token_D_123456789012345678901234",
    "\\\\.\\pipe\\secret-ahp-d",
  );
  const sessionState: SessionState = {
    provider: "copilot",
    title: "Initial",
    status: SessionStatus.InputNeeded,
    lifecycle: SessionLifecycle.Ready,
    activeClients: [],
    chats: [
      chatSummary(chatA),
      chatSummary(chatB),
    ],
    defaultChat: chatA,
    inputNeeded: [
      {
        id: "parameter-request",
        kind: SessionInputRequestKind.ToolConfirmation,
        chat: secondaryChat,
        turnId: "tool-turn",
        toolCall: {
          status: ToolCallStatus.PendingConfirmation,
          toolCallId: "tool-parameters",
          toolName: "write",
          displayName: "Write",
          invocationMessage: "Write a file",
          toolInput: "{\"path\":\"a\"}",
          editable: true,
          options: [
            {
              id: "approve-once",
              label: "Approve",
              kind: ConfirmationOptionKind.Approve,
            },
            {
              id: "deny-once",
              label: "Deny",
              kind: ConfirmationOptionKind.Deny,
            },
          ],
        },
      },
      {
        id: "result-request",
        kind: SessionInputRequestKind.ToolConfirmation,
        chat: secondaryChat,
        turnId: "tool-turn",
        toolCall: {
          status: ToolCallStatus.PendingResultConfirmation,
          toolCallId: "tool-result",
          toolName: "read",
          displayName: "Read",
          invocationMessage: "Read a file",
          toolInput: "{\"path\":\"a\"}",
          confirmed: ToolCallConfirmationReason.UserAction,
          success: true,
          pastTenseMessage: "Read a file",
        },
      },
      {
        id: "session-input-id",
        kind: SessionInputRequestKind.ChatInput,
        chat: chatB,
        request: {
          id: "elicitation-id",
          message: "Choose",
          questions: [
            {
              id: "answer",
              kind: ChatInputQuestionKind.Text,
              message: "Value",
              required: true,
            },
          ],
        },
      },
    ],
  };
  const idleChat = (resource: URI): ChatState => ({
    resource,
    title: resource,
    status: SessionStatus.Idle,
    modifiedAt: "2026-08-28T00:00:00.000Z",
    turns: [],
  });
  const resources = new Map<URI, ResourceSnapshot>([
    [sessionUri, { state: sessionState, fromSeq: 10 }],
    [chatA, { state: idleChat(chatA), fromSeq: 20 }],
    [chatB, { state: idleChat(chatB), fromSeq: 20 }],
  ]);
  const [clientTransport, serverTransport] = InMemoryTransport.pair();
  const server = new FakeAhpServer(serverTransport, {
    summary: summary(sessionUri, "Bound session"),
    resources,
    echoStartedTurns: true,
    beforeSubscribe: async (channel, currentServer) => {
      if (channel === sessionUri) {
        await currentServer.emitAction(
          sessionUri,
          {
            type: ActionType.SessionTitleChanged,
            title: "stale-title",
          },
          9,
        );
        await currentServer.emitAction(
          sessionUri,
          {
            type: ActionType.SessionDefaultChatChanged,
            defaultChat: chatB,
          },
          11,
        );
      }
      if (channel === chatB) {
        await currentServer.emitAction(
          chatB,
          {
            type: ActionType.ChatTurnStarted,
            turnId: "pre-response-turn",
            startedAt: "2026-08-28T00:00:00.000Z",
            message: {
              text: "already running",
              origin: { kind: "user" },
            },
          },
          21,
        );
      }
    },
  });
  const sessionSnapshots: SessionSnapshotEvent[] = [];
  const chatSnapshots: ChatSnapshotEvent[] = [];
  const actions: DomainActionEvent[] = [];
  const errors: CoreErrorEvent[] = [];
  let monotonicNow = 500;
  const ids = ["new-turn", "queued-message"];
  const core = new AhpCore({
    userDataDirectory: "unused",
    clientId: "stable-binding-client",
    watch: false,
    callbacks: {
      onSessionSnapshot: (event) => sessionSnapshots.push(event),
      onChatSnapshot: (event) => chatSnapshots.push(event),
      onAction: (event) => actions.push(event),
      onError: (event) => errors.push(event),
    },
    dependencies: {
      discoverEndpoints: async () => [entry],
      openTransport: async () => clientTransport,
      createId: () => ids.shift() ?? "fallback-id",
      monotonicNow: () => monotonicNow,
    },
  });

  try {
    const catalogue = await core.start();
    const endpointId = catalogue.endpoints[0]?.endpoint.id;
    assert.ok(endpointId);
    const binding = await core.bindSession(endpointId, sessionUri);
    await waitFor(
      () =>
        binding.snapshot().defaultChat?.resource === chatB &&
        binding.snapshot().defaultChat?.activeTurn?.id ===
          "pre-response-turn",
    );
    assert.equal(binding.snapshot().session?.defaultChat, chatB);
    assert.equal(binding.snapshot().session?.title, "Initial");
    assert.equal(
      actions.some(
        (event) =>
          event.envelope.serverSeq === 9,
      ),
      false,
    );

    await server.emitAction(
      chatB,
      {
        type: ActionType.ChatTurnComplete,
        turnId: "pre-response-turn",
        duration: 5,
      },
      22,
    );
    await waitFor(
      () => binding.snapshot().defaultChat?.activeTurn === undefined,
    );
    const actionCount = actions.length;
    await server.emitAction(
      chatB,
      { type: "chat/unknown" },
      23,
    );
    await waitFor(() => errors.length > 0);
    assert.equal(actions.length, actionCount);

    const firstPromise = binding.queueUserText("start now");
    const secondPromise = binding.queueUserText("queue next");
    const [first, second] = await Promise.all([
      firstPromise,
      secondPromise,
    ]);
    assert.equal(first.disposition, "started");
    assert.equal(first.id, "new-turn");
    assert.equal(second.disposition, "queued");
    assert.equal(second.id, "queued-message");
    await waitFor(
      () =>
        binding.snapshot().defaultChat?.activeTurn?.id === "new-turn",
    );

    monotonicNow = 575;
    const cancelled = await binding.cancelActiveTurn();
    assert.equal(cancelled.turnId, "new-turn");

    await assert.rejects(
      binding.reviewToolParameters({
        requestId: "parameter-request",
        decision: "approve",
        selectedOptionId: "deny-once",
      }),
      (error: unknown) =>
        error instanceof AhpOperationError &&
        error.code === "invalid-confirmation-option",
    );
    await binding.reviewToolParameters({
      requestId: "parameter-request",
      decision: "approve",
      editedToolInput: "{\"path\":\"b\"}",
      selectedOptionId: "approve-once",
    });
    await binding.reviewToolParameters({
      requestId: "parameter-request",
      decision: "deny",
      reasonMessage: "Not now",
      selectedOptionId: "deny-once",
    });
    await binding.reviewToolResult({
      requestId: "result-request",
      approved: false,
    });
    await binding.completeCurrentInput({
      response: ChatInputResponseKind.Accept,
      answers: {
        answer: {
          state: ChatInputAnswerState.Submitted,
          value: {
            kind: ChatInputAnswerValueKind.Text,
            value: "chosen",
          },
        },
      },
    });

    await waitFor(() => server.dispatches.length >= 7);
    const dispatched = server.dispatches.map(dispatchDetails);
    assert.equal(
      dispatched[0]?.actionType,
      ActionType.ChatTurnStarted,
    );
    assert.equal(
      dispatched[1]?.actionType,
      ActionType.ChatPendingMessageSet,
    );
    assert.equal(
      dispatched[2]?.actionType,
      ActionType.ChatTurnCancelled,
    );
    assert.equal(dispatched[2]?.duration, 75);
    assert.equal(dispatched[3]?.channel, secondaryChat);
    assert.equal(
      dispatched[3]?.actionType,
      ActionType.ChatToolCallConfirmed,
    );
    assert.equal(dispatched[4]?.channel, secondaryChat);
    assert.equal(
      dispatched[5]?.actionType,
      ActionType.ChatToolCallResultConfirmed,
    );
    assert.equal(dispatched[6]?.channel, chatB);
    assert.equal(
      dispatched[6]?.actionType,
      ActionType.ChatInputCompleted,
    );
    assert.ok(sessionSnapshots.length >= 2);
    assert.ok(
      chatSnapshots.some((event) => event.chatUri === chatA),
    );
    assert.ok(
      chatSnapshots.some((event) => event.chatUri === chatB),
    );
  } finally {
    await core.stop();
    await server.done;
  }
});

function fakeEndpoint(
  fileCharacter: string,
  instanceId: string,
  connectionToken: string,
  pipePath: string,
  protocolVersion = "0.9.0",
): EndpointRegistryEntry {
  return {
    schemaVersion: 2,
    type: "editor",
    pid: process.pid,
    instanceId,
    protocolVersion,
    connectionToken,
    endpoint: { type: "socket", path: pipePath },
    sourceFile: `C:\\registry\\${fileCharacter.repeat(64)}.json`,
  };
}

function summary(resource: URI, title: string): SessionSummary {
  return {
    resource,
    provider: "copilot",
    title,
    status: SessionStatus.Idle,
    createdAt: "2026-08-28T00:00:00.000Z",
    modifiedAt: "2026-08-28T00:00:00.000Z",
  };
}

function chatSummary(resource: URI): SessionState["chats"][number] {
  return {
    resource,
    title: resource,
    status: SessionStatus.Idle,
    modifiedAt: "2026-08-28T00:00:00.000Z",
  };
}

function decodeFrame(frame: TransportFrame): unknown {
  switch (frame.kind) {
    case "parsed":
      return frame.message;
    case "text":
      return JSON.parse(frame.text);
    case "binary":
      return JSON.parse(new TextDecoder().decode(frame.data));
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringProperty(
  record: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = record[key];
  return typeof value === "string" ? value : undefined;
}

function numberProperty(
  record: Record<string, unknown>,
  key: string,
): number | undefined {
  const value = record[key];
  return typeof value === "number" ? value : undefined;
}

function stringArrayProperty(
  record: Record<string, unknown>,
  key: string,
): string[] | undefined {
  const value = record[key];
  if (!Array.isArray(value)) {
    return undefined;
  }
  const strings: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") {
      return undefined;
    }
    strings.push(item);
  }
  return strings;
}

interface DispatchDetails {
  readonly channel?: string;
  readonly actionType?: string;
  readonly duration?: number;
}

function dispatchDetails(value: unknown): DispatchDetails {
  if (!isRecord(value)) {
    return {};
  }
  const action = isRecord(value.action) ? value.action : {};
  const channel = stringProperty(value, "channel");
  const actionType = stringProperty(action, "type");
  const duration = numberProperty(action, "duration");
  return {
    ...(channel ? { channel } : {}),
    ...(actionType ? { actionType } : {}),
    ...(duration !== undefined ? { duration } : {}),
  };
}

async function waitFor(
  condition: () => boolean,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) {
      throw new Error("timed out waiting for test condition");
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
  }
}
