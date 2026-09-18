import assert from "node:assert/strict";
import test from "node:test";

import {
  MessageKind,
  SessionInputRequestKind,
  SessionLifecycle,
  SessionStatus,
  ToolCallStatus,
  type ChatState,
  type SessionState,
  type SessionSummary,
  type URI,
} from "@microsoft/agent-host-protocol";

import {
  type AhpCoreLike,
  AdapterRuntime,
  type AdapterCommand,
  type BridgeBinding,
  type BridgeClientLike,
  type RegisterResult,
} from "../src/main.js";
import type {
  AhpCoreOptions,
  AhpSessionBinding,
  BoundSessionSnapshot,
  CatalogueSnapshot,
  ChatSnapshotEvent,
  ConnectionEvent,
  DomainActionEvent,
  ReviewToolParametersCommand,
  SessionSnapshotEvent,
} from "../src/ahp-core.js";
import type { BridgeRequest } from "../src/bridge-client.js";
import type { AdapterConfig } from "../src/config.js";
import {
  endpointPublicId,
  type EndpointRegistryEntry,
} from "../src/endpoint-registry.js";
import {
  managedTargetWorkspaceUri,
  type ConnectedManagedTarget,
  type ManagedTarget,
  type PreparedTarget,
} from "../src/managed-target.js";

interface LoggedRequest extends Record<string, unknown> {
  readonly operation: string;
}

interface FakePollResult {
  readonly commands: readonly AdapterCommand[];
  readonly authorization?: unknown;
}

const endpointId = "endpoint-1";
const hostInstanceId = "host-1";
const timestamp = "2026-09-02T00:00:00.000Z";

const config: AdapterConfig = {
  configPath: "C:\\config.toml",
  bridgePipePath: "\\\\.\\pipe\\qq-copilot-test",
  bridgeToken: "a".repeat(64),
  adapterId: "qq-adapter-test",
  userDataDirectory: "C:\\Users\\tester\\AppData\\Roaming\\Code\\User\\globalStorage",
  pollSeconds: 1,
  codeExecutable: "C:\\Program Files\\Microsoft VS Code\\bin\\code.cmd",
  authorizedTargets: [],
};

class FakeBridgeClient implements BridgeClientLike {
  readonly requests: LoggedRequest[] = [];

  readonly beforeCall = new Map<
    string,
    (request: LoggedRequest) => Promise<void> | void
  >();

  readonly #registration: RegisterResult;

  readonly #pendingPolls: Array<
    (result: FakePollResult) => void
  > = [];

  constructor(registration: RegisterResult) {
    this.#registration = registration;
  }

  async call<T>(request: BridgeRequest): Promise<T> {
    const logged = structuredClone(request) as LoggedRequest;
    this.requests.push(logged);
    await this.beforeCall.get(request.operation)?.(logged);
    switch (request.operation) {
      case "ahp_adapter_register":
        return this.#registration as T;
      case "ahp_poll_commands":
        return await new Promise<T>((resolve) => {
          this.#pendingPolls.push((result) => resolve(result as T));
        });
      case "ahp_catalog_replace":
      case "ahp_binding_ready":
      case "ahp_binding_failed":
      case "ahp_publish_events":
      case "ahp_ack_command":
        return { accepted: true } as T;
      default:
        throw new Error(`Unexpected bridge operation ${request.operation}`);
    }
  }

  resolvePoll(result: FakePollResult): void {
    const resolve = this.#pendingPolls.shift();
    assert.ok(resolve, "expected a pending poll");
    resolve(result);
  }

  get pendingPollCount(): number {
    return this.#pendingPolls.length;
  }
}

class FakeSessionBinding implements AhpSessionBinding {
  readonly endpointId = endpointId;

  readonly provider = "copilot";

  readonly sessionUri: URI;

  readonly #snapshot: BoundSessionSnapshot;

  readonly queueUserTextCalls: Array<{
    readonly text: string;
    readonly clientSeq: number | undefined;
  }> = [];

  readonly cancelActiveTurnCalls: Array<{
    readonly clientSeq: number | undefined;
    readonly expectedTurnId: string | undefined;
  }> = [];

  readonly reviewToolParametersCalls: ReviewToolParametersCommand[] = [];

  beforeQueueUserText: (() => Promise<void>) | undefined;

  closeCount = 0;

  constructor(snapshot: BoundSessionSnapshot) {
    this.#snapshot = snapshot;
    this.sessionUri = snapshot.sessionUri;
  }

  snapshot(): BoundSessionSnapshot {
    return structuredClone(this.#snapshot);
  }

  async queueUserText(
    text: string,
    clientSeq?: number,
  ): Promise<{ readonly disposition: "queued"; readonly id: string; readonly clientSeq: number }> {
    await this.beforeQueueUserText?.();
    this.queueUserTextCalls.push({ text, clientSeq });
    return {
      disposition: "queued",
      id: `message-${this.queueUserTextCalls.length}`,
      clientSeq: clientSeq ?? 0,
    };
  }

  async cancelActiveTurn(
    clientSeq?: number,
    expectedTurnId?: string,
  ): Promise<{ readonly turnId: string; readonly clientSeq: number }> {
    this.cancelActiveTurnCalls.push({ clientSeq, expectedTurnId });
    return { turnId: expectedTurnId ?? "turn-1", clientSeq: clientSeq ?? 0 };
  }

  async reviewToolParameters(command: ReviewToolParametersCommand): Promise<{ readonly clientSeq: number }> {
    this.reviewToolParametersCalls.push(structuredClone(command));
    return { clientSeq: 0 };
  }

  async reviewToolResult(): Promise<{ readonly clientSeq: number }> {
    return { clientSeq: 0 };
  }

  async completeCurrentInput(): Promise<{ readonly clientSeq: number }> {
    return { clientSeq: 0 };
  }

  async close(): Promise<void> {
    this.closeCount += 1;
  }
}

class FakeCore implements AhpCoreLike {
  catalogue: CatalogueSnapshot;

  beforeRefresh: (() => Promise<void>) | undefined;

  readonly #callbacks: NonNullable<AhpCoreOptions["callbacks"]>;

  readonly #bindings = new Map<URI, FakeSessionBinding>();

  readonly beforeBind = new Map<URI, () => Promise<void> | void>();

  readonly bindCalls: URI[] = [];

  startCount = 0;

  refreshCount = 0;

  stopCount = 0;

  constructor(
    options: AhpCoreOptions,
    catalogue: CatalogueSnapshot,
    bindings: readonly FakeSessionBinding[],
  ) {
    this.#callbacks = options.callbacks ?? {};
    this.catalogue = catalogue;
    for (const binding of bindings) {
      this.#bindings.set(binding.sessionUri, binding);
    }
  }

  async start(): Promise<CatalogueSnapshot> {
    this.startCount += 1;
    return this.catalogue;
  }

  async stop(): Promise<void> {
    this.stopCount += 1;
  }

  async refreshEndpoints(): Promise<CatalogueSnapshot> {
    this.refreshCount += 1;
    await this.beforeRefresh?.();
    return this.catalogue;
  }

  async bindSession(
    _endpointId: string,
    sessionUri: string,
  ): Promise<AhpSessionBinding> {
    this.bindCalls.push(sessionUri);
    await this.beforeBind.get(sessionUri)?.();
    const binding = this.#bindings.get(sessionUri);
    assert.ok(binding, `missing fake binding for ${sessionUri}`);
    return binding;
  }

  emitConnection(event: ConnectionEvent): void {
    this.#callbacks.onConnection?.(event);
  }

  emitCatalogue(snapshot: CatalogueSnapshot): void {
    this.#callbacks.onCatalogue?.(snapshot);
  }

  emitSessionSnapshot(event: SessionSnapshotEvent): void {
    this.#callbacks.onSessionSnapshot?.(event);
  }

  emitChatSnapshot(event: ChatSnapshotEvent): void {
    this.#callbacks.onChatSnapshot?.(event);
  }

  emitAction(event: DomainActionEvent): void {
    this.#callbacks.onAction?.(event);
  }
}

test("runtime restores every registered binding and publishes per binding", async () => {
  const first = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const second = bindingRecord(
    "binding-2",
    2,
    "copilot:/session-2",
    "ahp-chat://default/session-2",
  );
  const bridge = new FakeBridgeClient({
    bindings: [first, second],
    foreground_binding_id: first.binding_id,
  });
  const firstBinding = new FakeSessionBinding(
    boundSnapshot(first.session_uri, chatUriOf(first)),
  );
  const secondBinding = new FakeSessionBinding(
    boundSnapshot(second.session_uri, chatUriOf(second)),
  );
  let core: FakeCore | undefined;
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) => {
      core = new FakeCore(
        options,
        catalogue([
          summary(first.session_uri, "First"),
          summary(second.session_uri, "Second"),
        ]),
        [firstBinding, secondBinding],
      );
      return core;
    },
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 2,
    "expected both bindings to restore",
  );

  assert.deepEqual(core?.bindCalls, [first.session_uri, second.session_uri]);

  core?.emitChatSnapshot(chatSnapshot(first.session_uri, chatUriOf(first), 10));
  await waitFor(
    () => requestsFor(bridge, "ahp_publish_events").length >= 1,
    "expected a published chat snapshot",
  );
  assert.ok(
    requestsFor(bridge, "ahp_publish_events").some(
      (request) =>
        request.binding_id === first.binding_id &&
        publishedKinds(request).includes("chat_snapshot"),
    ),
  );

  core?.emitConnection({
    endpoint: publicEndpoint(),
    status: "disconnected",
  });
  await waitFor(
    () =>
      requestsFor(bridge, "ahp_publish_events").filter((request) =>
        publishedKinds(request).includes("host_disconnected"),
      ).length === 2,
    "expected one disconnect event per active binding",
  );
  assert.deepEqual(
    requestsFor(bridge, "ahp_publish_events")
      .filter((request) => publishedKinds(request).includes("host_disconnected"))
      .map((request) => String(request.binding_id))
      .sort(),
    [first.binding_id, second.binding_id].sort(),
  );

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;

  assert.equal(core?.startCount, 1);
  assert.equal(core?.stopCount, 1);
  assert.equal(firstBinding.closeCount, 1);
  assert.equal(secondBinding.closeCount, 1);
});

test("runtime serializes catalogue snapshots in observation order", async () => {
  const bridge = new FakeBridgeClient({ bindings: [] });
  let release: (() => void) | undefined;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let catalogueCalls = 0;
  bridge.beforeCall.set("ahp_catalog_replace", async () => {
    catalogueCalls += 1;
    if (catalogueCalls === 2) {
      await blocked;
    }
  });
  let core: FakeCore | undefined;
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) => {
      core = new FakeCore(options, catalogue([]), []);
      return core;
    },
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_catalog_replace").length === 1,
    "expected initial catalogue",
  );
  await waitFor(
    () => bridge.pendingPollCount === 1,
    "expected command polling",
  );

  core?.emitCatalogue(catalogue([summary("copilot:/first", "First")]));
  await waitFor(
    () => requestsFor(bridge, "ahp_catalog_replace").length === 2,
    "expected first callback catalogue",
  );
  core?.emitCatalogue(catalogue([summary("copilot:/second", "Second")]));
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(requestsFor(bridge, "ahp_catalog_replace").length, 2);

  release?.();
  await waitFor(
    () => requestsFor(bridge, "ahp_catalog_replace").length === 3,
    "expected queued callback catalogue",
  );
  const catalogues = requestsFor(bridge, "ahp_catalog_replace");
  const firstCallback = catalogues.at(1);
  const secondCallback = catalogues.at(2);
  assert.ok(firstCallback);
  assert.ok(secondCallback);
  assert.ok(Array.isArray(firstCallback.sessions));
  assert.ok(Array.isArray(secondCallback.sessions));
  const firstSession = firstCallback.sessions.at(0) as
    | Record<string, unknown>
    | undefined;
  const secondSession = secondCallback.sessions.at(0) as
    | Record<string, unknown>
    | undefined;
  assert.ok(firstSession);
  assert.ok(secondSession);
  assert.equal(
    firstSession.session_uri,
    "copilot:/first",
  );
  assert.equal(
    secondSession.session_uri,
    "copilot:/second",
  );

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
});

test("runtime publishes each Session from one stable connected endpoint", async () => {
  const bridge = new FakeBridgeClient({ bindings: [] });
  let core: FakeCore | undefined;
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) => {
      core = new FakeCore(options, catalogue([]), []);
      return core;
    },
  });
  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => bridge.pendingPollCount === 1,
    "expected command polling",
  );

  const duplicate = summary("copilot:/shared", "Shared");
  core?.emitCatalogue({
    revision: 2,
    endpoints: [
      {
        endpoint: {
          id: "endpoint-z",
          pid: 3,
          instanceId: "host-z",
          advertisedProtocol: "0.9.0",
        },
        connection: "connected",
        selectedProtocol: "0.9.0",
        sessions: [duplicate],
      },
      {
        endpoint: {
          id: "endpoint-0",
          pid: 1,
          instanceId: "host-0",
          advertisedProtocol: "0.9.0",
        },
        connection: "disconnected",
        sessions: [duplicate],
      },
      {
        endpoint: {
          id: "endpoint-a",
          pid: 2,
          instanceId: "host-a",
          advertisedProtocol: "0.9.0",
        },
        connection: "connected",
        selectedProtocol: "0.9.0",
        sessions: [duplicate],
      },
    ],
  });
  await waitFor(
    () => requestsFor(bridge, "ahp_catalog_replace").length >= 2,
    "expected duplicate catalogue publication",
  );
  const published = requestsFor(bridge, "ahp_catalog_replace").at(-1);
  assert.ok(published);
  assert.ok(Array.isArray(published.sessions));
  assert.equal(published.sessions.length, 1);
  assert.equal(
    (published.sessions[0] as Record<string, unknown>).endpoint_id,
    "endpoint-a",
  );

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
});

test("runtime routes pending and active bindings by binding id and generation", async () => {
  const first = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const second = bindingRecord(
    "binding-2",
    2,
    "copilot:/session-2",
    "ahp-chat://default/session-2",
    "binding",
  );
  const bridge = new FakeBridgeClient({ bindings: [first] });
  const firstBinding = new FakeSessionBinding(
    boundSnapshot(first.session_uri, chatUriOf(first), "turn-active"),
  );
  const secondBinding = new FakeSessionBinding(
    boundSnapshot(second.session_uri, chatUriOf(second)),
  );
  let core: FakeCore | undefined;
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) => {
      core = new FakeCore(
        options,
        catalogue([
          summary(first.session_uri, "First"),
          summary(second.session_uri, "Second"),
        ]),
        [firstBinding, secondBinding],
      );
      core.beforeBind.set(second.session_uri, async () => {
        core?.emitChatSnapshot(
          chatSnapshot(first.session_uri, chatUriOf(first), 20, "turn-active"),
        );
        core?.emitSessionSnapshot(
          sessionSnapshot(second.session_uri, chatUriOf(second), 21),
        );
        core?.emitChatSnapshot(
          chatSnapshot(second.session_uri, chatUriOf(second), 22),
        );
      });
      return core;
    },
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 1,
    "expected the first binding to restore",
  );

  bridge.resolvePoll({
    commands: [
      {
        command_id: 1,
        command_key: "bind:binding-2",
        binding_id: second.binding_id,
        binding_generation: second.generation,
        kind: "bind_session",
        data: {
          endpoint_id: second.endpoint_id,
          host_instance_id: hostInstanceId,
          session_uri: second.session_uri,
        },
      },
      {
        command_id: 2,
        command_key: "send:binding-1",
        binding_id: first.binding_id,
        binding_generation: first.generation,
        kind: "send_message",
        data: {
          content: "hello from binding 1",
        },
      },
      {
        command_id: 3,
        command_key: "cancel:binding-1",
        binding_id: first.binding_id,
        binding_generation: first.generation,
        kind: "cancel_turn",
        data: { turn_id: "turn-from-send-ack" },
      },
      {
        command_id: 4,
        command_key: "unbind:binding-1",
        binding_id: first.binding_id,
        binding_generation: first.generation,
        kind: "unbind_session",
        data: {},
      },
    ],
  });

  await waitFor(
    () =>
      requestsFor(bridge, "ahp_ack_command").length === 4 &&
      requestsFor(bridge, "ahp_binding_ready").length === 2,
    "expected all commands to be acked",
  );

  const secondReady = requestsFor(bridge, "ahp_binding_ready").find(
    (request) => request.binding_id === second.binding_id,
  );
  assert.ok(secondReady);
  assert.equal(secondReady.last_server_sequence, 22);

  assert.deepEqual(firstBinding.queueUserTextCalls, [
    {
      text: "hello from binding 1",
      clientSeq: 2,
    },
  ]);
  assert.deepEqual(firstBinding.cancelActiveTurnCalls, [
    {
      clientSeq: 3,
      expectedTurnId: "turn-from-send-ack",
    },
  ]);
  assert.deepEqual(secondBinding.queueUserTextCalls, []);
  assert.deepEqual(
    requestsFor(bridge, "ahp_ack_command").map((request) => request.outcome),
    ["applied", "applied", "applied", "applied"],
  );
  assert.ok(
    requestsFor(bridge, "ahp_publish_events").some(
      (request) =>
        request.binding_id === first.binding_id &&
        publishedKinds(request).includes("chat_snapshot"),
    ),
  );
  assert.ok(
    requestsFor(bridge, "ahp_publish_events").some(
      (request) =>
        request.binding_id === second.binding_id &&
        publishedKinds(request).includes("session_snapshot"),
    ),
  );
  assert.ok(
    requestsFor(bridge, "ahp_publish_events")
      .filter((request) => request.binding_id === second.binding_id)
      .every((request) =>
        publishedEvents(request).every(
          (event) =>
            typeof event.chat_uri !== "string" || event.chat_uri === second.chat_uri,
        ),
      ),
  );

  assert.equal(firstBinding.closeCount, 1);
  assert.equal(secondBinding.closeCount, 0);

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;

  assert.equal(firstBinding.closeCount, 1);
  assert.equal(secondBinding.closeCount, 1);
  assert.equal(core?.stopCount, 1);
});

test("runtime executes commands for different bindings concurrently", async () => {
  const first = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const second = bindingRecord(
    "binding-2",
    1,
    "copilot:/session-2",
    "ahp-chat://default/session-2",
  );
  const bridge = new FakeBridgeClient({ bindings: [first, second] });
  const firstBinding = new FakeSessionBinding(
    boundSnapshot(first.session_uri, chatUriOf(first)),
  );
  const secondBinding = new FakeSessionBinding(
    boundSnapshot(second.session_uri, chatUriOf(second)),
  );
  let releaseFirst: (() => void) | undefined;
  const firstBlocked = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  firstBinding.beforeQueueUserText = async () => {
    await firstBlocked;
  };
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) =>
      new FakeCore(
        options,
        catalogue([
          summary(first.session_uri, "First"),
          summary(second.session_uri, "Second"),
        ]),
        [firstBinding, secondBinding],
      ),
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 2,
    "expected both bindings to restore",
  );

  bridge.resolvePoll({
    commands: [
      {
        command_id: 1,
        command_key: "send:binding-1",
        binding_id: first.binding_id,
        binding_generation: first.generation,
        kind: "send_message",
        data: { content: "blocked" },
      },
      {
        command_id: 2,
        command_key: "send:binding-2",
        binding_id: second.binding_id,
        binding_generation: second.generation,
        kind: "send_message",
        data: { content: "not blocked" },
      },
    ],
  });

  await waitFor(
    () => secondBinding.queueUserTextCalls.length === 1,
    "expected the second binding command to complete independently",
  );
  assert.equal(firstBinding.queueUserTextCalls.length, 0);
  assert.equal(requestsFor(bridge, "ahp_ack_command").length, 1);
  assert.equal(
    requestsFor(bridge, "ahp_ack_command")[0]?.command_id,
    2,
  );

  releaseFirst?.();
  await waitFor(
    () => requestsFor(bridge, "ahp_ack_command").length === 2,
    "expected the blocked binding command to finish",
  );

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
});

test("runtime preserves command order within one binding", async () => {
  const record = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const bridge = new FakeBridgeClient({ bindings: [record] });
  const sessionBinding = new FakeSessionBinding(
    boundSnapshot(record.session_uri, chatUriOf(record)),
  );
  let release: (() => void) | undefined;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let invocations = 0;
  sessionBinding.beforeQueueUserText = async () => {
    invocations += 1;
    if (invocations === 1) {
      await blocked;
    }
  };
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) =>
      new FakeCore(
        options,
        catalogue([summary(record.session_uri, "First")]),
        [sessionBinding],
      ),
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 1,
    "expected binding restore",
  );
  bridge.resolvePoll({
    commands: [
      {
        command_id: 1,
        command_key: "send:first",
        binding_id: record.binding_id,
        binding_generation: record.generation,
        kind: "send_message",
        data: { content: "first" },
      },
      {
        command_id: 2,
        command_key: "send:second",
        binding_id: record.binding_id,
        binding_generation: record.generation,
        kind: "send_message",
        data: { content: "second" },
      },
    ],
  });

  await waitFor(() => invocations === 1, "expected first command to begin");
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(invocations, 1);
  release?.();
  await waitFor(
    () => sessionBinding.queueUserTextCalls.length === 2,
    "expected both commands",
  );
  assert.deepEqual(
    sessionBinding.queueUserTextCalls.map((call) => call.text),
    ["first", "second"],
  );

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
});

test("runtime does not enqueue a leased command twice while it is in flight", async () => {
  const record = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const bridge = new FakeBridgeClient({ bindings: [record] });
  const sessionBinding = new FakeSessionBinding(
    boundSnapshot(record.session_uri, chatUriOf(record)),
  );
  let release: (() => void) | undefined;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let invocations = 0;
  sessionBinding.beforeQueueUserText = async () => {
    invocations += 1;
    await blocked;
  };
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) =>
      new FakeCore(
        options,
        catalogue([summary(record.session_uri, "First")]),
        [sessionBinding],
      ),
  });

  const command: AdapterCommand = {
    command_id: 1,
    command_key: "send:binding-1",
    binding_id: record.binding_id,
    binding_generation: record.generation,
    kind: "send_message",
    data: { content: "once" },
  };
  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 1,
    "expected binding restore",
  );

  bridge.resolvePoll({ commands: [command] });
  await waitFor(() => invocations === 1, "expected command execution to begin");
  await waitFor(
    () => bridge.pendingPollCount === 1,
    "expected another command poll while the first command runs",
  );
  bridge.resolvePoll({ commands: [command] });
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(invocations, 1);

  release?.();
  await waitFor(
    () => requestsFor(bridge, "ahp_ack_command").length === 1,
    "expected one command acknowledgement",
  );
  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
});

test("runtime retries an applied acknowledgement without replaying the command", async () => {
  const record = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const bridge = new FakeBridgeClient({ bindings: [record] });
  const sessionBinding = new FakeSessionBinding(
    boundSnapshot(record.session_uri, chatUriOf(record)),
  );
  let firstAck = true;
  bridge.beforeCall.set("ahp_ack_command", () => {
    if (firstAck) {
      firstAck = false;
      throw new Error("simulated lost ACK response");
    }
  });
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) =>
      new FakeCore(
        options,
        catalogue([summary(record.session_uri, "First")]),
        [sessionBinding],
      ),
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 1,
    "expected binding restore",
  );
  bridge.resolvePoll({
    commands: [
      {
        command_id: 1,
        command_key: "send:binding-1",
        binding_id: record.binding_id,
        binding_generation: record.generation,
        kind: "send_message",
        data: { content: "once" },
      },
    ],
  });

  await waitFor(
    () => requestsFor(bridge, "ahp_ack_command").length === 2,
    "expected the acknowledgement retry",
  );
  assert.equal(sessionBinding.queueUserTextCalls.length, 1);
  assert.deepEqual(
    requestsFor(bridge, "ahp_ack_command").map((request) => request.outcome),
    ["applied", "applied"],
  );

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
});

test("runtime acknowledges a command that finishes during shutdown", async () => {
  const record = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const bridge = new FakeBridgeClient({ bindings: [record] });
  const sessionBinding = new FakeSessionBinding(
    boundSnapshot(record.session_uri, chatUriOf(record)),
  );
  let release: (() => void) | undefined;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  sessionBinding.beforeQueueUserText = () => blocked;
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) =>
      new FakeCore(
        options,
        catalogue([summary(record.session_uri, "First")]),
        [sessionBinding],
      ),
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 1,
    "expected binding restore",
  );
  bridge.resolvePoll({
    commands: [
      {
        command_id: 1,
        command_key: "send:binding-1",
        binding_id: record.binding_id,
        binding_generation: record.generation,
        kind: "send_message",
        data: { content: "finish while stopping" },
      },
    ],
  });
  await waitFor(
    () => bridge.pendingPollCount === 1,
    "expected another command poll",
  );

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await new Promise((resolve) => setTimeout(resolve, 20));
  release?.();
  await runTask;
  const acknowledgements = requestsFor(bridge, "ahp_ack_command");
  assert.equal(acknowledgements.length, 1);
  assert.equal(acknowledgements[0]?.outcome, "applied");
});

test("runtime applies a duplicate bind command idempotently during an active turn", async () => {
  const record = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const bridge = new FakeBridgeClient({ bindings: [record] });
  const sessionBinding = new FakeSessionBinding(
    boundSnapshot(record.session_uri, chatUriOf(record), "turn-active"),
  );
  let core: FakeCore | undefined;
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) => {
      core = new FakeCore(
        options,
        catalogue([summary(record.session_uri, "First")]),
        [sessionBinding],
      );
      return core;
    },
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 1,
    "expected the binding to restore",
  );

  bridge.resolvePoll({
    commands: [
      {
        command_id: 1,
        command_key: "bind:binding-1",
        binding_id: record.binding_id,
        binding_generation: record.generation,
        kind: "bind_session",
        data: {
          endpoint_id: record.endpoint_id,
          host_instance_id: hostInstanceId,
          session_uri: record.session_uri,
        },
      },
    ],
  });
  await waitFor(
    () => requestsFor(bridge, "ahp_ack_command").length === 1,
    "expected the duplicate bind command to be acked",
  );

  assert.deepEqual(core?.bindCalls, [record.session_uri]);
  assert.equal(sessionBinding.closeCount, 0);
  assert.equal(requestsFor(bridge, "ahp_binding_ready").length, 2);
  assert.equal(requestsFor(bridge, "ahp_ack_command")[0]?.outcome, "applied");

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
  assert.equal(sessionBinding.closeCount, 1);
});

test("runtime coalesces buffered snapshots and flushes the latest before unbinding", async () => {
  const record = bindingRecord(
    "binding-1",
    1,
    "copilot:/session-1",
    "ahp-chat://default/session-1",
  );
  const bridge = new FakeBridgeClient({ bindings: [record] });
  const sessionBinding = new FakeSessionBinding(
    boundSnapshot(record.session_uri, chatUriOf(record)),
  );
  let core: FakeCore | undefined;
  const runtime = new AdapterRuntime(config, {
    createBridgeClient: () => bridge,
    createCore: (options) => {
      core = new FakeCore(
        options,
        catalogue([summary(record.session_uri, "First")]),
        [sessionBinding],
      );
      return core;
    },
  });

  const abort = new AbortController();
  const runTask = runtime.run(abort.signal);
  await waitFor(
    () => requestsFor(bridge, "ahp_binding_ready").length === 1,
    "expected the binding to restore",
  );

  let releasePublish: (() => void) | undefined;
  const publishBlocked = new Promise<void>((resolve) => {
    releasePublish = resolve;
  });
  bridge.beforeCall.set("ahp_publish_events", async () => {
    bridge.beforeCall.delete("ahp_publish_events");
    await publishBlocked;
  });

  core?.emitChatSnapshot(chatSnapshot(record.session_uri, chatUriOf(record), 10));
  await waitFor(
    () => requestsFor(bridge, "ahp_publish_events").length === 1,
    "expected the first event batch to begin publishing",
  );
  core?.emitChatSnapshot(chatSnapshot(record.session_uri, chatUriOf(record), 11));
  core?.emitChatSnapshot(chatSnapshot(record.session_uri, chatUriOf(record), 12));
  bridge.resolvePoll({
    commands: [
      {
        command_id: 1,
        command_key: "unbind:binding-1",
        binding_id: record.binding_id,
        binding_generation: record.generation,
        kind: "unbind_session",
        data: {},
      },
    ],
  });

  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(sessionBinding.closeCount, 0);
  assert.equal(requestsFor(bridge, "ahp_ack_command").length, 0);

  releasePublish?.();
  await waitFor(
    () =>
      requestsFor(bridge, "ahp_publish_events").length === 2 &&
      requestsFor(bridge, "ahp_ack_command").length === 1,
    "expected all events to publish before the unbind command completed",
  );
  assert.equal(sessionBinding.closeCount, 1);
  assert.equal(requestsFor(bridge, "ahp_ack_command")[0]?.outcome, "applied");
  assert.deepEqual(
    requestsFor(bridge, "ahp_publish_events").flatMap((request) =>
      publishedEvents(request).map((event) => event.server_sequence),
    ),
    [10, 12],
  );

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
  assert.equal(sessionBinding.closeCount, 1);
});

test("runtime discovers SSH catalogues without /new, shares aliases and matches every exact folder", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const first = sshTarget("remote", "/project-a");
  const second = sshTarget("remote", "/project-b");
  const harness = managedHarness([first, second, { kind: "local", path: "C:\\local" }]);
  const sessions = [
    workspaceSession("a", "file:///project-a"),
    workspaceSession("b", "vscode-remote://ssh-remote%2Bremote/project-b"),
    workspaceSession("outside", "file:///project-a/child"),
    workspaceSession("wrong-alias", "vscode-remote://ssh-remote+other/project-b"),
    workspaceSession("missing-workspace"),
  ];
  harness.sessions.set("remote", sessions);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await spinUntil(() => harness.core.catalogue.endpoints.length === 2);
    await spinUntil(() => latestFullSessions(harness.bridge).length === 2);
    assert.deepEqual(harness.connectCalls, [first]);
    assert.deepEqual(
      latestFullSessions(harness.bridge).map((session) => [
        session.session_uri, session.target_kind, session.target_path, session.ssh_alias,
      ]),
      [
        ["ahp-session:/a", "ssh", "/project-a", "remote"],
        ["ahp-session:/b", "ssh", "/project-b", "remote"],
      ],
    );
    assert.deepEqual(harness.core.bindCalls, []);
    assert.equal(requestsFor(harness.bridge, "ahp_binding_ready").length, 0);
    assert.deepEqual(harness.clientRequests, []);

    t.mock.timers.tick(120_000);
    await spinUntil(() => harness.bridge.pendingPollCount === 1);
    assert.equal(harness.stopped.length, 0, "idle discovery tunnel must be retained");
    assert.equal(harness.connectCalls.length, 1);

    harness.sessions.set("remote", [...sessions, workspaceSession("pc-created", "file:///project-b")]);
    await harness.core.refreshEndpoints();
    await spinUntil(() => latestFullSessions(harness.bridge).length === 3);
    assert.equal(latestFullSessions(harness.bridge)[2]?.target_path, "/project-b");
    assert.equal(harness.connectCalls.length, 1, "root updates reuse the Core connection");
    const catalogues = requestsFor(harness.bridge, "ahp_catalog_replace");
    assert.ok(catalogues.some((request) => request.full_snapshot === false));
    const allowed = new Map([
      ["ahp-session:/a", "/project-a"],
      ["ahp-session:/b", "/project-b"],
      ["ahp-session:/pc-created", "/project-b"],
    ]);
    for (const request of catalogues) {
      for (const session of request.sessions as Array<Record<string, unknown>>) {
        assert.ok(allowed.has(String(session.session_uri)), "non-target Sessions must never be published");
        assert.equal(session.target_kind, "ssh");
        assert.equal(session.target_path, allowed.get(String(session.session_uri)));
        assert.equal(session.ssh_alias, "remote");
      }
    }
    harness.sessions.set("remote", [
      workspaceSession("a", "file:///project-a/child"),
      workspaceSession("b", "vscode-remote://ssh-remote+other/project-b"),
      workspaceSession("pc-created", "file:///project-b"),
    ]);
    await harness.core.refreshEndpoints();
    await spinUntil(() => latestFullSessions(harness.bridge).length === 1);
    assert.equal(latestFullSessions(harness.bridge)[0]?.session_uri, "ahp-session:/pc-created");
  } finally {
    await stopManagedHarness(harness, runTask);
  }
  assert.equal(harness.stopped.length, 1);
  assert.equal(harness.closed.length, 0);
});

test("slow SSH restore/discovery does not block local bindings, polls, commands or another remote", async () => {
  const slow = sshTarget("slow", "/slow");
  const healthy = sshTarget("healthy", "/healthy");
  const local = bindingRecord("local", 1, "copilot:/local", "ahp-chat://local/default");
  const slowPrepared = preparedTarget(slow);
  const remote: BridgeBinding = {
    ...bindingRecord("remote", 2, "ahp-session:/slow", "ahp-chat://slow/default"),
    endpoint_id: slowPrepared.endpointId,
    host_instance_id: slowPrepared.entry.instanceId,
    target_kind: "ssh", target_path: slow.path, ssh_alias: slow.alias,
  };
  const localBinding = new FakeSessionBinding(boundSnapshot(local.session_uri, chatUriOf(local)));
  let releaseSlow!: () => void;
  const slowGate = new Promise<void>((resolve) => { releaseSlow = resolve; });
  const harness = managedHarness([slow, healthy], [local, remote], [localBinding]);
  harness.beforeConnect = async (target) => {
    if (target.kind === "ssh" && target.alias === "slow") {
      await slowGate;
    }
  };
  harness.sessions.set("healthy", [workspaceSession("healthy", "file:///healthy")]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => harness.bridge.pendingPollCount === 1, "local poll should start");
    await waitFor(
      () => requestsFor(harness.bridge, "ahp_binding_ready").length === 1 &&
        latestFullSessions(harness.bridge).some((session) => session.target_path === "/healthy"),
      "local restore and healthy remote should not wait for slow discovery",
    );
    harness.bridge.resolvePoll({ commands: [{
      command_id: 1, command_key: "send-local", binding_id: local.binding_id,
      binding_generation: local.generation, kind: "send_message", data: { content: "hello" },
    }] });
    await waitFor(() => localBinding.queueUserTextCalls.length === 1, "local command should execute");
    await waitFor(() => harness.bridge.pendingPollCount === 1, "polling should continue");
    harness.abort.abort();
    releaseSlow();
    harness.bridge.resolvePoll({ commands: [] });
    await runTask;
    assert.deepEqual(harness.core.bindCalls, [local.session_uri]);
    assert.equal(harness.closed.filter((entry) => entry.target === slow).length, 1);
    assert.equal(harness.stopped.length, 1, "only the retained healthy tunnel needs final cleanup");
    assert.equal(latestFullSessions(harness.bridge).some((session) => session.target_path === "/slow"), false);
  } finally {
    releaseSlow();
    await stopManagedHarness(harness, runTask);
  }
});

test("SSH discovery retries independently with capped backoff and reconnects lost idle tunnels", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const target = sshTarget("retry", "/retry");
  const harness = managedHarness([target]);
  let failing = true;
  harness.beforeConnect = async () => {
    if (failing) {
      throw Object.assign(new Error("must not log credentials or remote output"), { code: "ssh-identity-changed" });
    }
  };
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await spinUntil(() => harness.connectCalls.length === 1);
    await spinUntil(() => harness.bridge.pendingPollCount === 1);
    for (const delay of [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000]) {
      const attempts = harness.connectCalls.length;
      await spinUntil(() => true);
      t.mock.timers.tick(delay - 1);
      await flushMicrotasks();
      assert.equal(harness.connectCalls.length, attempts);
      t.mock.timers.tick(1);
      await spinUntil(() => harness.connectCalls.length === attempts + 1);
      await flushMicrotasks();
    }
    failing = false;
    harness.sessions.set("retry", [workspaceSession("recovered", "file:///retry")]);
    t.mock.timers.tick(60_000);
    await spinUntil(() => latestFullSessions(harness.bridge).length === 1);
    const attempts = harness.connectCalls.length;
    harness.offline.add("retry");
    await harness.core.refreshEndpoints();
    harness.offline.delete("retry");
    t.mock.timers.tick(30_000);
    await spinUntil(() => harness.connectCalls.length === attempts + 1);
    await spinUntil(() => latestFullSessions(harness.bridge).length === 1);
    assert.equal(harness.stopped.length, 1, "failed idle tunnel should be released once");
  } finally {
    await stopManagedHarness(harness, runTask);
  }
  assert.equal(harness.stopped.length, 2);
});

test("prepare reuses discovery ownership for another folder and rejects unauthorized target changes", async () => {
  const first = sshTarget("shared", "/first");
  const second = sshTarget("shared", "/second");
  const harness = managedHarness([first, second]);
  harness.sessions.set("shared", [workspaceSession("second", "file:///second")]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => latestFullSessions(harness.bridge).length === 1, "expected discovery");
    await waitFor(() => harness.disconnected.length === 1, "discovery releases the control client");
    const commands = [
      second,
      { ...second, path: "/unregistered" },
      { ...second, alias: "changed" },
      { ...second, user: "changed" },
      { ...second, hostKeyFingerprints: ["changed"] },
    ].map((target, index): AdapterCommand => ({
      command_id: index + 1, command_key: `prepare-${index}`, binding_id: "new",
      binding_generation: 0, kind: "prepare_target",
      data: { target: wireTarget(target), retain_connection: true },
    }));
    harness.bridge.resolvePoll({ commands });
    await waitFor(() => requestsFor(harness.bridge, "ahp_ack_command").length === commands.length, "expected prepare acknowledgements");
    assert.deepEqual(requestsFor(harness.bridge, "ahp_ack_command").map((request) => request.outcome),
      ["applied", "rejected", "rejected", "rejected", "rejected"]);
    assert.equal(harness.connectCalls.length, 1);
    assert.equal(harness.reconnectCalls.length, 1);
    assert.equal(harness.reconnectCalls[0]?.workspaceUri, "file:///second");
    assert.equal(harness.reconnectCalls[0]?.entry, harness.connected[0]?.entry);
    assert.equal(harness.stopped.length, 0);
    assert.equal(latestFullSessions(harness.bridge)[0]?.target_path, "/second");
    assert.deepEqual(harness.clientRequests, []);
    const prepared = harness.reconnectCalls[0]!;
    assert.deepEqual(requestsFor(harness.bridge, "ahp_ack_command")[0]?.result, {
      endpoint_id: prepared.endpointId,
      host_instance_id: prepared.entry.instanceId,
      provider: "copilotcli",
      workspace_uri: "file:///second",
      host_label: "ssh:shared",
      editor_client_tools_available: false,
    });
  } finally {
    await stopManagedHarness(harness, runTask);
  }
  assert.equal(harness.stopped.length, 1, "shared prepared ownership is stopped only once");
});

test("discovery and prepare reserve one shared Host while connection is pending", async () => {
  const first = sshTarget("shared", "/first");
  const second = sshTarget("shared", "/second");
  const harness = managedHarness([first, second]);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  harness.beforeConnect = () => gate;
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => harness.bridge.pendingPollCount === 1, "expected polling during discovery");
    harness.bridge.resolvePoll({ commands: [{
      command_id: 1, command_key: "prepare", binding_id: "new", binding_generation: 0,
      kind: "prepare_target", data: { target: wireTarget(second), retain_connection: true },
    }] });
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(harness.connectCalls.length, 1);
    release();
    await waitFor(() => requestsFor(harness.bridge, "ahp_ack_command").length === 1, "prepare should reuse discovery");
    assert.equal(requestsFor(harness.bridge, "ahp_ack_command")[0]?.outcome, "applied");
    assert.equal(harness.connectCalls.length, 1);
    assert.equal(harness.reconnectCalls.length, 1);
  } finally {
    release();
    await stopManagedHarness(harness, runTask);
  }
});

test("conflicting pinned identities for one alias fail closed without connecting", async () => {
  const first = sshTarget("conflict", "/first");
  const harness = managedHarness([first, { ...first, path: "/second", user: "different" }]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => harness.bridge.pendingPollCount === 1, "local polling should still start");
    assert.equal(harness.connectCalls.length, 0);
    assert.deepEqual(harness.core.bindCalls, []);
  } finally {
    await stopManagedHarness(harness, runTask);
  }
});

test("SSH restore reuses discovery across folders and preserves bound Hosts during outages", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const first = sshTarget("restored", "/first");
  const second = sshTarget("restored", "/second");
  const prepared = preparedTarget(second);
  const record: BridgeBinding = {
    ...bindingRecord("remote", 1, "ahp-session:/restored", "ahp-chat://restored/default"),
    endpoint_id: prepared.endpointId,
    host_instance_id: prepared.entry.instanceId,
    target_kind: "ssh", target_path: second.path, ssh_alias: second.alias,
  };
  const binding = new FakeSessionBinding(boundSnapshot(record.session_uri, chatUriOf(record), "active"));
  const harness = managedHarness([first, second], [record], [binding]);
  harness.sessions.set("restored", [workspaceSession("restored", "file:///second")]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await spinUntil(() => requestsFor(harness.bridge, "ahp_binding_ready").length === 1);
    assert.equal(harness.connectCalls.length, 1);
    assert.equal(harness.reconnectCalls.length, 0);
    assert.deepEqual(harness.core.bindCalls, [record.session_uri]);
    harness.offline.add("restored");
    await harness.core.refreshEndpoints();
    t.mock.timers.tick(120_000);
    await flushMicrotasks();
    assert.equal(harness.stopped.length, 0, "discovery cannot replace a bound Host");
    assert.equal(harness.connectCalls.length, 1);
    assert.equal(binding.closeCount, 0);
    assert.equal(requestsFor(harness.bridge, "ahp_binding_ready").length, 1);
  } finally {
    await stopManagedHarness(harness, runTask);
  }
  assert.equal(binding.closeCount, 1);
  assert.equal(harness.stopped.length, 1);
});

test("discovery tries another authorized folder when the first folder on a shared Host is unavailable", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const first = sshTarget("folders", "/removed");
  const second = sshTarget("folders", "/existing");
  const harness = managedHarness([first, second]);
  harness.beforeConnect = async (target) => {
    if (target.path === first.path) {
      throw new Error("target-directory-unavailable");
    }
  };
  harness.sessions.set("folders", [workspaceSession("existing", "file:///existing")]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await spinUntil(() => harness.bridge.pendingPollCount === 1);
    assert.deepEqual(harness.connectCalls, [first]);
    t.mock.timers.tick(1_000);
    await spinUntil(() => latestFullSessions(harness.bridge).length === 1);
    assert.deepEqual(harness.connectCalls, [first, second]);
    assert.equal(latestFullSessions(harness.bridge)[0]?.target_path, "/existing");
  } finally {
    await stopManagedHarness(harness, runTask);
  }
  assert.equal(harness.stopped.length, 1);
});

test("discovery fails closed if another alias resolves to an already retained endpoint", async () => {
  const first = sshTarget("original", "/first");
  const other = sshTarget("other", "/second");
  const harness = managedHarness([first, other]);
  const original = preparedTarget(first);
  harness.prepareTarget = (target) => ({
    ...preparedTarget(target),
    entry: original.entry,
    endpointId: original.endpointId,
  });
  harness.sessions.set("original", [workspaceSession("first", "file:///first")]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => harness.closed.length === 1, "duplicate endpoint tunnel should close");
    await waitFor(() => latestFullSessions(harness.bridge).length === 1, "original endpoint should remain visible");
    assert.equal(harness.closed[0]?.target, other);
    assert.equal(harness.core.catalogue.endpoints.length, 2, "Core receives only one managed endpoint");
    assert.equal(latestFullSessions(harness.bridge)[0]?.ssh_alias, "original");
    assert.ok(requestsFor(harness.bridge, "ahp_catalog_replace").every((request) =>
      (request.hosts as Array<Record<string, unknown>>).every((host) => host.ssh_alias !== "other"),
    ));
  } finally {
    await stopManagedHarness(harness, runTask);
  }
  assert.equal(harness.stopped.length, 1);
});

test("discovery does not retire a Host while a sibling-folder prepare operation is active", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const first = sshTarget("active", "/first");
  const second = sshTarget("active", "/second");
  const harness = managedHarness([first, second]);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  harness.beforeReconnect = () => gate;
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await spinUntil(() => harness.disconnected.length === 1);
    t.mock.timers.tick(15_000);
    harness.bridge.resolvePoll({ commands: [{
      command_id: 1, command_key: "prepare", binding_id: "new", binding_generation: 0,
      kind: "prepare_target", data: { target: wireTarget(second), retain_connection: true },
    }] });
    await spinUntil(() => harness.reconnectCalls.length === 1);
    harness.offline.add("active");
    await harness.core.refreshEndpoints();
    t.mock.timers.tick(15_000);
    await flushMicrotasks();
    assert.equal(harness.stopped.length, 0);
    assert.equal(harness.connectCalls.length, 1);
    release();
    await spinUntil(() => requestsFor(harness.bridge, "ahp_ack_command").length === 1);
    harness.beforeReconnect = undefined;
    harness.offline.delete("active");
    t.mock.timers.tick(30_000);
    await spinUntil(() => harness.connectCalls.length === 2);
    await spinUntil(() => harness.disconnected.length === 3);
    assert.equal(harness.stopped.length, 1, "operation count should release the original shared prepared entry");
  } finally {
    release();
    await stopManagedHarness(harness, runTask);
  }
});

test("stale create and dispose commands are rejected without creating a Host or binding", async () => {
  const target: ManagedTarget = { kind: "local", path: "C:\\trusted" };
  const harness = managedHarness([target]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => harness.bridge.pendingPollCount === 1, "expected command polling");
    const commands = ["create_session", "dispose_session"].map((kind, index) => ({
      command_id: index + 1,
      command_key: `stale-${kind}`,
      binding_id: "retired-creation",
      binding_generation: 0,
      kind,
      data: {
        target,
        provider: "copilotcli",
        session_uri: "ahp-session:/stale",
        workspace_uri: "file:///C:/trusted",
        resolved_values: {},
        overrides: {},
      },
    } as unknown as AdapterCommand));
    harness.bridge.resolvePoll({ commands });
    await waitFor(
      () => requestsFor(harness.bridge, "ahp_ack_command").length === commands.length,
      "expected explicit stale-command rejections",
    );
    const acknowledgements = requestsFor(harness.bridge, "ahp_ack_command");
    assert.deepEqual(acknowledgements.map((request) => request.outcome), ["rejected", "rejected"]);
    assert.deepEqual(acknowledgements.map((request) => request.error_code), ["invalid-command", "invalid-command"]);
    assert.deepEqual(harness.connectCalls, []);
    assert.deepEqual(harness.reconnectCalls, []);
    assert.deepEqual(harness.clientRequests, []);
    assert.deepEqual(harness.core.bindCalls, []);
    assert.deepEqual(harness.stopped, []);
    assert.equal(harness.bridge.pendingPollCount, 1, "rejected commands do not stop polling");
  } finally {
    await stopManagedHarness(harness, runTask);
  }
});

test("registration authorization replaces file trust before restoring existing bindings", async () => {
  const target = sshTarget("authoritative", "/current");
  const prepared = preparedTarget(target);
  const session = workspaceSession("restored-authoritative", "file:///current");
  const record: BridgeBinding = {
    ...bindingRecord("restored", 7, session.resource, "ahp-chat://restored/default"),
    endpoint_id: prepared.endpointId, host_instance_id: prepared.entry.instanceId,
    target_kind: "ssh", target_path: target.path, ssh_alias: target.alias,
  };
  const binding = new FakeSessionBinding(boundSnapshot(record.session_uri, chatUriOf(record)));
  const fileTarget = { ...target, user: "stale-file-identity" };
  const removed = sshTarget("removed-from-bridge", "/removed");
  const harness = managedHarness([fileTarget, removed], [record], [binding], {
    authorization: authorizationSnapshot("bridge-baseline", [target]),
  });
  harness.sessions.set(target.alias, [session]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => requestsFor(harness.bridge, "ahp_binding_ready").length === 1, "expected authoritative restoration");
    await waitFor(() => harness.bridge.pendingPollCount === 1, "expected polling");
    assert.deepEqual(harness.connectCalls, [target]);
    assert.deepEqual(harness.core.bindCalls, [record.session_uri]);
    assert.equal(requestsFor(harness.bridge, "ahp_poll_commands")[0]?.applied_authorization_revision, "bridge-baseline");
    harness.bridge.resolvePoll({ commands: [{
      command_id: 1, command_key: "removed-file-target", binding_id: "prepare", binding_generation: 0,
      kind: "prepare_target", data: { target: wireTarget(removed), retain_connection: true },
    }] });
    await waitFor(() => requestsFor(harness.bridge, "ahp_ack_command").length === 1, "expected removed file target rejection");
    assert.equal(requestsFor(harness.bridge, "ahp_ack_command")[0]?.outcome, "rejected");
    assert.deepEqual(harness.connectCalls, [target]);
    assert.equal(binding.closeCount, 0);
  } finally {
    await stopManagedHarness(harness, runTask);
  }
});

test("hot sibling authorization reprojects cached Sessions without reconnecting or disturbing approvals", async () => {
  const first = sshTarget("hot-shared", "/first");
  const second = sshTarget("hot-shared", "/second");
  const prepared = preparedTarget(first);
  const firstSession = workspaceSession("hot-first", "file:///first");
  const secondSession = workspaceSession("hot-second", "file:///second");
  const firstRecord: BridgeBinding = {
    ...bindingRecord("hot-first", 4, firstSession.resource, "ahp-chat://hot-first/default"),
    endpoint_id: prepared.endpointId, host_instance_id: prepared.entry.instanceId,
    target_kind: "ssh", target_path: first.path, ssh_alias: first.alias,
  };
  const firstBinding = new FakeSessionBinding(boundSnapshot(firstRecord.session_uri, chatUriOf(firstRecord), "still-running"));
  const secondBinding = new FakeSessionBinding(boundSnapshot(secondSession.resource, "ahp-chat://hot-second/default"));
  const harness = managedHarness([], [firstRecord], [firstBinding, secondBinding], {
    authorization: authorizationSnapshot("r1", [first]),
  });
  harness.sessions.set(first.alias, [firstSession, secondSession]);
  let releaseCatalogue: (() => void) | undefined;
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => requestsFor(harness.bridge, "ahp_binding_ready").length === 1 && harness.disconnected.length === 1, "expected retained active Host");
    await waitFor(() => harness.bridge.pendingPollCount === 1, "expected polling");
    const snapshot = sessionSnapshot(firstRecord.session_uri, chatUriOf(firstRecord), 20);
    harness.core.emitSessionSnapshot({
      ...snapshot,
      endpointId: prepared.endpointId,
      state: {
        ...snapshot.state,
        status: SessionStatus.InputNeeded,
        inputNeeded: [{
          id: "pending-before-reload", kind: SessionInputRequestKind.ToolConfirmation,
          chat: chatUriOf(firstRecord), turnId: "still-running",
          toolCall: {
            status: ToolCallStatus.PendingConfirmation, toolCallId: "pending-tool",
            toolName: "terminal", displayName: "Terminal", invocationMessage: "Run tests",
          },
        }],
      },
    });
    const approvals = () => requestsFor(harness.bridge, "ahp_publish_events")
      .flatMap(publishedEvents).filter((event) => event.kind === "approval_pending");
    await waitFor(() => approvals().length === 1, "expected pending approval");
    const pendingApproval = structuredClone(approvals()[0]);
    assert.deepEqual(latestFullSessions(harness.bridge).map((session) => session.session_uri), [firstSession.resource]);
    const refreshCount = harness.core.refreshCount;
    const retained = harness.connected[0];
    harness.core.beforeBind.set(secondSession.resource, () => {
      assert.equal(latestFullSessions(harness.bridge).some((session) => session.session_uri === secondSession.resource), true,
        "authorization projection must finish before commands from the same poll");
    });
    let publishing = false;
    const publication = new Promise<void>((resolve) => { releaseCatalogue = resolve; });
    harness.bridge.beforeCall.set("ahp_catalog_replace", async () => {
      publishing = true;
      await publication;
    });
    harness.bridge.resolvePoll({
      authorization: authorizationSnapshot("r2", [first, second]),
      commands: [
        { command_id: 1, command_key: "keep-sending", binding_id: firstRecord.binding_id,
          binding_generation: firstRecord.generation, kind: "send_message", data: { content: "original still works" } },
        { command_id: 2, command_key: "approve-existing", binding_id: firstRecord.binding_id,
          binding_generation: firstRecord.generation, kind: "approve_tool",
          data: { approval_key: "pending-before-reload", approved: true } },
        { command_id: 3, command_key: "switch-sibling", binding_id: "hot-second",
          binding_generation: 1, kind: "bind_session",
          data: { endpoint_id: prepared.endpointId, host_instance_id: prepared.entry.instanceId, session_uri: secondSession.resource } },
        { command_id: 4, command_key: "send-sibling", binding_id: "hot-second",
          binding_generation: 1, kind: "send_message", data: { content: "new sibling works" } },
      ],
    });
    await waitFor(() => publishing, "expected cached catalogue publication");
    assert.equal(requestsFor(harness.bridge, "ahp_poll_commands").at(-1)?.applied_authorization_revision, "r1");
    assert.equal(harness.bridge.pendingPollCount, 0, "revision acknowledgement must wait for catalogue acceptance");
    assert.equal(firstBinding.queueUserTextCalls.length, 0, "commands must wait for authorization application");
    releaseCatalogue?.();
    harness.bridge.beforeCall.delete("ahp_catalog_replace");
    await waitFor(() => requestsFor(harness.bridge, "ahp_ack_command").length === 4, "expected switch/send/approval success");
    assert.ok(requestsFor(harness.bridge, "ahp_ack_command").every((request) => request.outcome === "applied"));
    assert.equal(firstBinding.queueUserTextCalls[0]?.text, "original still works");
    assert.equal(secondBinding.queueUserTextCalls[0]?.text, "new sibling works");
    assert.deepEqual(firstBinding.reviewToolParametersCalls, [{ requestId: "pending-before-reload", decision: "approve" }]);
    assert.equal(firstBinding.snapshot().defaultChat?.activeTurn?.id, "still-running");
    assert.deepEqual(approvals(), [pendingApproval]);
    assert.deepEqual(harness.core.bindCalls, [firstSession.resource, secondSession.resource], "only the explicit sibling switch binds");
    for (const revision of ["r2", "r3", "r3"]) {
      await waitFor(() => harness.bridge.pendingPollCount === 1, "expected replay polling");
      harness.bridge.resolvePoll({
        authorization: authorizationSnapshot(revision, [second, first, first, second]),
        commands: [],
      });
      await waitFor(() => harness.bridge.pendingPollCount === 1, "expected replay completion");
      assert.equal(requestsFor(harness.bridge, "ahp_poll_commands").at(-1)?.applied_authorization_revision, revision);
    }
    assert.equal(latestFullSessions(harness.bridge).length, 2);
    assert.equal(harness.core.refreshCount, refreshCount, "hot authorization must not refresh endpoint connections");
    assert.deepEqual(harness.connectCalls, [first]);
    assert.deepEqual(harness.reconnectCalls, []);
    assert.equal(harness.connected.length, 1);
    assert.equal(harness.connected[0], retained);
    assert.equal(harness.stopped.length, 0);
    assert.equal(firstBinding.closeCount, 0);
    assert.equal(secondBinding.closeCount, 0);
  } finally {
    releaseCatalogue?.();
    await stopManagedHarness(harness, runTask);
  }
  assert.equal(harness.stopped.length, 1, "one canonical Host is cleaned up at shutdown");
});

test("root-only authorization revisions are opaque and acknowledged without reconnecting", async () => {
  const target = sshTarget("root-only", "/unchanged-target");
  const harness = managedHarness([], [], [], {
    authorization: authorizationSnapshot("f0a9", [target]),
  });
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => harness.disconnected.length === 1 && harness.bridge.pendingPollCount === 1, "expected retained Host");
    const refreshCount = harness.core.refreshCount;
    const retained = harness.connected[0];
    for (const revision of ["00ac", "00ac", "f0a9"]) {
      harness.bridge.resolvePoll({
        authorization: authorizationSnapshot(revision, [target]),
        commands: [],
      });
      await waitFor(() => harness.bridge.pendingPollCount === 1, "expected root-only revision acknowledgement");
      assert.equal(requestsFor(harness.bridge, "ahp_poll_commands").at(-1)?.applied_authorization_revision, revision);
    }
    assert.deepEqual(harness.connectCalls, [target]);
    assert.deepEqual(harness.reconnectCalls, []);
    assert.equal(harness.connected.length, 1);
    assert.equal(harness.connected[0], retained);
    assert.equal(harness.core.refreshCount, refreshCount);
    assert.deepEqual(harness.core.bindCalls, []);
    assert.equal(harness.stopped.length, 0);
  } finally {
    await stopManagedHarness(harness, runTask);
  }
});

test("hot authorization launches one independent worker for a new Host without blocking polls", async () => {
  const first = sshTarget("hot-existing", "/first");
  const added = sshTarget("hot-slow", "/second");
  const local = bindingRecord("live-local", 1, "copilot:/live-local", "ahp-chat://live-local/default");
  const binding = new FakeSessionBinding(boundSnapshot(local.session_uri, chatUriOf(local), "live-turn"));
  const harness = managedHarness([first], [local], [binding]);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  harness.beforeConnect = async (target) => {
    if (target.kind === "ssh" && target.alias === added.alias) {
      await gate;
    }
  };
  harness.sessions.set(added.alias, [workspaceSession("hot-added", "file:///second")]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => harness.disconnected.length === 1 && harness.bridge.pendingPollCount === 1, "expected static compatibility startup");
    assert.equal("applied_authorization_revision" in requestsFor(harness.bridge, "ahp_poll_commands")[0]!, false);
    for (let replay = 0; replay < 3; replay += 1) {
      harness.bridge.resolvePoll({
        authorization: authorizationSnapshot("added-host", [first, added]),
        commands: [{
          command_id: replay + 1, command_key: `send-during-discovery-${replay}`,
          binding_id: local.binding_id, binding_generation: local.generation,
          kind: "send_message", data: { content: "keep the current task running" },
        }],
      });
      await waitFor(
        () => harness.connectCalls.length === 2 && harness.bridge.pendingPollCount === 1 &&
          binding.queueUserTextCalls.length === replay + 1,
        "new Host must not block polling or current tasks",
      );
    }
    assert.deepEqual(harness.connectCalls, [first, added]);
    assert.equal(harness.disconnected.length, 1, "new Host is still pending");
    assert.equal(harness.stopped.length, 0);
    assert.equal(binding.closeCount, 0);
    assert.equal(binding.snapshot().defaultChat?.activeTurn?.id, "live-turn");
    release();
    await waitFor(() => latestFullSessions(harness.bridge).some((session) => session.target_path === added.path), "expected newly authorized Host catalogue");
    assert.equal(harness.disconnected.length, 2);
  } finally {
    release();
    await stopManagedHarness(harness, runTask);
  }
  assert.equal(harness.stopped.length, 2);
});

test("a poll applies new local authorization before executing prepare_target in the same batch", async () => {
  const local: ManagedTarget = { kind: "local", path: "C:\\new-local-target" };
  const harness = managedHarness([], [], [], {
    authorization: authorizationSnapshot("empty-baseline", []),
  });
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await waitFor(() => harness.bridge.pendingPollCount === 1, "expected first poll");
    harness.bridge.resolvePoll({
      authorization: authorizationSnapshot("local-added", [local]),
      commands: [{
        command_id: 1, command_key: "new-local-prepare", binding_id: "prepare", binding_generation: 0,
        kind: "prepare_target", data: { target: wireTarget(local), retain_connection: true },
      }],
    });
    await waitFor(() => requestsFor(harness.bridge, "ahp_ack_command").length === 1, "expected newly authorized command");
    assert.equal(requestsFor(harness.bridge, "ahp_ack_command")[0]?.outcome, "applied");
    assert.deepEqual(harness.connectCalls, [local]);
    assert.equal(requestsFor(harness.bridge, "ahp_poll_commands").at(-1)?.applied_authorization_revision, "local-added");
    assert.equal(harness.stopped.length, 0);
  } finally {
    await stopManagedHarness(harness, runTask);
  }
});

test("an existing discovery worker sees added sibling folders on its next retry", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const first = sshTarget("hot-retry", "/unavailable");
  const second = sshTarget("hot-retry", "/available");
  const harness = managedHarness([first]);
  harness.beforeConnect = async (target) => {
    if (target.path === first.path) {
      throw new Error("workspace-unavailable");
    }
  };
  harness.sessions.set(first.alias, [workspaceSession("available", "file:///available")]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await spinUntil(() => harness.connectCalls.length === 1 && harness.bridge.pendingPollCount === 1);
    harness.bridge.resolvePoll({ authorization: authorizationSnapshot("retry-addition", [first, second]), commands: [] });
    await spinUntil(() => harness.bridge.pendingPollCount === 1);
    assert.equal(harness.connectCalls.length, 1, "an addition does not launch a duplicate worker");
    t.mock.timers.tick(1_000);
    await spinUntil(() => harness.disconnected.length === 1);
    assert.deepEqual(harness.connectCalls, [first, second]);
    assert.equal(latestFullSessions(harness.bridge)[0]?.target_path, second.path);
    assert.equal(harness.stopped.length, 0);
  } finally {
    await stopManagedHarness(harness, runTask);
  }
});

test("malformed registration authorization fails before starting or restoring any Host", async () => {
  const first = sshTarget("invalid-registration", "/first");
  for (const authorization of [
    undefined, null, {}, { revision: "r1" }, { revision: "", authorized_targets: [] },
    { revision: "r1", authorized_targets: "not-an-array" },
    authorizationSnapshot("r1", [first, { ...first, path: "/second", user: "conflicting" }]),
  ]) {
    const harness = managedHarness([first], [], [], { authorization });
    try {
      await assert.rejects(harness.runtime.run(harness.abort.signal), { code: "invalid-authorization" });
      assert.equal(harness.core.startCount, 0);
      assert.deepEqual(harness.connectCalls, []);
      assert.deepEqual(harness.core.bindCalls, []);
      assert.equal(requestsFor(harness.bridge, "ahp_poll_commands").length, 0);
    } finally {
      await harness.runtime.stop();
    }
  }
});

test("bad hot snapshots reject their command batch and preserve last-good authorization", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const first = sshTarget("last-good", "/first");
  const second = sshTarget("last-good", "/second");
  const local: ManagedTarget = { kind: "local", path: "C:\\trusted" };
  const baseline = authorizationSnapshot("last-good-revision", [first, local]);
  const harness = managedHarness([], [], [], { authorization: baseline });
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await spinUntil(() => harness.disconnected.length === 1 && harness.bridge.pendingPollCount === 1);
    const invalid = [
      undefined, null, {}, { ...baseline, authorized_targets: null },
      { ...baseline, revision: 3 }, { ...baseline, revision: " " },
      { ...baseline, authorized_targets: [{ ...wireTarget(first), port: 0 }] },
      authorizationSnapshot("remove-local", [first]),
      authorizationSnapshot("remove-remote", [local]),
      authorizationSnapshot("change-user", [{ ...first, user: "other" }, local]),
      authorizationSnapshot("change-host", [{ ...first, host: "other" }, local]),
      authorizationSnapshot("change-port", [{ ...first, port: 2222 }, local]),
      authorizationSnapshot("change-fingerprint", [{ ...first, hostKeyFingerprints: ["SHA256:changed"] }, local]),
      authorizationSnapshot("conflicting-alias", [first, { ...second, host: "other" }, local]),
      authorizationSnapshot("last-good-revision", [first, second, local]),
    ];
    for (const [index, authorization] of invalid.entries()) {
      harness.bridge.resolvePoll({ authorization, commands: [{
        command_id: index + 1, command_key: `must-not-execute-${index}`, binding_id: "prepare", binding_generation: 0,
        kind: "prepare_target", data: { target: wireTarget(first), retain_connection: true },
      }] });
      await flushMicrotasks();
      t.mock.timers.tick(1_000);
      await spinUntil(() => harness.bridge.pendingPollCount === 1);
      assert.equal(requestsFor(harness.bridge, "ahp_poll_commands").at(-1)?.applied_authorization_revision, "last-good-revision");
      assert.equal(requestsFor(harness.bridge, "ahp_ack_command").length, 0);
      assert.deepEqual(harness.reconnectCalls, []);
      assert.equal(harness.stopped.length, 0);
    }
    harness.bridge.resolvePoll({ commands: [{
      command_id: 100, command_key: "still-not-authorized", binding_id: "prepare", binding_generation: 0,
      kind: "prepare_target", data: { target: wireTarget(second), retain_connection: true },
    }] });
    await spinUntil(() => requestsFor(harness.bridge, "ahp_ack_command").length === 1 && harness.bridge.pendingPollCount === 1);
    assert.equal(requestsFor(harness.bridge, "ahp_ack_command")[0]?.outcome, "rejected");
    harness.bridge.resolvePoll({ authorization: baseline, commands: [{
      command_id: 101, command_key: "last-good-still-works", binding_id: "prepare", binding_generation: 0,
      kind: "prepare_target", data: { target: wireTarget(first), retain_connection: true },
    }] });
    await spinUntil(() => requestsFor(harness.bridge, "ahp_ack_command").length === 2);
    assert.equal(requestsFor(harness.bridge, "ahp_ack_command")[1]?.outcome, "applied");
    assert.deepEqual(harness.connectCalls, [first]);
    assert.equal(harness.reconnectCalls.length, 1);
    assert.equal(harness.stopped.length, 0);
  } finally {
    await stopManagedHarness(harness, runTask);
  }
});

test("failed authorization catalogue publication retains the last-good revision and retries safely", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const first = sshTarget("publish-existing", "/first");
  const added = sshTarget("publish-new", "/added");
  const harness = managedHarness([], [], [], {
    authorization: authorizationSnapshot("published", [first]),
  });
  const update = authorizationSnapshot("pending-publication", [first, added]);
  const runTask = harness.runtime.run(harness.abort.signal);
  try {
    await spinUntil(() => harness.disconnected.length === 1 && harness.bridge.pendingPollCount === 1);
    harness.bridge.beforeCall.set("ahp_catalog_replace", () => {
      throw new Error("catalogue-publication-unavailable");
    });
    harness.bridge.resolvePoll({ authorization: update, commands: [{
      command_id: 1, command_key: "must-wait-for-authorization", binding_id: "prepare", binding_generation: 0,
      kind: "prepare_target", data: { target: wireTarget(added), retain_connection: true },
    }] });
    await flushMicrotasks();
    t.mock.timers.tick(1_000);
    await spinUntil(() => harness.bridge.pendingPollCount === 1);
    assert.equal(requestsFor(harness.bridge, "ahp_poll_commands").at(-1)?.applied_authorization_revision, "published");
    assert.equal(requestsFor(harness.bridge, "ahp_ack_command").length, 0);
    assert.deepEqual(harness.connectCalls, [first]);
    harness.bridge.beforeCall.delete("ahp_catalog_replace");
    harness.bridge.resolvePoll({ commands: [{
      command_id: 2, command_key: "not-yet-authorized", binding_id: "prepare", binding_generation: 0,
      kind: "prepare_target", data: { target: wireTarget(added), retain_connection: true },
    }] });
    await spinUntil(() => requestsFor(harness.bridge, "ahp_ack_command").length === 1 && harness.bridge.pendingPollCount === 1);
    assert.equal(requestsFor(harness.bridge, "ahp_ack_command")[0]?.outcome, "rejected");
    harness.bridge.resolvePoll({ authorization: update, commands: [] });
    await spinUntil(() => harness.disconnected.length === 2 && harness.bridge.pendingPollCount === 1);
    assert.deepEqual(harness.connectCalls, [first, added]);
    assert.equal(requestsFor(harness.bridge, "ahp_poll_commands").at(-1)?.applied_authorization_revision, "pending-publication");
    assert.equal(harness.stopped.length, 0);
  } finally {
    await stopManagedHarness(harness, runTask);
  }
});

function authorizationSnapshot(revision: string, targets: readonly ManagedTarget[]) {
  return { revision, authorized_targets: targets.map(wireTarget) };
}

function sshTarget(alias: string, path: string): Extract<ManagedTarget, { kind: "ssh" }> {
  return { kind: "ssh", alias, path, user: "root", host: alias, port: 22, hostKeyFingerprints: ["SHA256:pinned"] };
}

function wireTarget(target: ManagedTarget) {
  if (target.kind === "local") {
    return target;
  }
  const { hostKeyFingerprints, ...fields } = target;
  return { ...fields, host_key_fingerprints: hostKeyFingerprints };
}

function workspaceSession(id: string, uri?: string): SessionSummary {
  return { ...summary(`ahp-session:/${id}`, id), ...(uri ? { workingDirectories: [uri] } : {}) };
}

function preparedTarget(target: ManagedTarget): PreparedTarget {
  const alias = target.kind === "ssh" ? target.alias : "local";
  const entry: EndpointRegistryEntry = {
    schemaVersion: 2, type: "standalone", pid: 1234, instanceId: `host-${alias}`,
    protocolVersion: "0.1.0", wireProtocolVersion: "0.9.0", connectionToken: "not-logged",
    endpoint: { type: "tcp", host: "127.0.0.1", port: 9999 },
    sourceFile: `C:\\registry\\${alias}.json`,
  };
  return { target, entry, endpointId: endpointPublicId(entry), hostLabel: `ssh:${alias}`,
    workspaceUri: managedTargetWorkspaceUri(target), editorClientToolsAvailable: false };
}

function managedHarness(
  targets: readonly ManagedTarget[],
  registration: readonly BridgeBinding[] = [],
  bindings: readonly FakeSessionBinding[] = [],
  options: Pick<RegisterResult, "authorization"> = {},
) {
  const bridge = new FakeBridgeClient({ bindings: registration, ...options });
  let core!: FakeCore;
  const state = {
    bridge, abort: new AbortController(),
    sessions: new Map<string, readonly SessionSummary[]>(),
    offline: new Set<string>(),
    connectCalls: [] as ManagedTarget[],
    reconnectCalls: [] as PreparedTarget[],
    connected: [] as PreparedTarget[],
    disconnected: [] as PreparedTarget[],
    closed: [] as PreparedTarget[],
    stopped: [] as PreparedTarget[],
    clientRequests: [] as string[],
    prepareTarget: preparedTarget,
    beforeConnect: undefined as ((target: ManagedTarget) => Promise<void>) | undefined,
    beforeReconnect: undefined as ((prepared: PreparedTarget) => Promise<void>) | undefined,
  };
  const connection = (prepared: PreparedTarget): ConnectedManagedTarget => {
    state.connected.push(prepared);
    const alias = prepared.target.kind === "ssh" ? prepared.target.alias : "local";
    return {
      prepared,
      client: {
        request: async (method: string) => {
          state.clientRequests.push(method);
          assert.fail(`Unexpected managed request: ${method}`);
        },
      } as unknown as ConnectedManagedTarget["client"],
      rootState: { agents: [{ provider: "copilotcli", displayName: "Copilot", description: "", models: [] }] },
      sessions: state.sessions.get(alias) ?? [],
      releasePreparedOwnership() {},
      async disconnect() { state.disconnected.push(prepared); },
      async close() { state.closed.push(prepared); },
    };
  };
  const runtime = new AdapterRuntime({ ...config, authorizedTargets: targets }, {
    createBridgeClient: () => bridge,
    createCore: (options) => {
      core = new FakeCore(options, catalogue([]), bindings);
      core.beforeRefresh = async () => {
        const entries = await options.dependencies!.discoverEndpoints!(config.userDataDirectory);
        core.catalogue = {
          revision: core.catalogue.revision + 1,
          endpoints: [
            ...catalogue([]).endpoints,
            ...entries.map((entry): CatalogueSnapshot["endpoints"][number] => {
              const id = endpointPublicId(entry);
              return {
                endpoint: { id, instanceId: entry.instanceId, pid: entry.pid, advertisedProtocol: entry.protocolVersion },
                connection: state.offline.has(id) ? "disconnected" : "connected",
                selectedProtocol: entry.wireProtocolVersion ?? entry.protocolVersion,
                sessions: state.sessions.get(id) ?? [],
              };
            }),
          ],
        };
        core.emitCatalogue(core.catalogue);
        for (const endpoint of core.catalogue.endpoints) {
          core.emitConnection({ endpoint: endpoint.endpoint, status: endpoint.connection,
            ...(endpoint.selectedProtocol ? { selectedProtocol: endpoint.selectedProtocol } : {}) });
        }
      };
      return core;
    },
    connectManagedTarget: async (_config, target) => {
      state.connectCalls.push(target);
      await state.beforeConnect?.(target);
      return connection(state.prepareTarget(target));
    },
    connectPreparedManagedTarget: async (prepared) => {
      state.reconnectCalls.push(prepared);
      await state.beforeReconnect?.(prepared);
      return connection(prepared);
    },
    stopManagedTargetProcesses: async (prepared) => { state.stopped.push(prepared as PreparedTarget); },
  });
  return Object.assign(state, { runtime, core });
}

async function stopManagedHarness(harness: ReturnType<typeof managedHarness>, runTask: Promise<void>): Promise<void> {
  harness.abort.abort();
  if (harness.bridge.pendingPollCount > 0) {
    harness.bridge.resolvePoll({ commands: [] });
  }
  await runTask;
}

function latestFullSessions(bridge: FakeBridgeClient): Array<Record<string, unknown>> {
  return (requestsFor(bridge, "ahp_catalog_replace").filter((request) => request.full_snapshot).at(-1)?.sessions ?? []) as Array<Record<string, unknown>>;
}

async function flushMicrotasks(): Promise<void> {
  for (let count = 0; count < 20; count += 1) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}

async function spinUntil(predicate: () => boolean): Promise<void> {
  for (let count = 0; count < 2_000; count += 1) {
    if (predicate()) {
      await flushMicrotasks();
      return;
    }
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  assert.fail("runtime did not reach the expected state");
}

function bindingRecord(
  bindingId: string,
  generation: number,
  sessionUri: URI,
  chatUri: URI,
  state = "bound",
): BridgeBinding {
  return {
    binding_id: bindingId,
    generation,
    endpoint_id: endpointId,
    host_instance_id: hostInstanceId,
    session_uri: sessionUri,
    chat_uri: chatUri,
    state,
    last_server_sequence: 0,
  };
}

function chatUriOf(binding: BridgeBinding): URI {
  assert.ok(binding.chat_uri, `missing chat uri for ${binding.binding_id}`);
  return binding.chat_uri;
}

function boundSnapshot(
  sessionUri: URI,
  chatUri: URI,
  activeTurnId?: string,
): BoundSessionSnapshot {
  return {
    endpointId,
    sessionUri,
    provider: "copilot",
    defaultChat: chatState(chatUri, activeTurnId),
  };
}

function chatState(chatUri: URI, activeTurnId?: string): ChatState {
  return {
    resource: chatUri,
    title: chatUri,
    status: activeTurnId ? SessionStatus.InProgress : SessionStatus.Idle,
    modifiedAt: timestamp,
    turns: [],
    ...(activeTurnId
      ? {
          activeTurn: {
            id: activeTurnId,
            startedAt: timestamp,
            message: {
              text: "question",
              origin: { kind: MessageKind.User },
            },
            responseParts: [],
            usage: undefined,
          },
        }
      : {}),
  };
}

function sessionSnapshot(
  sessionUri: URI,
  chatUri: URI,
  serverSeq: number,
): SessionSnapshotEvent {
  const state: SessionState = {
    provider: "copilot",
    title: sessionUri,
    status: SessionStatus.Idle,
    lifecycle: SessionLifecycle.Ready,
    activeClients: [],
    chats: [chatSummary(chatUri)],
    defaultChat: chatUri,
    inputNeeded: [],
  };
  return {
    endpointId,
    sessionUri,
    provider: "copilot",
    serverSeq,
    state,
  };
}

function chatSnapshot(
  sessionUri: URI,
  chatUri: URI,
  serverSeq: number,
  activeTurnId?: string,
): ChatSnapshotEvent {
  return {
    endpointId,
    sessionUri,
    provider: "copilot",
    chatUri,
    serverSeq,
    state: chatState(chatUri, activeTurnId),
  };
}

function summary(resource: URI, title: string): SessionSummary {
  return {
    resource,
    provider: "copilot",
    title,
    status: SessionStatus.Idle,
    createdAt: timestamp,
    modifiedAt: timestamp,
  };
}

function chatSummary(resource: URI): SessionState["chats"][number] {
  return {
    resource,
    title: resource,
    status: SessionStatus.Idle,
    modifiedAt: timestamp,
  };
}

function publicEndpoint(): ConnectionEvent["endpoint"] {
  return {
    id: endpointId,
    pid: 1234,
    instanceId: hostInstanceId,
    advertisedProtocol: "0.9.0",
  };
}

function catalogue(sessions: readonly SessionSummary[]): CatalogueSnapshot {
  return {
    revision: 1,
    endpoints: [
      {
        endpoint: publicEndpoint(),
        connection: "connected",
        selectedProtocol: "0.9.0",
        sessions,
      },
    ],
  };
}

function requestsFor(
  bridge: FakeBridgeClient,
  operation: string,
): readonly LoggedRequest[] {
  return bridge.requests.filter((request) => request.operation === operation);
}

function publishedKinds(request: LoggedRequest): readonly string[] {
  return publishedEvents(request).map((event) => String(event.kind));
}

function publishedEvents(
  request: LoggedRequest,
): ReadonlyArray<Record<string, unknown>> {
  const events = request.events;
  assert.ok(Array.isArray(events), "expected published events");
  return events as Array<Record<string, unknown>>;
}

async function waitFor(
  predicate: () => boolean,
  message: string,
): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail(message);
}
