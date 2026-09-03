import assert from "node:assert/strict";
import test from "node:test";

import {
  MessageKind,
  SessionLifecycle,
  SessionStatus,
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
  SessionSnapshotEvent,
} from "../src/ahp-core.js";
import type { BridgeRequest } from "../src/bridge-client.js";
import type { AdapterConfig } from "../src/config.js";

interface LoggedRequest extends Record<string, unknown> {
  readonly operation: string;
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
    (result: { readonly commands: readonly AdapterCommand[] }) => void
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

  resolvePoll(result: {
    readonly commands: readonly AdapterCommand[];
  }): void {
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
  ): Promise<{ readonly turnId: string; readonly clientSeq: number }> {
    return { turnId: "turn-1", clientSeq: clientSeq ?? 0 };
  }

  async reviewToolParameters(): Promise<{ readonly clientSeq: number }> {
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
  readonly catalogue: CatalogueSnapshot;

  readonly #callbacks: NonNullable<AhpCoreOptions["callbacks"]>;

  readonly #bindings = new Map<URI, FakeSessionBinding>();

  readonly beforeBind = new Map<URI, () => Promise<void> | void>();

  readonly bindCalls: URI[] = [];

  startCount = 0;

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
      requestsFor(bridge, "ahp_ack_command").length === 3 &&
      requestsFor(bridge, "ahp_binding_ready").length === 2,
    "expected all commands to be acked",
  );

  const secondReady = requestsFor(bridge, "ahp_binding_ready").find(
    (request) => request.binding_id === second.binding_id,
  );
  assert.ok(secondReady);
  assert.equal(secondReady.last_server_sequence, 22);

  assert.deepEqual(firstBinding.queueUserTextCalls, [
    { text: "hello from binding 1", clientSeq: 2 },
  ]);
  assert.deepEqual(secondBinding.queueUserTextCalls, []);
  assert.deepEqual(
    requestsFor(bridge, "ahp_ack_command").map((request) => request.outcome),
    ["applied", "applied", "applied"],
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

test("runtime flushes every buffered event before unbinding", async () => {
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

  abort.abort();
  bridge.resolvePoll({ commands: [] });
  await runTask;
  assert.equal(sessionBinding.closeCount, 1);
});

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
