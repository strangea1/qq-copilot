import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { CODEX_PROTOCOL, CodexCore } from "../src/codex-core.js";
import { record, type CodexClient, type CodexMessage, type CodexRequestId } from "../src/codex-rpc.js";
import { AhpEventNormalizer, type PublishedEvent } from "../src/event-normalizer.js";
import { buildInputCompletion } from "../src/input-completion.js";
import { CombinedCore } from "../src/provider-core.js";

class FakeCodex implements CodexClient {
  readonly pid = process.pid;
  readonly calls: { method: string; params: unknown }[] = [];
  readonly replies: { id: CodexRequestId; result: unknown }[] = [];
  readonly rejected: CodexRequestId[] = [];
  readonly threads: Record<string, unknown>[];
  onMessage: (message: CodexMessage) => void = () => {};
  onClose: () => void = () => {};
  nextTurn = 0;
  closed = false;
  repeatedCursor = false;
  activeOnRead = false;
  resumeCwd: string | undefined;

  constructor(cwd: string) {
    this.threads = [this.thread("thread-a", cwd), this.thread("thread-b", cwd),
      this.thread("not-trusted", `${cwd}\\child`)];
  }

  thread(id: string, cwd: string) {
    return {
      id, cwd, name: id, preview: "Preview", createdAt: 1_780_000_000,
      updatedAt: 1_780_000_001, status: { type: "idle" }, turns: [],
    };
  }

  async request(method: string, params: unknown): Promise<unknown> {
    this.calls.push({ method, params });
    const data = params as Record<string, unknown>;
    switch (method) {
      case "initialize":
        return { userAgent: "codex-cli/0.163.0" };
      case "thread/list":
        return { data: this.threads, nextCursor: this.repeatedCursor ? "repeated" : null };
      case "thread/read":
      case "thread/resume": {
        const thread = this.threads.find((candidate) => candidate.id === data.threadId);
        assert.ok(thread);
        return {
          thread: { ...thread, status: { type: this.activeOnRead ? "active" : "idle" } },
          model: "saved-model", sandbox: { type: "workspaceWrite" }, approvalPolicy: "on-request",
          cwd: method === "thread/resume" ? this.resumeCwd ?? thread.cwd : thread.cwd,
        };
      }
      case "turn/start": {
        const turn = { id: `turn-${++this.nextTurn}`, status: "inProgress", startedAt: 1_780_000_010, items: [] };
        this.emit("turn/started", { threadId: data.threadId, turn });
        return { turn };
      }
      case "turn/interrupt":
      case "thread/unsubscribe":
        return {};
      default:
        throw new Error(`Unexpected Codex call: ${method}`);
    }
  }

  async notify(): Promise<void> {}
  async respond(id: CodexRequestId, result: unknown): Promise<void> {
    this.replies.push({ id, result });
  }
  async reject(id: CodexRequestId): Promise<void> {
    this.rejected.push(id);
  }
  async close(): Promise<void> { this.closed = true; }

  emit(method: string, params: unknown, id?: CodexRequestId): void {
    this.onMessage({ method, params, ...(id === undefined ? {} : { id }) });
  }
}

async function settle(): Promise<void> {
  for (let i = 0; i < 6; i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 3_000;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error("Timed out waiting for Codex event");
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

test("Codex uses existing trusted local threads, isolates bindings and routes replies/approvals", async (context) => {
  const cwd = await mkdtemp(join(tmpdir(), "qq-codex-test-"));
  const fake = new FakeCodex(cwd);
  const normalizers = new Map<string, AhpEventNormalizer>();
  const events: PublishedEvent[] = [];
  const errors: string[] = [];
  let trusted = true;
  const core = new CodexCore({
    executable: "C:\\Codex\\codex.exe", clientId: "test-adapter",
    authorizedTargets: () => trusted ? [{ kind: "local", path: cwd }] : [],
    createClient: async (_exe, onMessage, onClose) => {
      fake.onMessage = onMessage; fake.onClose = onClose; return fake;
    },
    pollIntervalMs: 60_000,
    callbacks: {
      onSessionSnapshot: (event) => {
        events.push(...(normalizers.get(event.sessionUri)?.sessionSnapshot(event) ?? []));
      },
      onChatSnapshot: (event) => {
        events.push(...(normalizers.get(event.sessionUri)?.chatSnapshot(event) ?? []));
      },
      onAction: (event) => {
        if (event.scope !== "root") {
          events.push(...(normalizers.get(event.sessionUri)?.action(event) ?? []));
        }
      },
      onError: (event) => errors.push(event.code),
    },
  });
  context.after(async () => { await core.stop(); await rm(cwd, { recursive: true, force: true }); });
  const catalogue = await core.start();
  const endpoint = catalogue.endpoints[0]!;
  assert.equal(endpoint.selectedProtocol, CODEX_PROTOCOL);
  assert.equal(endpoint.sessions.length, 2);
  assert.ok(endpoint.sessions.every((session) => session.title.startsWith("[Codex]")));
  for (const session of endpoint.sessions) {
    normalizers.set(session.resource, new AhpEventNormalizer({
      adapterId: "test-adapter", endpointId: endpoint.endpoint.id,
      hostInstanceId: endpoint.endpoint.instanceId, generation: 1,
      sessionUri: session.resource, chatUri: `${session.resource}/chat`,
    }));
  }
  const first = await core.bindSession(endpoint.endpoint.id, endpoint.sessions[0]!.resource);
  const second = await core.bindSession(endpoint.endpoint.id, endpoint.sessions[1]!.resource);
  assert.equal(first.snapshot().defaultChat?.turns.length, 0);
  assert.equal(events.some((event) => event.kind === "assistant_message"), false);
  const resume = fake.calls.find((call) => call.method === "thread/resume");
  assert.deepEqual(resume?.params, { threadId: "thread-a", excludeTurns: true });
  assert.equal(fake.calls.some((call) => call.method === "thread/start"), false);

  await first.queueUserText("Run first", 10);
  await settle();
  const queued = await first.queueUserText("Run queued", 11);
  assert.equal(queued.disposition, "queued");
  assert.deepEqual(await first.queueUserText("Run queued", 11), queued);
  assert.equal(first.snapshot().defaultChat?.queuedMessages?.length, 1);
  await second.queueUserText("Run second", 12);
  await settle();
  assert.equal(first.snapshot().defaultChat?.activeTurn?.id, "turn-1");
  assert.equal(second.snapshot().defaultChat?.activeTurn?.id, "turn-2");
  await assert.rejects(first.close(), { code: "binding-unavailable" });
  await assert.rejects(first.cancelActiveTurn(13, "turn-2"), { code: "stale-command" });
  await first.cancelActiveTurn(13, "turn-1");
  assert.deepEqual(fake.calls.at(-1), {
    method: "turn/interrupt", params: { threadId: "thread-a", turnId: "turn-1" },
  });

  const item = { id: "commentary", type: "agentMessage", text: "Working", phase: "commentary" };
  fake.emit("item/completed", { threadId: "thread-a", turnId: "turn-1", item });
  fake.emit("item/completed", { threadId: "thread-a", turnId: "turn-1", item });
  await settle();
  assert.equal(events.filter((event) =>
    event.kind === "assistant_message" && record(event.data).content === "Working").length, 1);

  fake.emit("item/commandExecution/requestApproval", {
    threadId: "thread-a", turnId: "turn-1", itemId: "command-1",
    command: "Get-ChildItem", cwd, reason: "Execute command",
  }, 100);
  await waitFor(() => (first.snapshot().session?.inputNeeded?.length ?? 0) > 0 || errors.length > 0);
  assert.deepEqual(errors, []);
  const approval = first.snapshot().session?.inputNeeded?.[0];
  assert.ok(approval);
  assert.ok(events.some((event) => event.kind === "approval_pending" &&
    record(event.data).approval_key === approval.id));
  fake.emit("item/started", {
    threadId: "thread-a", turnId: "turn-1",
    item: { id: "command-1", type: "commandExecution", command: "Get-ChildItem" },
  });
  await settle();
  assert.equal(first.snapshot().session?.inputNeeded?.[0]?.id, approval.id);
  assert.equal(fake.replies.length, 0, "tool notifications must not approve a pending request");
  assert.equal(second.snapshot().session?.inputNeeded?.length, 0);
  await assert.rejects(second.reviewToolParameters({
    requestId: approval.id, decision: "approve",
  }, 14), { code: "pending-tool-not-found" });
  await first.reviewToolParameters({ requestId: approval.id, decision: "approve" }, 14);
  assert.deepEqual(fake.replies.at(-1), { id: 100, result: { decision: "accept" } });
  assert.equal(first.snapshot().session?.inputNeeded?.length, 0);
  await assert.rejects(first.reviewToolParameters({
    requestId: approval.id, decision: "approve",
  }, 15), { code: "pending-tool-not-found" });

  fake.emit("item/fileChange/requestApproval", {
    threadId: "thread-b", turnId: "turn-2", itemId: "patch-1", reason: "Edit file",
  }, "100");
  await waitFor(() => (second.snapshot().session?.inputNeeded?.length ?? 0) > 0);
  const secondApproval = second.snapshot().session?.inputNeeded?.[0];
  assert.ok(secondApproval);
  await second.reviewToolParameters({ requestId: secondApproval.id, decision: "deny" }, 16);
  assert.deepEqual(fake.replies.at(-1), { id: "100", result: { decision: "decline" } });

  fake.emit("item/tool/requestUserInput", {
    threadId: "thread-a", turnId: "turn-1", itemId: "question-1",
    questions: [{
      id: "choice", question: "Choose", header: "Choice", isSecret: false,
      isOther: false, options: [{ label: "A", description: "First" }, { label: "B", description: "Second" }],
    }],
  }, 101);
  await waitFor(() => (first.snapshot().session?.inputNeeded?.length ?? 0) > 0);
  const question = first.snapshot().session?.inputNeeded?.[0];
  assert.ok(question);
  await first.completeCurrentInput(buildInputCompletion(first, question.id, "A"), 17);
  assert.deepEqual(fake.replies.at(-1), { id: 101, result: { answers: { choice: { answers: ["A"] } } } });
  const resolved = events.find((event) => event.kind === "input_resolved");
  assert.equal(record(resolved?.data).client_id, "test-adapter");
  fake.emit("serverRequest/resolved", { threadId: "thread-a", requestId: 101 });
  await settle();
  assert.equal(events.filter((event) => event.kind === "input_resolved").length, 1);

  fake.emit("item/tool/requestUserInput", {
    threadId: "thread-a", turnId: "turn-1", itemId: "secret",
    questions: [{ id: "secret", question: "Enter password", isSecret: true }],
  }, 102);
  await waitFor(() => fake.rejected.includes(102));
  assert.ok(fake.rejected.includes(102));
  assert.equal(first.snapshot().session?.inputNeeded?.length, 0);
  fake.emit("item/permissions/requestApproval", {
    threadId: "thread-a", turnId: "turn-1", itemId: "permission",
  }, 103);
  await waitFor(() => fake.rejected.includes(103));
  assert.ok(fake.rejected.includes(103));
  assert.ok(errors.length > 0);

  const finalItem = { id: "final", type: "agentMessage", text: "Done", phase: "final_answer" };
  fake.emit("item/completed", { threadId: "thread-a", turnId: "turn-1", item: finalItem });
  fake.emit("turn/completed", {
    threadId: "thread-a", turn: { id: "turn-1", status: "completed", durationMs: 10, items: [finalItem] },
  });
  await waitFor(() => first.snapshot().defaultChat?.activeTurn?.id === "turn-3");
  assert.equal(events.filter((event) =>
    event.kind === "assistant_message" && record(event.data).content === "Done" && record(event.data).final_response === true).length, 1);
  assert.equal(first.snapshot().defaultChat?.activeTurn?.id, "turn-3");
  assert.equal(second.snapshot().defaultChat?.activeTurn?.id, "turn-2");
  assert.equal(first.snapshot().defaultChat?.queuedMessages?.length ?? 0, 0);
  assert.equal(fake.calls.filter((call) => call.method === "turn/start").length, 3);
  trusted = false;
  await assert.rejects(first.queueUserText("Not allowed", 20), { code: "binding-unavailable" });
  fake.onClose();
  await settle();
  assert.equal(core.catalogue.endpoints[0]?.connection, "disconnected");
  assert.equal(fake.closed, true);
  assert.equal(fake.calls.filter((call) => call.method === "turn/start").length, 3);
});

test("Codex fails closed for busy PC threads and repeated pagination, without affecting AHP", async (context) => {
  const cwd = await mkdtemp(join(tmpdir(), "qq-codex-test-"));
  const fake = new FakeCodex(cwd);
  const errors: string[] = [];
  const core = new CodexCore({
    executable: "C:\\Codex\\codex.exe", clientId: "test",
    authorizedTargets: () => [{ kind: "local", path: cwd }],
    createClient: async () => fake,
    pollIntervalMs: 60_000,
    callbacks: { onError: (event) => errors.push(event.code) },
  });
  context.after(async () => { await core.stop(); await rm(cwd, { recursive: true, force: true }); });
  await core.start();
  fake.activeOnRead = true;
  const endpoint = core.catalogue.endpoints[0]!;
  await assert.rejects(core.bindSession(endpoint.endpoint.id, endpoint.sessions[0]!.resource),
    { code: "binding-unavailable" });
  assert.equal(fake.calls.some((call) => call.method === "thread/resume"), false);
  fake.repeatedCursor = true;
  await core.refreshEndpoints();
  assert.equal(core.catalogue.endpoints[0]?.connection, "disconnected");
  assert.ok(errors.includes("codex-repeated-thread-cursor"));

  const ahpCatalogue = { revision: 1, endpoints: [] };
  const combined = new CombinedCore([{
    catalogue: ahpCatalogue,
    start: async () => {},
    stop: async () => {},
    refreshEndpoints: async () => ahpCatalogue,
    bindSession: async () => { throw new Error("unreachable"); },
  }, core]);
  assert.equal(combined.catalogue.endpoints[0]?.endpoint.advertisedProtocol, CODEX_PROTOCOL);
  await assert.rejects(combined.bindSession("unknown-endpoint", "thread"), { code: "endpoint-not-found" });
});

test("Codex hot authorization reuses its process and reconnection never replays a turn", async (context) => {
  const firstCwd = await mkdtemp(join(tmpdir(), "qq-codex-trust-"));
  const secondCwd = await mkdtemp(join(tmpdir(), "qq-codex-trust-"));
  let paths = [firstCwd];
  const first = new FakeCodex(firstCwd);
  first.threads[1] = first.thread("thread-b", secondCwd);
  const second = new FakeCodex(firstCwd);
  let creates = 0;
  let releaseClose: (() => void) | undefined;
  const core = new CodexCore({
    executable: "C:\\Codex\\codex.exe", clientId: "test",
    authorizedTargets: () => paths.map((path) => ({ kind: "local", path })),
    createClient: async (_exe, onMessage, onClose) => {
      const client = ++creates === 1 ? first : second;
      client.onMessage = onMessage;
      client.onClose = onClose;
      return client;
    },
    pollIntervalMs: 60_000, callbacks: {},
  });
  context.after(async () => {
    releaseClose?.();
    await core.stop();
    await rm(firstCwd, { recursive: true, force: true });
    await rm(secondCwd, { recursive: true, force: true });
  });
  await core.start();
  const original = core.catalogue.endpoints[0]!;
  assert.equal(original.sessions.length, 1);
  paths = [firstCwd, secondCwd];
  await core.refreshEndpoints();
  assert.equal(core.catalogue.endpoints[0]?.sessions.length, 2);
  assert.equal(creates, 1, "appending trusted directories must reuse app-server");
  first.resumeCwd = secondCwd;
  await assert.rejects(core.bindSession(original.endpoint.id, original.sessions[0]!.resource),
    { code: "binding-unavailable" });
  assert.equal(first.calls.at(-1)?.method, "thread/unsubscribe");
  first.resumeCwd = undefined;
  const binding = await core.bindSession(original.endpoint.id, original.sessions[0]!.resource);
  await binding.queueUserText("Only once", 1);
  first.close = async () => {
    await new Promise<void>((resolve) => { releaseClose = resolve; });
    first.closed = true;
  };
  first.onClose();
  const reconnecting = core.refreshEndpoints();
  await settle();
  assert.equal(creates, 1, "wait for old process cleanup before reconnecting");
  assert.ok(releaseClose);
  releaseClose();
  await reconnecting;
  assert.equal(creates, 2);
  assert.equal(first.closed, true);
  assert.notEqual(core.catalogue.endpoints[0]?.endpoint.instanceId, original.endpoint.instanceId);
  await assert.rejects(binding.queueUserText("Stale", 2), { code: "binding-unavailable" });
  assert.equal(second.calls.some((call) => call.method === "turn/start"), false);
  const rebound = await core.bindSession(original.endpoint.id, original.sessions[0]!.resource);
  await rebound.queueUserText("Explicit new task", 3);
  assert.equal(second.calls.filter((call) => call.method === "turn/start").length, 1);
  first.onClose();
  first.emit("turn/completed", {
    threadId: "thread-a", turn: { id: "turn-1", status: "completed", items: [] },
  });
  await settle();
  assert.equal(core.catalogue.endpoints[0]?.connection, "connected");
  assert.equal(rebound.snapshot().defaultChat?.activeTurn?.id, "turn-1",
    "old process notifications must not finish the new instance's turn");
});
