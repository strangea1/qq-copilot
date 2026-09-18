const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const test = require("node:test");
const { promisify } = require("node:util");
const { runInNewContext } = require("node:vm");

const manifest = require("../package.json");

function statusResponse() {
  return {
    owner_bound: true,
    owner_enabled: true,
    qq_gateway: { state: "connected" },
    ahp: {
      adapter: { state: "connected", version: "2" },
      bindings: [
        { state: "bound", active_turn_id: "turn-1" },
        { state: "bound" },
      ],
      pending_commands: 2,
      pending_approvals: 3,
      pending_inputs: 1,
      pending_projections: 0,
    },
  };
}

function activateExtension(t, response = statusResponse()) {
  const commands = new Map();
  const events = {};
  const calls = [];
  const messages = [];
  const executedCommands = [];
  const timers = new Set();
  const subscriptions = [];
  const item = { show() {}, dispose() {} };
  const state = { response, requests: [] };
  const subscribe = (name) => (callback) => {
    events[name] = callback;
    return { dispose() { delete events[name]; } };
  };
  const workspace = {
    workspaceFolders: [],
    isTrusted: false,
    getConfiguration: () => ({
      get: (_key, fallback) => fallback,
      inspect: () => undefined,
    }),
    onDidGrantWorkspaceTrust: subscribe("trust"),
    onDidChangeWorkspaceFolders: subscribe("folders"),
    onDidChangeConfiguration: subscribe("configuration"),
  };
  const vscode = {
    workspace,
    StatusBarAlignment: { Right: 2 },
    ThemeColor: class {
      constructor(id) { this.id = id; }
    },
    MarkdownString: class {
      constructor(value) { this.value = value; }
    },
    window: {
      createStatusBarItem: () => item,
      showInformationMessage: async (message) => { messages.push(message); },
    },
    commands: {
      registerCommand: (name, callback) => {
        commands.set(name, callback);
        return { dispose() { commands.delete(name); } };
      },
      executeCommand: async (name) => { executedCommands.push(name); },
    },
  };
  const execFile = () => assert.fail("Expected promisified Bridge command");
  execFile[promisify.custom] = async (_file, args) => {
    const commandArgs = Array.from(args).slice(2);
    calls.push(commandArgs);
    let result;
    switch (commandArgs[0]) {
      case "status":
        if (state.response instanceof Error) {
          throw state.response;
        }
        result = state.response;
        break;
      case "report-trust":
        result = { updated: 1 };
        break;
      case "trust-requests":
        result = { requests: state.requests };
        break;
      default:
        assert.fail(`Unexpected Bridge command: ${commandArgs[0]}`);
    }
    return { stdout: JSON.stringify(result) };
  };
  const dependencies = new Map([
    ["node:child_process", { execFile }],
    ["node:path", require("node:path")],
    ["node:util", require("node:util")],
    ["vscode", vscode],
    ["./trust-helpers.js", require("../dist/trust-helpers.js")],
  ]);
  const exports = {};
  runInNewContext(
    readFileSync(join(__dirname, "..", "dist", "extension.js"), "utf8"),
    {
      exports,
      Error,
      process: { env: { LOCALAPPDATA: "C:\\test-app-data" } },
      require: (id) => {
        assert.ok(dependencies.has(id), `Unexpected extension dependency: ${id}`);
        return dependencies.get(id);
      },
      setInterval: () => {
        const timer = { unref() {} };
        timers.add(timer);
        return timer;
      },
      clearInterval: (timer) => timers.delete(timer),
    },
  );
  exports.activate({ subscriptions });
  t.after(() => {
    subscriptions.forEach((subscription) => subscription.dispose());
    assert.equal(timers.size, 0);
  });
  return {
    commands, events, calls, messages, executedCommands, timers,
    subscriptions, item, state, workspace,
    refresh: () => commands.get("qqCopilot.refreshStatus")(),
    show: () => commands.get("qqCopilot.showStatus")(),
  };
}

test("manifest contributes only status commands and connection settings", () => {
  const commands = ["qqCopilot.refreshStatus", "qqCopilot.showStatus"];
  assert.deepEqual(
    manifest.contributes.commands.map((command) => command.command),
    commands,
  );
  assert.deepEqual(manifest.activationEvents, [
    "onStartupFinished",
    ...commands.map((command) => `onCommand:${command}`),
  ]);
  assert.deepEqual(Object.keys(manifest.contributes.configuration.properties), [
    "qqCopilot.bridgeExecutable",
    "qqCopilot.configPath",
    "qqCopilot.statusPollSeconds",
  ]);
});

test("activation displays existing Session status without authentication or creation UI", async (t) => {
  const extension = activateExtension(t);
  await extension.refresh();
  assert.deepEqual([...extension.commands.keys()], [
    "qqCopilot.refreshStatus",
    "qqCopilot.showStatus",
  ]);
  assert.equal(extension.subscriptions.length, 6);
  assert.equal(extension.timers.size, 1);
  assert.equal(extension.item.text, "$(radio-tower) QQ AHP 已连接 2");
  assert.equal(extension.item.backgroundColor, undefined);
  assert.equal(extension.item.tooltip.value, [
    "**QQ Gateway:** connected",
    "**AHP Adapter:** connected",
    "**Sessions:** 2 bound（运行中 1）",
    "**待交互:** 4",
    "**待补发:** 0",
  ].join("  \n"));
  extension.show();
  assert.deepEqual(extension.messages, [
    "Bridge: online | QQ Gateway: connected | AHP Adapter: connected | " +
      "Session bindings: 2 bound | Active Turns: 1 | Pending Adapter commands: 2 | " +
      "Pending interactions: 4 | Pending QQ deliveries: 0",
  ]);

  const [timer] = extension.timers;
  extension.events.configuration({ affectsConfiguration: () => false });
  assert.ok(extension.timers.has(timer));
  extension.events.configuration({ affectsConfiguration: () => true });
  await extension.refresh();
  assert.equal(extension.timers.size, 1);
  assert.equal(extension.timers.has(timer), false);
});

test("status retains pending delivery, unready binding, and offline indicators", async (t) => {
  const response = statusResponse();
  response.ahp.pending_projections = 5;
  const extension = activateExtension(t, response);
  await extension.refresh();
  assert.equal(extension.item.text, "$(warning) QQ 待补发 5");
  assert.equal(extension.item.backgroundColor.id, "statusBarItem.warningBackground");

  response.ahp.pending_projections = 0;
  response.ahp.bindings[1].state = "failed";
  await extension.refresh();
  assert.equal(extension.item.text, "$(warning) QQ AHP 未就绪");
  assert.match(extension.item.tooltip.value, /1\/2 bound/u);

  extension.state.response = Object.assign(new Error("Bridge unavailable"), {
    code: "ENOENT",
  });
  await extension.refresh();
  assert.equal(extension.item.text, "$(debug-disconnect) QQ AHP 离线");
  assert.equal(extension.item.backgroundColor.id, "statusBarItem.errorBackground");
  extension.show();
  assert.match(extension.messages.at(-1), /Error: ENOENT$/u);
});

test("status accepts the foreground compatibility binding and missing AHP status", async (t) => {
  const response = statusResponse();
  delete response.ahp.bindings;
  response.ahp.binding = { state: "bound" };
  const extension = activateExtension(t, response);
  await extension.refresh();
  assert.equal(extension.item.text, "$(radio-tower) QQ AHP 已连接 1");

  delete response.ahp;
  await extension.refresh();
  assert.equal(extension.item.text, "$(warning) QQ AHP 未就绪");
  assert.match(extension.item.tooltip.value, /AHP Adapter:\*\* not registered/u);
  assert.match(extension.item.tooltip.value, /Sessions:\*\* unbound/u);
});

test("workspace trust reporting, target UI, and workspace listeners remain active", async (t) => {
  const extension = activateExtension(t);
  const uri = "file:///C:/trusted-target";
  extension.workspace.workspaceFolders = [{ uri: { toString: () => uri } }];
  extension.state.requests = [{
    request_id: "trust-1",
    workspace_uri: uri,
    open_trust_ui: true,
    trusted: false,
  }];
  await extension.refresh();
  assert.deepEqual(extension.calls, [
    ["status"],
    ["report-trust", "--workspace-uri", uri],
    ["trust-requests"],
  ]);
  assert.deepEqual(extension.executedCommands, ["workbench.trust.manage"]);

  await extension.refresh();
  assert.equal(extension.executedCommands.length, 1);
  extension.workspace.isTrusted = true;
  await extension.events.trust();
  assert.deepEqual(extension.calls.at(-2), [
    "report-trust", "--workspace-uri", uri, "--trusted",
  ]);
  assert.equal(extension.executedCommands.length, 1);

  extension.workspace.workspaceFolders = [];
  extension.calls.length = 0;
  await extension.events.folders();
  assert.deepEqual(extension.calls, [["status"]]);
});
