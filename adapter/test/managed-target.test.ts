import assert from "node:assert/strict";
import test from "node:test";

import {
  connectManagedTarget,
  defaultManagedProvider,
  managedTargetMatchesWorkspaceUri,
  managedTargetWorkspaceUri,
  standaloneWireProtocolVersion,
} from "../src/managed-target.js";

test("managed target reports executable spawn failures without crashing", async () => {
  await assert.rejects(
    connectManagedTarget(
      {
        configPath: "test",
        bridgePipePath: "test",
        bridgeToken: "a".repeat(64),
        adapterId: "test",
        userDataDirectory: process.cwd(),
        pollSeconds: 1,
        codeExecutable: "Z:\\missing\\code-tunnel.exe",
        authorizedTargets: [],
      },
      {
        kind: "local",
        path: process.cwd(),
      },
    ),
    /ENOENT|not found/iu,
  );
});

test("standalone registry 0.1.0 is an audited alias for the 0.9.0 wire", () => {
  assert.equal(standaloneWireProtocolVersion("0.1.0"), "0.9.0");
  assert.equal(standaloneWireProtocolVersion("0.9.0"), "0.9.0");
  assert.equal(standaloneWireProtocolVersion("1.0.0"), "1.0.0");
  assert.equal(standaloneWireProtocolVersion("0.2.0"), undefined);
});

test("managed targets prefer the standalone Copilot provider", () => {
  const agent = (provider: string) => ({
    provider,
    displayName: provider,
    description: provider,
    models: [],
  });
  assert.equal(
    defaultManagedProvider({
      agents: [agent("claude"), agent("copilotcli")],
    }),
    "copilotcli",
  );
  assert.equal(
    defaultManagedProvider({
      agents: [agent("claude"), agent("copilot")],
    }),
    "copilot",
  );
  assert.equal(
    defaultManagedProvider({
      agents: [agent("custom")],
    }),
    "custom",
  );
  assert.throws(
    () =>
      defaultManagedProvider({
        agents: [agent("custom-a"), agent("custom-b")],
      }),
    /provider-selection-required/u,
  );
});

test("managed target workspace URIs preserve path delimiters as data", () => {
  const local = new URL(
    managedTargetWorkspaceUri({
      kind: "local",
      path: "C:\\work\\hash#query?\\project",
    }),
  );
  assert.equal(local.protocol, "file:");
  assert.equal(local.hash, "");
  assert.equal(local.search, "");
  assert.equal(decodeURIComponent(local.pathname), "/C:/work/hash#query?/project");

  const remote = new URL(
    managedTargetWorkspaceUri({
      kind: "ssh",
      alias: "devbox",
      path: "/srv/hash#query?/project",
      user: "tester",
      host: "devbox.internal",
      port: 22,
      hostKeyFingerprints: ["SHA256:test"],
    }),
  );
  assert.equal(remote.hash, "");
  assert.equal(remote.search, "");
  assert.equal(decodeURIComponent(remote.pathname), "/srv/hash#query?/project");

  const literalSegments = new URL(
    managedTargetWorkspaceUri({
      kind: "ssh",
      alias: "devbox",
      path: "/srv/%2e%2e/back\\slash",
      user: "tester",
      host: "devbox.internal",
      port: 22,
      hostKeyFingerprints: ["SHA256:test"],
    }),
  );
  assert.equal(
    decodeURIComponent(literalSegments.pathname),
    "/srv/%2e%2e/back\\slash",
  );
});

test("managed SSH targets match file and VS Code Remote workspace URIs", () => {
  const target = {
    kind: "ssh" as const,
    alias: "devbox",
    path: "/srv/hash#query?/project",
    user: "tester",
    host: "devbox.internal",
    port: 22,
    hostKeyFingerprints: ["SHA256:test"],
  };
  assert.equal(
    managedTargetMatchesWorkspaceUri(
      target,
      managedTargetWorkspaceUri(target),
    ),
    true,
  );
  assert.equal(
    managedTargetMatchesWorkspaceUri(
      target,
      "vscode-remote://ssh-remote+devbox/srv/hash%23query%3F/project",
    ),
    true,
  );
  assert.equal(
    managedTargetMatchesWorkspaceUri(
      target,
      "vscode-remote://ssh-remote+other/srv/hash%23query%3F/project",
    ),
    false,
  );
});

test("managed local targets match encoded drive-letter file URIs", () => {
  const target = {
    kind: "local" as const,
    path: "C:\\test",
  };
  assert.equal(
    managedTargetMatchesWorkspaceUri(target, "file:///c%3A/test"),
    true,
  );
  assert.equal(
    managedTargetMatchesWorkspaceUri(target, "file:///C:/test/"),
    true,
  );
  assert.equal(
    managedTargetMatchesWorkspaceUri(target, "file:///C:/other"),
    false,
  );
});
