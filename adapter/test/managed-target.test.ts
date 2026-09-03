import assert from "node:assert/strict";
import test from "node:test";

import {
  connectManagedTarget,
  managedTargetMatchesWorkspaceUri,
  managedTargetWorkspaceUri,
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
