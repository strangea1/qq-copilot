import assert from "node:assert/strict";
import { once } from "node:events";
import test from "node:test";

import { SessionStatus, type SessionSummary } from "@microsoft/agent-host-protocol";
import { WebSocketServer } from "ws";

import {
  connectManagedTarget,
  connectPreparedManagedTarget,
  defaultManagedProvider,
  managedTargetMatchesWorkspaceUri,
  managedTargetWorkspaceUri,
  prepareTargetResult,
  standaloneWireProtocolVersion,
  type PreparedTarget,
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

test("managed discovery and reconnect list existing Sessions without authentication or model configuration", async () => {
  const requests: string[] = [];
  const advertisedVersions: string[][] = [];
  const sessions: SessionSummary[] = ["first", "second"].map((id) => ({
    resource: `ahp-session:/${id}`,
    provider: "copilotcli",
    title: id,
    status: SessionStatus.Idle,
    workingDirectories: ["file:///srv/project"],
    createdAt: "2026-09-16T00:00:00.000Z",
    modifiedAt: "2026-09-16T00:00:00.000Z",
  }));
  const server = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  let connections = 0;
  server.on("connection", (socket) => {
    connections += 1;
    socket.on("message", (data) => {
      const message = JSON.parse(data.toString()) as {
        readonly id?: number;
        readonly method: string;
        readonly params: { readonly cursor?: string; readonly protocolVersions?: string[] };
      };
      requests.push(message.method);
      let result: unknown;
      switch (message.method) {
        case "initialize":
          advertisedVersions.push(message.params.protocolVersions ?? []);
          result = {
            protocolVersion: "0.9.0",
            serverSeq: 1,
            snapshots: [{
              resource: "ahp-root://",
              fromSeq: 1,
              state: {
                agents: [{
                  provider: "copilotcli",
                  displayName: "Copilot",
                  description: "",
                  models: [],
                  protectedResources: [{
                    resource: "https://api.github.com",
                    authorization_servers: ["https://github.com/login/oauth"],
                    scopes_supported: ["read:user"],
                    required: true,
                  }],
                }],
              },
            }],
          };
          break;
        case "listSessions":
          result = message.params.cursor
            ? { items: sessions.slice(1) }
            : { items: sessions.slice(0, 1), nextCursor: "next" };
          break;
        default:
          socket.send(JSON.stringify({
            jsonrpc: "2.0", id: message.id,
            error: { code: -32601, message: "Unexpected method" },
          }));
          return;
      }
      socket.send(JSON.stringify({ jsonrpc: "2.0", id: message.id, result }));
    });
  });
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const prepared: PreparedTarget = {
    target: {
      kind: "ssh", alias: "existing", path: "/srv/project",
      user: "tester", host: "existing", port: 22, hostKeyFingerprints: ["SHA256:pinned"],
    },
    entry: {
      schemaVersion: 2, type: "standalone", pid: process.pid, instanceId: "existing-host",
      protocolVersion: "0.1.0", wireProtocolVersion: "0.9.0",
      connectionToken: "test-connection-token",
      endpoint: { type: "tcp", host: "127.0.0.1", port: address.port },
      sourceFile: "unused",
    },
    endpointId: "existing-endpoint",
    hostLabel: "ssh:existing",
    workspaceUri: "file:///srv/project",
    editorClientToolsAvailable: false,
  };
  try {
    for (let reconnect = 0; reconnect < 2; reconnect += 1) {
      const connection = await connectPreparedManagedTarget(prepared);
      try {
        assert.equal(connection.prepared, prepared);
        assert.deepEqual(connection.sessions, sessions);
        assert.deepEqual(await prepareTargetResult(connection), {
          endpoint_id: prepared.endpointId,
          host_instance_id: prepared.entry.instanceId,
          provider: "copilotcli",
          workspace_uri: prepared.workspaceUri,
          host_label: prepared.hostLabel,
          editor_client_tools_available: false,
        });
      } finally {
        await connection.disconnect();
      }
      sessions[0] = { ...sessions[0]!, title: "Updated on PC" };
    }
    assert.equal(connections, 2);
    assert.deepEqual(requests, [
      "initialize", "listSessions", "listSessions",
      "initialize", "listSessions", "listSessions",
    ]);
    assert.ok(advertisedVersions.every((versions) => versions.includes("0.9.0") && !versions.includes("0.1.0")));
  } finally {
    for (const socket of server.clients) {
      socket.terminate();
    }
    await new Promise<void>((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
  }
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
  for (const authority of ["ssh-remote%2Bdevbox", "SSH-REMOTE%2bDEVBOX"]) {
    assert.equal(
      managedTargetMatchesWorkspaceUri(
        target,
        `vscode-remote://${authority}/srv/hash%23query%3F/project`,
      ),
      true,
      authority,
    );
  }
  assert.equal(
    managedTargetMatchesWorkspaceUri(
      target,
      "vscode-remote://ssh-remote+other/srv/hash%23query%3F/project",
    ),
    false,
  );
});

test("managed SSH target matching rejects invalid authorities and non-target URIs", () => {
  const target = {
    kind: "ssh" as const,
    alias: "devbox",
    path: "/srv/project",
    user: "tester",
    host: "devbox.internal",
    port: 22,
    hostKeyFingerprints: ["SHA256:test"],
  };
  for (const uri of [
    "vscode-remote://ssh-remote%2Bother/srv/project",
    "vscode-remote://ssh-remote%252Bdevbox/srv/project",
    "vscode-remote://ssh-remote%2Bdevbox.evil/srv/project",
    "vscode-remote://ssh-remote%2Bdevbox%2Fother/srv/project",
    "vscode-remote://ssh-remote%GGdevbox/srv/project",
    "vscode-remote://user@ssh-remote+devbox/srv/project",
    "vscode-remote://user@ssh-remote%2Bdevbox/srv/project",
    "vscode-remote://ssh-remote%2Bdevbox:22/srv/project",
    "vscode-remote://ssh-remote%2Bdevbox/srv/project/child",
    "vscode-remote://ssh-remote%2Bdevbox/srv/Project",
    "vscode-remote://ssh-remote%2Bdevbox/srv/project?query",
    "vscode-remote://ssh-remote%2Bdevbox/srv/project#fragment",
    "vscode-remote://ssh-remote%2Bdevbox/srv/%GG",
    "file://devbox/srv/project",
  ]) {
    assert.equal(managedTargetMatchesWorkspaceUri(target, uri), false, uri);
  }
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
