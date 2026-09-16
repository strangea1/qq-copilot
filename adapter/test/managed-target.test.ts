import assert from "node:assert/strict";
import test from "node:test";

import { PolicyState } from "@microsoft/agent-host-protocol";
import {
  AhpClient,
  InMemoryTransport,
  type TransportFrame,
} from "@microsoft/agent-host-protocol/client";

import type { ProtectedResourceRequest } from "../src/credential-broker.js";
import {
  ManagedAuthenticator,
  connectManagedTarget,
  defaultManagedProvider,
  managedTargetMatchesWorkspaceUri,
  managedTargetWorkspaceUri,
  standaloneWireProtocolVersion,
  supportedModelSelection,
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

test("managed model picker uses AgentInfo models and defaultable model config", () => {
  const selection = supportedModelSelection({
    provider: "copilotcli",
    displayName: "Copilot",
    description: "Copilot",
    models: [
      {
        id: "disabled",
        provider: "copilotcli",
        name: "Disabled",
        policyState: PolicyState.Disabled,
      },
      {
        id: "gpt-test",
        provider: "copilotcli",
        name: "GPT Test",
        supportsVision: true,
        maxContextWindow: 128_000,
        configSchema: {
          type: "object",
          properties: {
            thinking: {
              type: "string",
              title: "Thinking",
              default: "medium",
              enum: ["low", "medium", "high"],
            },
          },
          required: ["thinking"],
        },
      },
    ],
  });
  assert.deepEqual(selection, {
    options: [
      {
        value: {
          id: "gpt-test",
          config: { thinking: "medium" },
        },
        label: "GPT Test",
        description: "vision, context 128,000",
      },
    ],
    selected: {
      id: "gpt-test",
      config: { thinking: "medium" },
    },
  });
});

test("managed model picker fails closed for required config without defaults", () => {
  assert.throws(
    () =>
      supportedModelSelection({
        provider: "copilotcli",
        displayName: "Copilot",
        description: "Copilot",
        models: [
          {
            id: "gpt-test",
            provider: "copilotcli",
            name: "GPT Test",
            configSchema: {
              type: "object",
              properties: {
                thinking: {
                  type: "string",
                  title: "Thinking",
                },
              },
              required: ["thinking"],
            },
          },
        ],
      }),
    /managed-model-configuration-required/u,
  );
});

test("managed authentication brokers an advertised resource into AHP authenticate", async () => {
  const [clientTransport, serverTransport] = InMemoryTransport.pair();
  const client = new AhpClient(clientTransport);
  const brokerRequests: ProtectedResourceRequest[] = [];
  const authenticator = new ManagedAuthenticator(
    client,
    () => ({
      agents: [
        {
          provider: "copilotcli",
          displayName: "Copilot",
          description: "Copilot",
          models: [],
          protectedResources: [
            {
              resource: "https://api.github.com",
              resource_name: "GitHub Copilot",
              authorization_servers: ["https://github.com/login/oauth"],
              scopes_supported: ["read:user", "user:email"],
              required: true,
            },
          ],
        },
      ],
    }),
    {
      resolve: async (request) => {
        brokerRequests.push(request);
        return { token: "managed-test-token" };
      },
    },
  );
  client.connect();
  try {
    const authentication = authenticator.authenticateProvider("copilotcli");
    const frame = await serverTransport.recv();
    if (frame === null) {
      assert.fail("AHP client closed before sending authenticate");
    }
    const request = decodeFrame(frame);
    assert.equal(request.method, "authenticate");
    assert.deepEqual(request.params, {
      channel: "ahp-root://",
      resource: "https://api.github.com",
      token: "managed-test-token",
      scopes: ["read:user", "user:email"],
    });
    await serverTransport.send(
      JSON.stringify({
        jsonrpc: "2.0",
        id: request.id,
        result: {},
      }),
    );
    assert.equal(await authentication, true);
    assert.deepEqual(brokerRequests, [
      {
        provider: "copilotcli",
        resource: "https://api.github.com",
        authorizationServers: ["https://github.com/login/oauth"],
        scopes: ["read:user", "user:email"],
      },
    ]);
  } finally {
    await client.shutdown();
  }
});

test("managed authentication propagates shutdown cancellation to the broker", async () => {
  const [clientTransport] = InMemoryTransport.pair();
  const client = new AhpClient(clientTransport);
  const controller = new AbortController();
  let brokerStarted = false;
  const authenticator = new ManagedAuthenticator(
    client,
    () => ({
      agents: [
        {
          provider: "copilotcli",
          displayName: "Copilot",
          description: "Copilot",
          models: [],
          protectedResources: [
            {
              resource: "https://api.github.com",
              resource_name: "GitHub Copilot",
              authorization_servers: ["https://github.com/login/oauth"],
              scopes_supported: ["read:user"],
              required: true,
            },
          ],
        },
      ],
    }),
    {
      resolve: async (_request, signal) =>
        await new Promise((_resolve, reject) => {
          brokerStarted = true;
          assert.equal(signal, controller.signal);
          signal?.addEventListener(
            "abort",
            () => reject(new Error("authentication cancelled")),
            { once: true },
          );
        }),
    },
    controller.signal,
  );
  client.connect();
  try {
    const authentication = authenticator.authenticateProvider("copilotcli");
    await Promise.resolve();
    assert.equal(brokerStarted, true);
    controller.abort();
    await assert.rejects(authentication, /authentication cancelled/u);
  } finally {
    await client.shutdown();
  }
});

function decodeFrame(frame: TransportFrame): Record<string, unknown> {
  const value =
    frame.kind === "parsed"
      ? frame.message
      : frame.kind === "text"
        ? JSON.parse(frame.text)
        : JSON.parse(new TextDecoder().decode(frame.data));
  assert.equal(typeof value, "object");
  assert.notEqual(value, null);
  assert.equal(Array.isArray(value), false);
  return value as Record<string, unknown>;
}

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
