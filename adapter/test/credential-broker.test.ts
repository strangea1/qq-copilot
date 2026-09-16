import assert from "node:assert/strict";
import { createServer } from "node:net";
import test from "node:test";

import {
  deriveCredentialBrokerIdentity,
  requestBrokeredCredential,
} from "../src/credential-broker.js";

test("credential broker identity matches the VS Code broker test vector", () => {
  const token = "ab".repeat(32);
  const identity = deriveCredentialBrokerIdentity(token);
  assert.deepEqual(identity, {
    pipePath:
      "\\\\.\\pipe\\qq-copilot-auth-3c1a4a8cf1caca92f1121e0f648e6598",
    authorization:
      "3c1a4a8cf1caca92f1121e0f648e6598a0354669a1203f51016e85e5fb097546",
  });
  assert.equal(JSON.stringify(identity).includes(token), false);
});

test("credential broker client sends metadata and accepts a direct token response", async () => {
  const bridgeToken = "ef".repeat(32);
  const identity = deriveCredentialBrokerIdentity(bridgeToken);
  let request: Record<string, unknown> | undefined;
  const server = createServer((socket) => {
    socket.setEncoding("utf8");
    let contents = "";
    socket.on("data", (chunk) => {
      contents += chunk;
      const newline = contents.indexOf("\n");
      if (newline < 0) {
        return;
      }
      request = JSON.parse(contents.slice(0, newline)) as Record<string, unknown>;
      socket.end(
        `${JSON.stringify({
          version: 1,
          request_id: request.request_id,
          ok: true,
          token: "brokered-test-token",
        })}\n`,
      );
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(identity.pipePath, resolve);
  });
  try {
    const credential = await requestBrokeredCredential(bridgeToken, {
      provider: "copilotcli",
      resource: "https://api.github.com",
      authorizationServers: ["https://github.com/login/oauth"],
      scopes: ["read:user", "user:email"],
    });
    assert.equal(credential.token, "brokered-test-token");
    assert.equal(request?.authorization, identity.authorization);
    assert.equal(request?.provider, "copilotcli");
    assert.equal(request?.resource, "https://api.github.com");
    assert.deepEqual(request?.scopes, ["read:user", "user:email"]);
    assert.equal(JSON.stringify(request).includes(bridgeToken), false);
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
});

test("credential broker client cancels an in-flight request", async () => {
  const bridgeToken = "cd".repeat(32);
  const identity = deriveCredentialBrokerIdentity(bridgeToken);
  let connected: (() => void) | undefined;
  const accepted = new Promise<void>((resolve) => {
    connected = resolve;
  });
  const server = createServer((socket) => {
    socket.resume();
    connected?.();
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(identity.pipePath, resolve);
  });
  try {
    const controller = new AbortController();
    const request = requestBrokeredCredential(
      bridgeToken,
      {
        provider: "copilotcli",
        resource: "https://api.github.com",
        authorizationServers: ["https://github.com/login/oauth"],
        scopes: ["read:user", "user:email"],
      },
      undefined,
      controller.signal,
    );
    await accepted;
    controller.abort();
    await assert.rejects(
      request,
      (error: unknown) =>
        error instanceof Error &&
        "code" in error &&
        error.code === "authentication-broker-cancelled",
    );
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
});
