const assert = require("node:assert/strict");
const net = require("node:net");
const test = require("node:test");

const {
  AuthBrokerError,
  AuthBrokerServer,
  bridgeIpcTokenFromToml,
  deriveAuthBrokerIdentity,
} = require("../dist/auth-broker.js");

const TOKEN = "ab".repeat(32);

test("auth broker identity is deterministic without exposing the IPC token", () => {
  const identity = deriveAuthBrokerIdentity(TOKEN);
  assert.deepEqual(identity, {
    pipePath:
      "\\\\.\\pipe\\qq-copilot-auth-3c1a4a8cf1caca92f1121e0f648e6598",
    authorization:
      "3c1a4a8cf1caca92f1121e0f648e6598a0354669a1203f51016e85e5fb097546",
  });
  assert.equal(JSON.stringify(identity).includes(TOKEN), false);
});

test("bridge IPC token is read only from the bridge table", () => {
  const parsed = bridgeIpcTokenFromToml(`
[qq]
ipc_token = "${"cd".repeat(32)}"

[bridge]
ipc_token = "${TOKEN}"

[ahp]
enabled = true
`);
  assert.equal(parsed, TOKEN);
});

test("auth broker validates metadata and returns a token only on its direct pipe", async () => {
  const identity = deriveAuthBrokerIdentity("cd".repeat(32));
  const requests = [];
  const server = new AuthBrokerServer(
    identity,
    async (request) => {
      requests.push(request);
      if (request.provider === "consent-denied") {
        throw new AuthBrokerError("consent_required", "Consent required");
      }
      return { accessToken: "test-access-token" };
    },
    (error) => assert.fail(error),
  );
  assert.equal(await server.start(), true);
  try {
    const response = await sendRequest(identity.pipePath, {
      version: 1,
      authorization: identity.authorization,
      request_id: "request-1",
      provider: "copilotcli",
      resource: "https://api.github.com",
      authorization_servers: ["https://github.com/login/oauth"],
      scopes: ["read:user", "user:email"],
    });
    assert.deepEqual(response, {
      version: 1,
      request_id: "request-1",
      ok: true,
      token: "test-access-token",
    });
    assert.deepEqual(requests, [
      {
        requestId: "request-1",
        provider: "copilotcli",
        resource: "https://api.github.com",
        authorizationServers: ["https://github.com/login/oauth"],
        scopes: ["read:user", "user:email"],
      },
    ]);
    const rejected = await sendRequest(identity.pipePath, {
      version: 1,
      authorization: identity.authorization,
      request_id: "request-2",
      provider: "copilotcli",
      resource: "https://api.github.com",
      authorization_servers: ["https://github.com/login/oauth"],
      scopes: ["repo"],
    });
    assert.deepEqual(rejected, {
      version: 1,
      request_id: "request-2",
      ok: false,
      error: "unsupported_scope",
    });
    assert.equal(requests.length, 1);
    const unsupportedResource = await sendRequest(identity.pipePath, {
      version: 1,
      authorization: identity.authorization,
      request_id: "request-3",
      provider: "copilotcli",
      resource: "https://ghe.example.test/api/v3",
      authorization_servers: ["https://ghe.example.test/login/oauth"],
      scopes: ["read:user", "user:email"],
    });
    assert.deepEqual(unsupportedResource, {
      version: 1,
      request_id: "request-3",
      ok: false,
      error: "unsupported_resource",
    });
    const unauthorized = await sendRequest(identity.pipePath, {
      version: 1,
      authorization: "00".repeat(32),
      request_id: "request-4",
      provider: "copilotcli",
      resource: "https://api.github.com",
      authorization_servers: ["https://github.com/login/oauth"],
      scopes: ["read:user", "user:email"],
    });
    assert.deepEqual(unauthorized, {
      version: 1,
      ok: false,
      error: "unauthorized",
    });
    const consentDenied = await sendRequest(identity.pipePath, {
      version: 1,
      authorization: identity.authorization,
      request_id: "request-5",
      provider: "consent-denied",
      resource: "https://api.github.com",
      authorization_servers: ["https://github.com/login/oauth"],
      scopes: ["read:user", "user:email"],
    });
    assert.deepEqual(consentDenied, {
      version: 1,
      request_id: "request-5",
      ok: false,
      error: "consent_required",
    });
    assert.equal(requests.length, 2);
  } finally {
    server.dispose();
  }
});

test("disposing the auth broker closes in-flight token requests", async () => {
  const identity = deriveAuthBrokerIdentity("12".repeat(32));
  let releaseAuthentication;
  let markStarted;
  const started = new Promise((resolve) => {
    markStarted = resolve;
  });
  const server = new AuthBrokerServer(
    identity,
    () =>
      new Promise((resolve) => {
        releaseAuthentication = resolve;
        markStarted();
      }),
    (error) => assert.fail(error),
  );
  assert.equal(await server.start(), true);
  const socket = net.connect(identity.pipePath);
  socket.setEncoding("utf8");
  let response = "";
  const closed = new Promise((resolve, reject) => {
    socket.on("connect", () => {
      socket.write(
        `${JSON.stringify({
          version: 1,
          authorization: identity.authorization,
          request_id: "request-in-flight",
          provider: "copilotcli",
          resource: "https://api.github.com",
          authorization_servers: ["https://github.com/login/oauth"],
          scopes: ["read:user", "user:email"],
        })}\n`,
      );
    });
    socket.on("data", (chunk) => {
      response += chunk;
    });
    socket.on("close", resolve);
    socket.on("error", reject);
  });
  await started;
  server.dispose();
  releaseAuthentication({ accessToken: "must-not-be-delivered" });
  await closed;
  assert.equal(response.includes("must-not-be-delivered"), false);
});

function sendRequest(pipePath, request) {
  return new Promise((resolve, reject) => {
    const socket = net.connect(pipePath);
    let response = "";
    socket.setEncoding("utf8");
    socket.on("connect", () => {
      socket.write(`${JSON.stringify(request)}\n`);
    });
    socket.on("data", (chunk) => {
      response += chunk;
      const newline = response.indexOf("\n");
      if (newline >= 0) {
        socket.end();
        resolve(JSON.parse(response.slice(0, newline)));
      }
    });
    socket.on("error", reject);
  });
}
