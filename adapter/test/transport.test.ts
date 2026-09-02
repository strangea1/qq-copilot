import assert from "node:assert/strict";
import { once } from "node:events";
import test from "node:test";

import { WebSocketServer } from "ws";

import { openEndpointTransport } from "../src/named-pipe-transport.js";

test("endpoint transport connects to tcp and websocket endpoints with the token query", async () => {
  const queries: string[] = [];
  const server = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  server.on("connection", (_socket, request) => {
    queries.push(request.url ?? "");
  });
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");

  const tcpTransport = await openEndpointTransport(
    {
      type: "tcp",
      host: "127.0.0.1",
      port: address.port,
    },
    "tcp-secret",
  );
  await Promise.resolve(tcpTransport.close());

  const websocketTransport = await openEndpointTransport(
    {
      type: "websocket",
      url: `ws://127.0.0.1:${address.port}/ahp`,
    },
    "ws-secret",
  );
  await Promise.resolve(websocketTransport.close());

  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });

  assert.equal(queries.length, 2);
  assert.equal(queries[0], "/?tkn=tcp-secret");
  assert.equal(queries[1], "/ahp?tkn=ws-secret");
});
