import net from "node:net";

import { TransportError, type AhpTransport } from "@microsoft/agent-host-protocol/client";
import { WebSocketTransport } from "@microsoft/agent-host-protocol/ws";
import Ws from "ws";

const OPEN_TIMEOUT_MS = 10_000;
const MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;

export async function openNamedPipeTransport(
  pipePath: string,
  connectionToken: string,
): Promise<AhpTransport> {
  const url = new URL("ws://localhost/");
  url.searchParams.set("tkn", connectionToken);

  let socket: Ws;
  try {
    socket = new Ws(url, {
      createConnection: () => net.createConnection(pipePath),
      followRedirects: false,
      perMessageDeflate: false,
      maxPayload: MAX_PAYLOAD_BYTES,
    });
  } catch (error) {
    throw new TransportError("io", "failed to construct Agent Host transport", {
      cause: error,
    });
  }

  try {
    await waitForOpen(socket);
    // `ws` implements the DOM WebSocket contract at runtime; its Node-only
    // typings are intentionally bridged at this single transport boundary.
    return WebSocketTransport.fromSocket(
      socket as unknown as globalThis.WebSocket,
    );
  } catch (error) {
    socket.terminate();
    if (error instanceof TransportError) {
      throw error;
    }
    throw new TransportError("io", "Agent Host WebSocket upgrade failed", {
      cause: error,
    });
  }
}

function waitForOpen(socket: Ws): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new TransportError("io", "Agent Host connection timed out"));
    }, OPEN_TIMEOUT_MS);
    timer.unref();

    const opened = (): void => {
      cleanup();
      resolve();
    };
    const failed = (): void => {
      cleanup();
      reject(new TransportError("io", "Agent Host connection failed"));
    };
    const cleanup = (): void => {
      clearTimeout(timer);
      socket.off("open", opened);
      socket.off("error", failed);
      socket.off("close", failed);
    };

    socket.once("open", opened);
    socket.once("error", failed);
    socket.once("close", failed);
  });
}
