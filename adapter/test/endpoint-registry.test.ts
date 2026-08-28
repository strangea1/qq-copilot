import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  rm,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import test from "node:test";

import { watchEditorEndpoints } from "../src/endpoint-registry.js";

test("registry watcher detects editor endpoint creation and aborts cleanly", async () => {
  const userDataDirectory = await mkdtemp(
    join(process.cwd(), ".endpoint-watch-test-"),
  );
  const abort = new AbortController();

  try {
    const watcher = watchEditorEndpoints(userDataDirectory, {
      signal: abort.signal,
      pollIntervalMs: 5,
    });
    const initial = await watcher.next();
    assert.deepEqual(initial.value, []);

    const instanceId = "instance_12345678";
    const connectionToken = "token_12345678901234567890123456";
    const identity = `editor\0${process.pid}\0${instanceId}`;
    const fileName = `${createHash("sha256").update(identity).digest("hex")}.json`;
    const entriesDirectory = join(
      userDataDirectory,
      "agent-host",
      "local-endpoint",
      "entries",
    );
    await mkdir(entriesDirectory, { recursive: true });
    await writeFile(
      join(entriesDirectory, fileName),
      JSON.stringify({
        schemaVersion: 2,
        type: "editor",
        pid: process.pid,
        instanceId,
        protocolVersion: "1.0.0",
        connectionToken,
        endpoint: {
          type: "socket",
          path: "\\\\.\\pipe\\qq-copilot-watcher-test",
        },
      }),
      "utf8",
    );

    const changed = await watcher.next();
    assert.equal(changed.done, false);
    assert.equal(changed.value?.length, 1);
    assert.equal(changed.value?.[0]?.instanceId, instanceId);

    abort.abort();
    const completed = await watcher.next();
    assert.equal(completed.done, true);
  } finally {
    abort.abort();
    await rm(userDataDirectory, { recursive: true, force: true });
  }
});
