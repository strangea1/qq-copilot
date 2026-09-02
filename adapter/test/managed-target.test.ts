import assert from "node:assert/strict";
import test from "node:test";

import { connectManagedTarget } from "../src/managed-target.js";

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
