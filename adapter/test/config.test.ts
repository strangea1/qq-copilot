import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import test from "node:test";

import { loadAdapterConfig } from "../src/config.js";

test("adapter config loads code paths and authorized targets", async (context) => {
  const root = await mkdtemp(join(process.cwd(), ".adapter-config-test-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const configPath = join(root, "config.toml");
  await writeFile(
    configPath,
    `
[bridge]
pipe_name = "pipe-1"
ipc_token = "${"a".repeat(64)}"

[ahp]
enabled = true
poll_seconds = 25
code_executable = "C:\\\\Tools\\\\code-tunnel.exe"
ssh_executable = "C:\\\\Windows\\\\System32\\\\OpenSSH\\\\ssh.exe"
shared_workspaces = ["C:\\\\test"]

[[ahp.authorized_targets]]
kind = "ssh"
alias = "devbox"
path = "/home/test/project"
user = "tester"
host = "devbox.internal"
port = 22
host_key_fingerprints = ["SHA256:example"]
`,
    "utf8",
  );
  const config = await loadAdapterConfig(configPath, root);
  assert.equal(config.codeExecutable?.endsWith("code-tunnel.exe"), true);
  assert.equal(config.sshExecutable?.endsWith("ssh.exe"), true);
  assert.equal(config.authorizedTargets.length, 2);
  assert.deepEqual(config.authorizedTargets[0], {
    kind: "local",
    path: "C:\\test",
  });
  assert.deepEqual(config.authorizedTargets[1], {
    kind: "ssh",
    alias: "devbox",
    path: "/home/test/project",
    user: "tester",
    host: "devbox.internal",
    port: 22,
    hostKeyFingerprints: ["SHA256:example"],
  });
});

test("adapter config keeps editor-only upgrades compatible without code CLI", async (context) => {
  const root = await mkdtemp(join(process.cwd(), ".adapter-config-test-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const configPath = join(root, "config.toml");
  await writeFile(
    configPath,
    `
[bridge]
pipe_name = "pipe-1"
ipc_token = "${"a".repeat(64)}"

[ahp]
enabled = true
poll_seconds = 25
shared_workspaces = ["C:\\\\test"]
`,
    "utf8",
  );

  const config = await loadAdapterConfig(configPath, root);
  assert.equal(config.codeExecutable, undefined);
  assert.equal(config.authorizedTargets.length, 1);
});
