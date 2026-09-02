const assert = require("node:assert/strict");
const test = require("node:test");

const {
  matchingTrustRequest,
  userConfigurationValue,
  workspaceTrustCommandArgs,
} = require("../dist/trust-helpers.js");

test("userConfigurationValue ignores workspace-controlled overrides", () => {
  assert.equal(
    userConfigurationValue({
      defaultValue: "",
      globalValue: "C:\\safe\\qq-bridge.exe",
      workspaceValue: "C:\\repo\\malware.exe",
      workspaceFolderValue: "C:\\repo\\other.exe",
    }),
    "C:\\safe\\qq-bridge.exe",
  );
  assert.equal(
    userConfigurationValue({
      defaultValue: "",
      workspaceValue: "C:\\repo\\malware.exe",
    }),
    undefined,
  );
});

test("workspaceTrustCommandArgs adds trusted flag only when needed", () => {
  assert.deepEqual(workspaceTrustCommandArgs(["file:///C:/a"], false), [
    "report-trust",
    "--workspace-uri",
    "file:///C:/a",
  ]);
  assert.deepEqual(workspaceTrustCommandArgs(["file:///C:/a"], true), [
    "report-trust",
    "--workspace-uri",
    "file:///C:/a",
    "--trusted",
  ]);
});

test("matchingTrustRequest ignores trusted or already-opened requests", () => {
  const requests = [
    {
      request_id: "trust-1",
      workspace_uri: "file:///C:/a",
      open_trust_ui: true,
      trusted: false,
    },
    {
      request_id: "trust-2",
      workspace_uri: "file:///C:/b",
      open_trust_ui: true,
      trusted: false,
    },
  ];
  assert.equal(
    matchingTrustRequest(["file:///C:/a"], true, requests, new Set()),
    undefined,
  );
  assert.equal(
    matchingTrustRequest(["file:///C:/a"], false, requests, new Set(["trust-1"])),
    undefined,
  );
  assert.deepEqual(
    matchingTrustRequest(["file:///C:/a"], false, requests, new Set()),
    requests[0],
  );
});
