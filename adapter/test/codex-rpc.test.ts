import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import test from "node:test";

import { startCodexClient, StdioCodexClient, type CodexMessage } from "../src/codex-rpc.js";

test("Codex stdio handles fragmented UTF-8, server requests, errors and orderly shutdown", async () => {
  const script = `
    const readline = require("node:readline");
    const lines = readline.createInterface({input:process.stdin});
    lines.on("line",line=>{
      const m=JSON.parse(line);
      if(m.method==="test"){
        const b=Buffer.from(JSON.stringify({id:m.id,result:{text:"\\u6d4b\\u8bd5"}})+"\\n");
        const i=b.indexOf(Buffer.from("\\u6d4b"))+1;
        process.stdout.write(b.subarray(0,i));
        setTimeout(()=>process.stdout.write(b.subarray(i)),5);
      }else if(m.method==="approve"){
        process.stdout.write(JSON.stringify({id:"request-1",method:"approval",params:{threadId:"thread"}})+"\\n");
        process.stdout.write(JSON.stringify({id:m.id,result:{}})+"\\n");
      }else if(m.method==="failure"){
        process.stdout.write(JSON.stringify({id:m.id,error:{code:-32602,message:"PRIVATE-ERROR"}})+"\\n");
      }else if(m.method==="hang"){
        process.exit(1);
      }
    });
  `;
  const child = spawn(process.execPath, ["-e", script], { stdio: ["pipe", "pipe", "pipe"] });
  const received: CodexMessage[] = [];
  let disconnected = 0;
  const client = new StdioCodexClient(child, (message) => received.push(message), () => disconnected++);
  try {
    assert.deepEqual(await client.request("test", {}), { text: "\u6d4b\u8bd5" });
    await client.request("approve", {});
    assert.deepEqual(received, [{ method: "approval", id: "request-1", params: { threadId: "thread" } }]);
    await client.respond("request-1", { decision: "accept" });
    await assert.rejects(client.request("failure", {}), (error: unknown) =>
      error instanceof Error && error.message === "codex-rpc-error:-32602" && !error.message.includes("PRIVATE"));
    await assert.rejects(client.request("hang", {}), /codex-disconnected/u);
    await assert.rejects(client.request("test", {}), /codex-disconnected/u);
    assert.equal(disconnected, 1);
  } finally {
    await client.close();
  }
});

test("Codex rejects malformed protocol output without hanging pending commands", async () => {
  const child = spawn(process.execPath, ["-e", `
    process.stdin.once("data",()=>process.stdout.write("not JSON\\n"));
  `], { stdio: ["pipe", "pipe", "pipe"] });
  let disconnected = 0;
  const client = new StdioCodexClient(child, () => {}, () => disconnected++);
  try {
    await assert.rejects(client.request("initialize", {}), /codex-invalid-frame/u);
    assert.equal(disconnected, 1);
  } finally {
    await client.close();
  }
});

test("Codex stops only its owned child and rejects unaudited binaries", async () => {
  const child = spawn(process.execPath, ["-e", "process.stdin.resume()"], { stdio: ["pipe", "pipe", "pipe"] });
  await once(child, "spawn");
  let disconnected = 0;
  const client = new StdioCodexClient(child, () => {}, () => disconnected++);
  await client.close();
  assert.notEqual(child.exitCode, null);
  assert.equal(disconnected, 0);
  await assert.rejects(startCodexClient(process.execPath, () => {}, () => {}),
    /codex-unsupported-version/u);
});

test("Codex rejects malformed RPC errors while keeping the pending request rejectable", async () => {
  const child = spawn(process.execPath, ["-e", `
    require("node:readline").createInterface({input:process.stdin}).on("line",line=>{
      process.stdout.write(JSON.stringify({id:JSON.parse(line).id,error:"invalid error"})+"\\n");
    });
  `], { stdio: ["pipe", "pipe", "pipe"] });
  const client = new StdioCodexClient(child, () => {}, () => {});
  try {
    await assert.rejects(client.request("turn/start", {}), /codex-invalid-frame/u);
  } finally {
    await client.close();
  }
});

test("Codex closes the owned process after a timed-out mutation without replaying it", async (t) => {
  const child = spawn(process.execPath, ["-e", "process.stdin.resume()"], { stdio: ["pipe", "pipe", "pipe"] });
  await once(child, "spawn");
  let disconnected = 0;
  const client = new StdioCodexClient(child, () => {}, () => disconnected++);
  t.mock.timers.enable({ apis: ["setTimeout"] });
  try {
    const pending = assert.rejects(client.request("turn/start", {}), /codex-request-timeout/u);
    t.mock.timers.tick(30_000);
    await pending;
    assert.equal(disconnected, 1);
    await assert.rejects(client.request("turn/start", {}), /codex-disconnected/u);
    assert.equal(child.killed, true);
  } finally {
    t.mock.timers.reset();
    await client.close();
  }
});
