import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { parse } from "smol-toml";

import { defaultUserDataDirectory } from "./endpoint-registry.js";

export interface AdapterConfig {
  readonly configPath: string;
  readonly bridgePipePath: string;
  readonly bridgeToken: string;
  readonly adapterId: string;
  readonly userDataDirectory: string;
  readonly pollSeconds: number;
}

export async function loadAdapterConfig(
  configPath: string,
  userDataDirectory?: string,
): Promise<AdapterConfig> {
  const absoluteConfigPath = resolve(configPath);
  const parsed = parse(await readFile(absoluteConfigPath, "utf8"));
  const bridge = requireTable(parsed.bridge, "bridge");
  const ahp = requireTable(parsed.ahp, "ahp");
  if (ahp.enabled !== true) {
    throw new Error("AHP mode is disabled in Bridge config");
  }
  const pipeName = requireString(bridge.pipe_name, "bridge.pipe_name");
  if (
    pipeName.includes("\\") ||
    pipeName.includes("/") ||
    pipeName.length > 200
  ) {
    throw new Error("bridge.pipe_name is invalid");
  }
  const bridgeToken = requireString(
    bridge.ipc_token,
    "bridge.ipc_token",
  );
  if (!/^[a-fA-F0-9]{64,}$/u.test(bridgeToken)) {
    throw new Error("bridge.ipc_token is invalid");
  }
  const pollSeconds = requireInteger(ahp.poll_seconds, "ahp.poll_seconds");
  if (pollSeconds < 1 || pollSeconds > 60) {
    throw new Error("ahp.poll_seconds must be between 1 and 60");
  }
  const adapterId = `qq-copilot-ahp-${createHash("sha256")
    .update(`adapter\0${pipeName}\0${bridgeToken}`, "utf8")
    .digest("hex")
    .slice(0, 24)}`;
  return {
    configPath: absoluteConfigPath,
    bridgePipePath: `\\\\.\\pipe\\${pipeName}`,
    bridgeToken,
    adapterId,
    userDataDirectory: resolve(
      userDataDirectory ?? defaultUserDataDirectory(),
    ),
    pollSeconds,
  };
}

export interface AdapterArguments {
  readonly configPath: string;
  readonly userDataDirectory?: string;
}

export function parseAdapterArguments(args: readonly string[]): AdapterArguments {
  let configPath: string | undefined;
  let userDataDirectory: string | undefined;
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    const value = args[index + 1];
    if (argument === "--config" && value) {
      configPath = value;
      index += 1;
    } else if (argument === "--user-data-dir" && value) {
      userDataDirectory = value;
      index += 1;
    } else {
      throw new Error(
        "usage: qq-ahp-adapter --config <path> [--user-data-dir <path>]",
      );
    }
  }
  if (!configPath) {
    throw new Error("--config is required");
  }
  return {
    configPath,
    ...(userDataDirectory ? { userDataDirectory } : {}),
  };
}

function requireTable(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${name} must be a TOML table`);
  }
  return value as Record<string, unknown>;
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
  return value;
}
function requireInteger(value: unknown, name: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new Error(`${name} must be an integer`);
  }
  return value;
}
