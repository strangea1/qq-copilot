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
  readonly codeExecutable: string;
  readonly sshExecutable?: string;
  readonly authorizedTargets: readonly AuthorizedTargetConfig[];
}

export type AuthorizedTargetConfig =
  | {
      readonly kind: "local";
      readonly path: string;
    }
  | {
      readonly kind: "ssh";
      readonly alias: string;
      readonly path: string;
      readonly user: string;
      readonly host: string;
      readonly port: number;
      readonly hostKeyFingerprints: readonly string[];
    };

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
  const codeExecutable = requireString(
    ahp.code_executable,
    "ahp.code_executable",
  );
  const sshExecutable =
    typeof ahp.ssh_executable === "string" && ahp.ssh_executable.length > 0
      ? ahp.ssh_executable
      : undefined;
  const authorizedTargets = parseAuthorizedTargets(ahp);
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
    codeExecutable: resolve(codeExecutable),
    ...(sshExecutable ? { sshExecutable: resolve(sshExecutable) } : {}),
    authorizedTargets,
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

function parseAuthorizedTargets(
  ahp: Record<string, unknown>,
): readonly AuthorizedTargetConfig[] {
  const targets: AuthorizedTargetConfig[] = [];
  const shared = Array.isArray(ahp.shared_workspaces)
    ? ahp.shared_workspaces
    : typeof ahp.shared_workspace === "string"
      ? [ahp.shared_workspace]
      : [];
  for (const value of shared) {
    if (typeof value === "string" && value.length > 0) {
      targets.push({
        kind: "local",
        path: resolve(value),
      });
    }
  }
  if (Array.isArray(ahp.authorized_targets)) {
    for (const item of ahp.authorized_targets) {
      const target = requireTable(item, "ahp.authorized_targets[]");
      if (target.kind === "local") {
        targets.push({
          kind: "local",
          path: resolve(requireString(target.path, "authorized target path")),
        });
        continue;
      }
      if (target.kind === "ssh") {
        const alias = requireString(target.alias, "authorized target alias");
        if (
          alias.startsWith("-") ||
          !/^[A-Za-z0-9._-]+$/u.test(alias)
        ) {
          throw new Error("authorized target alias is invalid");
        }
        const hostKeyFingerprints = requireStringArray(
          target.host_key_fingerprints,
          "authorized target host key fingerprints",
        );
        if (
          hostKeyFingerprints.length === 0 ||
          hostKeyFingerprints.some(
            (fingerprint) =>
              !fingerprint.startsWith("SHA256:") || /\s/u.test(fingerprint),
          )
        ) {
          throw new Error("authorized target host key fingerprints are invalid");
        }
        targets.push({
          kind: "ssh",
          alias,
          path: requireString(target.path, "authorized target path"),
          user: requireString(target.user, "authorized target user"),
          host: requireString(target.host, "authorized target host"),
          port: requireInteger(target.port, "authorized target port"),
          hostKeyFingerprints,
        });
        continue;
      }
      throw new Error("authorized target kind is invalid");
    }
  }
  return targets;
}

function requireStringArray(value: unknown, name: string): string[] {
  if (
    !Array.isArray(value) ||
    value.some((item) => typeof item !== "string" || item.length === 0)
  ) {
    throw new Error(`${name} must be an array of non-empty strings`);
  }
  return value;
}
