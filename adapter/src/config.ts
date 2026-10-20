import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve, win32 } from "node:path";

import { parse } from "smol-toml";

import { defaultUserDataDirectory } from "./endpoint-registry.js";

export interface AdapterConfig {
  readonly configPath: string;
  readonly bridgePipePath: string;
  readonly bridgeToken: string;
  readonly adapterId: string;
  readonly userDataDirectory: string;
  readonly pollSeconds: number;
  readonly codeExecutable?: string;
  readonly sshExecutable?: string;
  readonly codexExecutable?: string;
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
  const codeExecutable =
    typeof ahp.code_executable === "string" && ahp.code_executable.length > 0
      ? ahp.code_executable
      : undefined;
  const sshExecutable =
    typeof ahp.ssh_executable === "string" && ahp.ssh_executable.length > 0
      ? ahp.ssh_executable
      : undefined;
  const authorizedTargets = parseConfiguredTargets(ahp);
  const codexExecutable = ahp.codex_executable;
  if (codexExecutable !== undefined &&
      (typeof codexExecutable !== "string" ||
       !win32.isAbsolute(codexExecutable) || !/\.exe$/iu.test(codexExecutable))) {
    throw new Error("ahp.codex_executable must be an absolute native Windows .exe path");
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
    ...(codeExecutable ? { codeExecutable: resolve(codeExecutable) } : {}),
    ...(sshExecutable ? { sshExecutable: resolve(sshExecutable) } : {}),
    ...(typeof codexExecutable === "string" ? { codexExecutable } : {}),
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

function parseConfiguredTargets(
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
    targets.push(...parseAuthorizedTargets(ahp.authorized_targets, true));
  }
  return targets;
}

export function parseAuthorizedTargets(
  value: unknown,
  resolveLocalPaths = false,
): readonly AuthorizedTargetConfig[] {
  if (!Array.isArray(value)) {
    throw new Error("authorized_targets must be an array");
  }
  return value.map((item): AuthorizedTargetConfig => {
    const target = requireTable(item, "authorized_targets[]");
    const path = requireTargetString(target.path, "authorized target path");
    if (target.kind === "local") {
      if (
        !resolveLocalPaths &&
        !/^[A-Za-z]:[\\/]/u.test(path) &&
        !/^\\\\[^\\]+\\[^\\]+/u.test(path)
      ) {
        throw new Error("authorized local target path must be absolute");
      }
      return {
        kind: "local",
        path: resolveLocalPaths ? resolve(path) : win32.resolve(path),
      };
    }
    if (target.kind !== "ssh") {
      throw new Error("authorized target kind is invalid");
    }
    const alias = requireTargetString(target.alias, "authorized target alias");
    if (alias.startsWith("-") || !/^[A-Za-z0-9._-]+$/u.test(alias)) {
      throw new Error("authorized target alias is invalid");
    }
    if (!path.startsWith("/")) {
      throw new Error("authorized SSH target path must be absolute");
    }
    const hostKeyFingerprints = requireStringArray(
      target.host_key_fingerprints,
      "authorized target host key fingerprints",
    );
    if (
      hostKeyFingerprints.length === 0 ||
      hostKeyFingerprints.some(
        (fingerprint) =>
          !fingerprint.startsWith("SHA256:") ||
          fingerprint.length === "SHA256:".length ||
          /\s/u.test(fingerprint),
      )
    ) {
      throw new Error("authorized target host key fingerprints are invalid");
    }
    const port = requireInteger(target.port, "authorized target port");
    if (port < 1 || port > 65_535) {
      throw new Error("authorized target port is out of range");
    }
    return {
      kind: "ssh",
      alias,
      path,
      user: requireTargetString(target.user, "authorized target user"),
      host: requireTargetString(target.host, "authorized target host"),
      port,
      hostKeyFingerprints: [...new Set(hostKeyFingerprints)].sort(),
    };
  });
}

function requireTargetString(value: unknown, name: string): string {
  const result = requireString(value, name);
  if (!result.trim() || /[\0\r\n]/u.test(result)) {
    throw new Error(`${name} is invalid`);
  }
  return result;
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
