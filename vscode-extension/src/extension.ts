import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";

import * as vscode from "vscode";

import {
  AuthBrokerError,
  AuthBrokerServer,
  bridgeIpcTokenFromToml,
  deriveAuthBrokerIdentity,
  type AuthBrokerRequest,
} from "./auth-broker.js";
import {
  matchingTrustRequest,
  userConfigurationValue,
  workspaceTrustCommandArgs,
  type PendingTrustRequest,
} from "./trust-helpers.js";

const execFileAsync = promisify(execFile);
const MAX_OUTPUT_BYTES = 1024 * 1024;

interface LegacyStatus {
  readonly owner_bound: boolean;
  readonly owner_enabled: boolean;
  readonly qq_gateway: {
    readonly state: string;
    readonly last_seen_at?: number;
  };
  readonly ahp?: AhpStatus;
}

interface AhpStatus {
  readonly adapter?: {
    readonly state: string;
    readonly version: string;
  };
  readonly binding?: {
    readonly state: string;
    readonly active_turn_id?: string;
  };
  readonly bindings?: ReadonlyArray<{
    readonly state: string;
    readonly active_turn_id?: string;
  }>;
  readonly creation?: {
    readonly mode: string;
    readonly state: string;
  };
  readonly pending_commands: number;
  readonly pending_approvals?: number;
  readonly pending_inputs?: number;
  readonly pending_projections: number;
}

interface TrustRequestsResponse {
  readonly requests: readonly PendingTrustRequest[];
}

interface CombinedStatus {
  readonly bridgeOnline: boolean;
  readonly ownerEnabled: boolean;
  readonly gatewayState: string;
  readonly adapterState: string;
  readonly bindingState: string;
  readonly bindingsReady: boolean;
  readonly bindingCount: number;
  readonly activeTurnCount: number;
  readonly pendingCommands: number;
  readonly pendingInteractions: number;
  readonly pendingProjections: number;
  readonly creationState: string;
  readonly error?: string;
}

class AuthenticationBrokerController implements vscode.Disposable {
  #server: AuthBrokerServer | undefined;

  #pipePath: string | undefined;

  #starting: Promise<void> | undefined;

  readonly #timer: NodeJS.Timeout;

  #disposed = false;

  #lifecycleGeneration = 0;

  #consentGeneration = 0;

  #startSuspended = false;

  constructor() {
    this.#timer = setInterval(() => this.#scheduleStart(), 5_000);
    this.#timer.unref();
    this.#scheduleStart();
  }

  dispose(): void {
    this.#disposed = true;
    this.#invalidateBroker();
    clearInterval(this.#timer);
  }

  restart(): void {
    if (this.#disposed) {
      return;
    }
    this.#startSuspended = false;
    this.#invalidateBroker();
    this.#scheduleStart();
  }

  async enable(): Promise<void> {
    const choice = await vscode.window.showWarningMessage(
      "Enable QQ Copilot managed Session authentication? The local Adapter may request a GitHub token directly from VS Code. Tokens stay in memory and are never sent through QQ or Bridge commands.",
      { modal: true },
      "Enable",
    );
    if (choice !== "Enable") {
      return;
    }
    await vscode.authentication.getSession(
      "github",
      ["read:user", "user:email"],
      { createIfNone: true },
    );
    await vscode.workspace
      .getConfiguration("qqCopilot")
      .update(
        "managedSessionAuthentication",
        true,
        vscode.ConfigurationTarget.Global,
      );
    this.restart();
    await vscode.window.showInformationMessage(
      "QQ Copilot managed Session authentication is enabled.",
    );
  }

  async disable(): Promise<void> {
    this.#startSuspended = true;
    this.#invalidateBroker();
    await vscode.workspace
      .getConfiguration("qqCopilot")
      .update(
        "managedSessionAuthentication",
        false,
        vscode.ConfigurationTarget.Global,
      );
    this.#startSuspended = false;
    this.#scheduleStart();
    await vscode.window.showInformationMessage(
      "QQ Copilot managed Session authentication is disabled.",
    );
  }

  #scheduleStart(): void {
    if (
      this.#disposed ||
      this.#startSuspended ||
      this.#starting ||
      !this.#authenticationEnabled()
    ) {
      return;
    }
    const generation = this.#lifecycleGeneration;
    this.#starting = this.#ensureStarted(generation)
      .catch((error) => {
        console.warn(
          `QQ Copilot authentication broker unavailable: ${errorCode(error)}`,
        );
      })
      .finally(() => {
        this.#starting = undefined;
        if (
          !this.#disposed &&
          generation !== this.#lifecycleGeneration
        ) {
          this.#scheduleStart();
        }
      });
  }

  async #ensureStarted(generation: number): Promise<void> {
    const { configPath } = installationPaths();
    const bridgeToken = bridgeIpcTokenFromToml(
      await readFile(configPath, "utf8"),
    );
    const identity = deriveAuthBrokerIdentity(bridgeToken);
    if (this.#disposed || generation !== this.#lifecycleGeneration) {
      return;
    }
    if (this.#server && this.#pipePath === identity.pipePath) {
      return;
    }
    this.#server?.dispose();
    this.#server = undefined;
    this.#pipePath = undefined;
    const server = new AuthBrokerServer(
      identity,
      (request) => this.#resolveAuthentication(request),
      (error) => this.#handleServerError(server, error),
    );
    const listening = await server.start();
    if (this.#disposed || generation !== this.#lifecycleGeneration) {
      server.dispose();
      return;
    }
    if (listening) {
      this.#server = server;
      this.#pipePath = identity.pipePath;
    }
  }

  #handleServerError(server: AuthBrokerServer, error: Error): void {
    console.warn(
      `QQ Copilot authentication broker stopped: ${errorCode(error)}`,
    );
    if (this.#server !== server) {
      return;
    }
    this.#server = undefined;
    this.#pipePath = undefined;
    this.#scheduleStart();
  }

  async #resolveAuthentication(
    request: AuthBrokerRequest,
  ): Promise<{ readonly accessToken: string }> {
    const consentGeneration = this.#consentGeneration;
    if (!this.#authenticationEnabled()) {
      throw new AuthBrokerError(
        "consent_required",
        "Managed Session authentication consent is required",
      );
    }
    const session = await vscode.authentication.getSession(
      "github",
      [...request.scopes],
      { createIfNone: true },
    );
    if (
      consentGeneration !== this.#consentGeneration ||
      !this.#authenticationEnabled()
    ) {
      throw new AuthBrokerError(
        "consent_required",
        "Managed Session authentication consent was revoked",
      );
    }
    if (!session.accessToken) {
      throw new AuthBrokerError(
        "authentication_unavailable",
        "GitHub authentication is unavailable",
      );
    }
    return { accessToken: session.accessToken };
  }

  #authenticationEnabled(): boolean {
    return (
      vscode.workspace
        .getConfiguration("qqCopilot")
        .inspect<boolean>("managedSessionAuthentication")?.globalValue === true
    );
  }

  #invalidateBroker(): void {
    this.#lifecycleGeneration += 1;
    this.#consentGeneration += 1;
    this.#server?.dispose();
    this.#server = undefined;
    this.#pipePath = undefined;
  }
}

class StatusController implements vscode.Disposable {
  readonly #item = vscode.window.createStatusBarItem(
    vscode.StatusBarAlignment.Right,
    100,
  );

  #timer: NodeJS.Timeout | undefined;

  #refreshing: Promise<void> | undefined;

  #status: CombinedStatus = unavailableStatus("not yet checked");

  readonly #openedTrustRequests = new Set<string>();

  constructor() {
    this.#item.name = "QQ Copilot AHP";
    this.#item.command = "qqCopilot.showStatus";
    this.#item.show();
    this.restartTimer();
  }

  dispose(): void {
    if (this.#timer) {
      clearInterval(this.#timer);
    }
    this.#item.dispose();
  }

  restartTimer(): void {
    if (this.#timer) {
      clearInterval(this.#timer);
    }
    const seconds = vscode.workspace
      .getConfiguration("qqCopilot")
      .get<number>("statusPollSeconds", 5);
    this.#timer = setInterval(() => void this.refresh(), seconds * 1_000);
    this.#timer.unref();
    void this.refresh();
  }

  refresh(): Promise<void> {
    if (this.#refreshing) {
      return this.#refreshing;
    }
    this.#refreshing = this.#refreshInner().finally(() => {
      this.#refreshing = undefined;
    });
    return this.#refreshing;
  }

  show(): void {
    const status = this.#status;
    const lines = [
      `Bridge: ${status.bridgeOnline ? "online" : "offline"}`,
      `QQ Gateway: ${status.gatewayState}`,
      `AHP Adapter: ${status.adapterState}`,
      `Session bindings: ${status.bindingState}`,
      `Active Turns: ${status.activeTurnCount}`,
      `Creation: ${status.creationState}`,
      `Pending Adapter commands: ${status.pendingCommands}`,
      `Pending interactions: ${status.pendingInteractions}`,
      `Pending QQ deliveries: ${status.pendingProjections}`,
    ];
    if (status.error) {
      lines.push(`Error: ${status.error}`);
    }
    void vscode.window.showInformationMessage(lines.join(" | "));
  }

  async #refreshInner(): Promise<void> {
    try {
      const legacy = await runBridgeCommand<LegacyStatus>(["status"]);
      const ahp = legacy.ahp;
      const bindings = ahp?.bindings ?? (ahp?.binding ? [ahp.binding] : []);
      const boundCount = bindings.filter((binding) => binding.state === "bound").length;
      const bindingCount = bindings.length;
      this.#status = {
        bridgeOnline: true,
        ownerEnabled: legacy.owner_bound && legacy.owner_enabled,
        gatewayState: legacy.qq_gateway.state,
        adapterState: ahp?.adapter?.state ?? "not registered",
        bindingState:
          bindingCount === 0
            ? "unbound"
            : boundCount === bindingCount
              ? `${bindingCount} bound`
              : `${boundCount}/${bindingCount} bound`,
        bindingsReady:
          bindingCount > 0 && bindings.every((binding) => binding.state === "bound"),
        bindingCount,
        activeTurnCount: bindings.filter((binding) => binding.active_turn_id).length,
        pendingCommands: ahp?.pending_commands ?? 0,
        pendingInteractions:
          (ahp?.pending_approvals ?? 0) + (ahp?.pending_inputs ?? 0),
        pendingProjections: ahp?.pending_projections ?? 0,
        creationState: ahp?.creation
          ? `${ahp.creation.mode}/${ahp.creation.state}`
          : "none",
      };
    } catch (error) {
      this.#status = unavailableStatus(errorCode(error));
    }
    await this.#syncTrustState();
    this.#render();
  }

  #render(): void {
    const status = this.#status;
    if (!status.bridgeOnline) {
      this.#item.text = "$(debug-disconnect) QQ AHP 离线";
      this.#item.backgroundColor = new vscode.ThemeColor(
        "statusBarItem.errorBackground",
      );
    } else if (status.pendingProjections > 0) {
      this.#item.text = `$(warning) QQ 待补发 ${status.pendingProjections}`;
      this.#item.backgroundColor = new vscode.ThemeColor(
        "statusBarItem.warningBackground",
      );
    } else if (
      !status.ownerEnabled ||
      status.gatewayState !== "connected" ||
      status.adapterState !== "connected" ||
      !status.bindingsReady
    ) {
      this.#item.text = "$(warning) QQ AHP 未就绪";
      this.#item.backgroundColor = new vscode.ThemeColor(
        "statusBarItem.warningBackground",
      );
    } else {
      this.#item.text = `$(radio-tower) QQ AHP 已连接 ${status.bindingCount}`;
      this.#item.backgroundColor = undefined;
    }
    this.#item.tooltip = new vscode.MarkdownString(
      [
        `**QQ Gateway:** ${status.gatewayState}`,
        `**AHP Adapter:** ${status.adapterState}`,
        `**Sessions:** ${status.bindingState}（运行中 ${status.activeTurnCount}）`,
        `**待交互:** ${status.pendingInteractions}`,
        `**创建:** ${status.creationState}`,
        `**待补发:** ${status.pendingProjections}`,
      ].join("  \n"),
    );
  }

  async #syncTrustState(): Promise<void> {
    const workspaceUris =
      vscode.workspace.workspaceFolders?.map((folder) =>
        folder.uri.toString(),
      ) ?? [];
    if (!workspaceUris.length) {
      return;
    }
    const trustArgs = workspaceTrustCommandArgs(
      workspaceUris,
      vscode.workspace.isTrusted,
    );
    await runBridgeCommand<{ readonly updated: number }>(trustArgs).catch(
      () => undefined,
    );
    const requests = await runBridgeCommand<TrustRequestsResponse>([
      "trust-requests",
    ]).catch(() => undefined);
    if (!requests) {
      return;
    }
    const matching = matchingTrustRequest(
      workspaceUris,
      vscode.workspace.isTrusted,
      requests.requests,
      this.#openedTrustRequests,
    );
    if (
      matching &&
      !vscode.workspace.isTrusted &&
      !this.#openedTrustRequests.has(matching.request_id)
    ) {
      this.#openedTrustRequests.add(matching.request_id);
      await vscode.commands.executeCommand("workbench.trust.manage");
    }
  }
}

export function activate(context: vscode.ExtensionContext): void {
  const controller = new StatusController();
  const authenticationBroker = new AuthenticationBrokerController();
  context.subscriptions.push(
    controller,
    authenticationBroker,
    vscode.commands.registerCommand("qqCopilot.refreshStatus", () =>
      controller.refresh(),
    ),
    vscode.commands.registerCommand("qqCopilot.showStatus", () =>
      controller.show(),
    ),
    vscode.commands.registerCommand(
      "qqCopilot.enableManagedSessionAuthentication",
      () => authenticationBroker.enable(),
    ),
    vscode.commands.registerCommand(
      "qqCopilot.disableManagedSessionAuthentication",
      () => authenticationBroker.disable(),
    ),
    vscode.workspace.onDidGrantWorkspaceTrust(() => controller.refresh()),
    vscode.workspace.onDidChangeWorkspaceFolders(() => controller.refresh()),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("qqCopilot")) {
        controller.restartTimer();
        authenticationBroker.restart();
      }
    }),
  );
}

export function deactivate(): void {}

async function runBridgeCommand<T>(
  args: readonly string[],
): Promise<T> {
  const { bridge, configPath } = installationPaths();
  const result = await execFileAsync(
    bridge,
    ["--config", configPath, ...args],
    {
      windowsHide: true,
      timeout: 5_000,
      maxBuffer: MAX_OUTPUT_BYTES,
      encoding: "utf8",
    },
  );
  return JSON.parse(result.stdout) as T;
}

function installationPaths(): {
  readonly bridge: string;
  readonly configPath: string;
} {
  const config = vscode.workspace.getConfiguration("qqCopilot");
  const localAppData = process.env.LOCALAPPDATA;
  if (!localAppData) {
    throw new Error("LOCALAPPDATA is not set");
  }
  const bridge =
    userConfigurationValue(config.inspect<string>("bridgeExecutable")) ||
    join(localAppData, "Programs", "CopilotQQBridge", "qq-bridge.exe");
  const configPath =
    userConfigurationValue(config.inspect<string>("configPath")) ||
    join(localAppData, "CopilotQQBridge", "config.toml");
  return { bridge, configPath };
}

function unavailableStatus(error: string): CombinedStatus {
  return {
    bridgeOnline: false,
    ownerEnabled: false,
    gatewayState: "unknown",
    adapterState: "unknown",
    bindingState: "unknown",
    bindingsReady: false,
    bindingCount: 0,
    activeTurnCount: 0,
    pendingCommands: 0,
    pendingInteractions: 0,
    pendingProjections: 0,
    creationState: "unknown",
    error,
  };
}

function errorCode(error: unknown): string {
  if (error instanceof Error) {
    const code =
      "code" in error && typeof error.code === "string"
        ? error.code
        : error.name;
    return code.replace(/[^A-Za-z0-9_.-]/gu, "-").slice(0, 80);
  }
  return "unknown";
}
