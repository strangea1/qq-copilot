import { execFile } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";

import * as vscode from "vscode";

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
  readonly pending_commands: number;
  readonly pending_approvals?: number;
  readonly pending_inputs?: number;
  readonly pending_projections: number;
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
  readonly error?: string;
}

class StatusController implements vscode.Disposable {
  readonly #item = vscode.window.createStatusBarItem(
    vscode.StatusBarAlignment.Right,
    100,
  );

  #timer: NodeJS.Timeout | undefined;

  #refreshing: Promise<void> | undefined;

  #status: CombinedStatus = unavailableStatus("not yet checked");

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
      const legacy = await runBridgeCommand<LegacyStatus>("status");
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
      };
    } catch (error) {
      this.#status = unavailableStatus(errorCode(error));
    }
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
        `**待补发:** ${status.pendingProjections}`,
      ].join("  \n"),
    );
  }
}

export function activate(context: vscode.ExtensionContext): void {
  const controller = new StatusController();
  context.subscriptions.push(
    controller,
    vscode.commands.registerCommand("qqCopilot.refreshStatus", () =>
      controller.refresh(),
    ),
    vscode.commands.registerCommand("qqCopilot.showStatus", () =>
      controller.show(),
    ),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("qqCopilot")) {
        controller.restartTimer();
      }
    }),
  );
}

export function deactivate(): void {}

async function runBridgeCommand<T>(
  command: "status",
): Promise<T> {
  const config = vscode.workspace.getConfiguration("qqCopilot");
  const localAppData = process.env.LOCALAPPDATA;
  if (!localAppData) {
    throw new Error("LOCALAPPDATA is not set");
  }
  const bridge =
    config.get<string>("bridgeExecutable") ||
    join(localAppData, "Programs", "CopilotQQBridge", "qq-bridge.exe");
  const configPath =
    config.get<string>("configPath") ||
    join(localAppData, "CopilotQQBridge", "config.toml");
  const result = await execFileAsync(
    bridge,
    ["--config", configPath, command],
    {
      windowsHide: true,
      timeout: 5_000,
      maxBuffer: MAX_OUTPUT_BYTES,
      encoding: "utf8",
    },
  );
  return JSON.parse(result.stdout) as T;
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
