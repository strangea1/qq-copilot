# 部署指南

本文说明如何在 Windows 上从源码构建、安装并运行 QQ Copilot AHP Bridge。

## 1. 部署模型

生产模式使用 VS Code 托管的 Agent Host：

```text
VS Code Agents Window ───────────────┐
                                     ├── VS Code Agent Host
QQ Bot ↔ qq-bridge ↔ AHP Adapter ────┘
```

- VS Code 必须保持运行。
- QQ 与 VS Code 共享一个已绑定的 Agent Host Session。
- Bridge、Adapter 和状态栏扩展安装在 Agent 可编辑工作区之外。
- 旧 Hooks/MCP 链只作为回滚模式。

## 2. 系统要求

- Windows 10/11，推荐使用非管理员标准用户。
- VS Code 1.135，或已完成兼容测试的后续版本。
- GitHub Copilot 已登录。
- Rust 1.89 或更高版本。
- Node.js 24.18 或更高版本。
- Git。
- QQ Bot 应用具备：
  - C2C 单聊事件。
  - `GROUP_AND_C2C_EVENT (1 << 25)`。
  - `INTERACTION (1 << 26)`。
  - Markdown。
  - 自定义 Keyboard。

如果没有自定义 Keyboard 权限，审批和选择题仍可以通过文本命令完成。

## 3. 获取源码

```powershell
git clone https://github.com/strangea1/qq-copilot.git
Set-Location .\qq-copilot
```

## 4. 构建固定 AHP 客户端

VS Code 1.135 实际使用的 AHP revision 是：

```text
f770e26b8483de59050e8de71b65a20efdab62d4
```

仓库已经包含生成后的固定 tarball。需要重新生成时运行：

```powershell
.\scripts\vendor-ahp-client.ps1
```

预期 SHA-256：

```text
d17e139368c0c9d97a86abe68ac2d1f111b5215710fab7106fa5aa907dcb17b0
```

不要用 npm registry 中同名 `0.8.0` 包替换。其版本号相同，但 wire types
并不与 VS Code 1.135 完全一致。

## 5. 构建和验证

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo build --release --locked

npm ci --prefix adapter
npm run typecheck --prefix adapter
npm test --prefix adapter
npm run build --prefix adapter

npm ci --prefix vscode-extension
npm run typecheck --prefix vscode-extension
npm run build --prefix vscode-extension
```

## 6. 安装

示例使用 `C:\test` 作为共享工作区：

```powershell
.\scripts\install.ps1 `
  -Workspace "C:\test" `
  -AhpWorkspace "C:\test"
```

如果还要允许其他工作区：

```powershell
.\scripts\install.ps1 `
  -Workspace "C:\test","C:\src\another-project" `
  -AhpWorkspace "C:\test"
```

默认安装位置：

```text
%LOCALAPPDATA%\Programs\CopilotQQBridge
%LOCALAPPDATA%\CopilotQQBridge\config.toml
%USERPROFILE%\.vscode\extensions\guoyu-local.qq-copilot-ahp-status-0.1.0
```

安装器会收紧安装目录和配置目录 ACL，并把 Node 与 Adapter 的绝对路径写入配置。

## 7. 配置 QQ

编辑：

```text
%LOCALAPPDATA%\CopilotQQBridge\config.toml
```

至少设置：

```toml
[qq]
app_id = "你的 QQ Bot AppID"
app_secret_source = "credential_manager"
intents = 100663296
approval_buttons_enabled = true
```

不要把 AppSecret 写入 TOML。交互式存入 Windows Credential Manager：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" store-secret
```

成功提示：

```text
QQ AppSecret stored and verified in Windows Credential Manager.
```

## 8. 启动与 Owner 绑定

前台启动用于首次验证：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" run
```

首次运行会显示一次性绑定码。在 QQ Bot 单聊发送：

```text
/bind <绑定码>
```

只有事件中的 `author.user_openid` 会成为 Owner；正文中的任何 OpenID 都不会被信任。

## 9. 启用 AHP 模式

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\switch-vscode-integration.ps1" `
  -Mode Ahp
```

然后在 VS Code 运行：

```text
Developer: Reload Window
```

右下角应显示：

```text
QQ AHP 已连接
```

## 10. 创建并绑定 Session

1. 运行 `code --agents`。
2. 在 Agents Window 新建 Copilot Session。
3. 工作目录选择 `C:\test`。
4. 提交并完成至少一轮消息。
5. 列出 Session：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" ahp-sessions
```

6. 使用输出中的精确 endpoint ID 和 Session URI 绑定：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" ahp-bind `
  --endpoint <endpoint-id> `
  --session <session-uri>
```

绑定完成后，QQ 发送普通文本即可进入同一个 Session。

## 11. Session 切换

QQ 中发送：

```text
/switch
```

Bot 会展示共享工作区内全部可用 Session 按钮。只有满足以下条件时才允许切换：

- 当前 Turn 已结束。
- 没有排队消息。
- 没有 Pending 审批。
- 没有 Pending 澄清输入。

文本兜底：

```text
/sessions
/switch <Session短码>
```

短码会跨 Adapter 重启保持稳定。单页最多 25 个 Session，最多 4 页。

## 12. QQ 命令

```text
普通文本
/ask <文本>
/sessions
/switch
/switch <Session短码>
/allow <审批码>
/deny <审批码>
/answer <问题码> <文本>
/cancel
/status
/help
```

## 13. 通知配置

默认工具通知为精简模式：

```toml
[ahp]
tool_notification_mode = "compact"
typing_indicator_enabled = true
typing_duration_seconds = 60
typing_refresh_seconds = 45
```

- `compact`：只在工具完成或取消后通知一次，审批始终通知。
- `full`：通知工具全部状态变化。
- Turn 执行时显示 QQ 官方“正在输入”状态。
- 等待审批或澄清时暂停输入状态。

修改配置后重启 Bridge。

## 14. 登录后自动启动

完成前台验收后：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\register-startup.ps1"
```

计划任务以当前用户和 `LIMITED` 权限启动 Bridge。Bridge 负责监督 Adapter。

## 15. 升级

升级前：

1. 备份 `%LOCALAPPDATA%\CopilotQQBridge`。
2. 保留 Windows Credential Manager 中的 AppSecret。
3. 运行完整测试。
4. 重新执行安装脚本。
5. 重载 VS Code。
6. 验证 `qq-bridge status` 中：
   - `qq_gateway.state = connected`
   - `ahp.adapter.state = connected`
   - `ahp.binding.state = bound`
   - `pending_commands = 0`

VS Code 自动升级后如果 AHP Schema 不兼容，Adapter 会对相关 Host 降为只读。
完成兼容性测试前，不要强行恢复 QQ 写入或审批。

## 16. 回滚到 Legacy

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\switch-vscode-integration.ps1" `
  -Mode Legacy
```

回滚会：

- 停止 AHP Adapter。
- 恢复用户级 `qq-copilot` MCP Server。
- 恢复 QQ Remote Supervisor Custom Agent。
- 移除 AHP 状态栏扩展。

恢复 AHP：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\switch-vscode-integration.ps1" `
  -Mode Ahp
```

每次切换后运行 `Developer: Reload Window`。

## 17. 故障排查

### 状态栏显示未就绪

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" status
```

检查 Gateway、Adapter、binding 和 pending projection 数量。

### 找不到 Session

- 确保使用独立 Agents Window，而不是普通 Chat 面板。
- 确保 Session 完成过至少一轮消息。
- 确保 Session 工作目录与 `ahp.shared_workspace` 一致。

### QQ 按钮不可用

- 确认 Bot 具有 `INTERACTION` Intent。
- 确认自定义 Keyboard 权限已开通。
- 使用 `/allow`、`/deny` 或直接回复选项文本作为兜底。

### QQ 没有实时收到 PC 消息

- 检查 `qq_gateway.state`。
- 检查状态栏的“待补发”数量。
- Owner 下一次发送 QQ 消息时，Bridge 会把遗漏内容合并进被动回复。

### VS Code 关闭后 QQ 无法继续

这是编辑器托管 Agent Host 模式的预期边界。重新打开 VS Code 后，Adapter 会发现新
Host 并恢复已绑定 Session。进行中的 Turn 不会自动重放。
