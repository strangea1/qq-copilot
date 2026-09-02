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
- QQ 与 VS Code 最多同时共享 5 个已监控的 Agent Host Session；一个作为 QQ 前台，
  其余可继续后台运行。
- Bridge、Adapter 和状态栏扩展安装在 Agent 可编辑工作区之外。
- 旧 Hooks/MCP 链只作为回滚模式。

## 2. 系统要求

- Windows 10/11，推荐使用非管理员标准用户。
- VS Code 1.136，或已完成兼容测试的后续版本。
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

VS Code 1.136 实际使用的 AHP 类型/reducer revision 是：

```text
a0bc67f840788f816c9b44bb1325181cb4c4661d
```

该版本在 endpoint 和握手中宣告 `0.9.0`。仓库的
`vendor/ahp-vscode-1.136.patch` 复现 VS Code stable 的 registry overlay，
并包含生成后的固定 tarball。需要重新生成时运行：

```powershell
.\scripts\vendor-ahp-client.ps1 -Force
```

预期 SHA-256：

```text
575eef7a2a166b08b804c56768cc727c65cf8be0e6d080fb2381affed8495185
```

不要用 npm registry 中同名 `0.8.0` 包替换。其版本号相同，但 wire types
并不与 VS Code 1.136 完全一致。

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

示例同时配置两个 QQ 可见的目标目录：

```powershell
.\scripts\install.ps1 `
  -Workspace "C:\test","C:\src\another-project" `
  -AhpWorkspace "C:\test","C:\src\another-project"
```

`-Workspace` 设置安全允许根目录，`-AhpWorkspace` 设置 QQ 可见的精确目标目录。每个
AHP 目标目录也会自动加入安全允许根目录。升级已有安装时，应向 `-AhpWorkspace`
传入希望保留的完整目标目录列表；该参数会重设 AHP 目标列表，而不是只追加本次参数。

读取旧配置后，单值 `ahp.shared_workspace` 会在下一次保存时自动迁移为：

```toml
[ahp]
shared_workspaces = [
    'C:\test',
    'C:\src\another-project',
]
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
voice_input_enabled = true
```

不要把 AppSecret 写入 TOML。交互式存入 Windows Credential Manager：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" store-secret
```

成功提示：

```text
QQ AppSecret stored and verified in Windows Credential Manager.
```

`voice_input_enabled = true` 启用 QQ 私聊语音输入。Bridge 使用
`C2C_MESSAGE_CREATE.attachments[].asr_refer_text` 中的 QQ 内置 ASR 结果，不需要
腾讯云 ASR 凭证，也不会下载或保存原始语音。语音结果只作为普通共享会话输入或澄清
回答；审批、取消和 Session 切换仍要求文字命令或按钮。

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

## 10. 增加目标目录并绑定 Session

安装后增加目录时，使用托管脚本完成路径规范化、存在性校验、配置去重、安全空闲检查和
Bridge/Adapter 重启。`-Workspace` 接受一个或多个目录：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\add-workspace.ps1" `
  -Workspace "C:\src\project-a","C:\src\project-b"
```

目标目录按精确路径匹配；只添加共同父目录不会展示其子目录中的 Session。脚本要求
Bridge 正在运行且已配置 AHP，并会拒绝在任一 Binding 存在活动 Turn、排队消息、
Pending 审批/澄清或 Adapter 命令时重启。

只需要写入配置并稍后手动重启时，可运行：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" `
  add-workspace `
  --workspace "C:\src\project-a" `
  --workspace "C:\src\project-b"

& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\switch-vscode-integration.ps1" `
  -Mode Ahp
```

1. 运行 `code --agents`。
2. 在 Agents Window 新建 Copilot Session。
3. 工作目录选择任一 `ahp.shared_workspaces` 精确目标目录。
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

QQ `/sessions` 的示例输出：

```text
AHP Sessions（最多后台监控 5 个）:
* APYZB | Clone and deploy qq-copilot | D:\work\vscode\qq-copilot | 前台
  AZ49U | 接入 GitHub Copilot 功能实现 | D:\work\vscode\CodexPlusPlus | 后台 · 运行中
  A7K2P | 新任务 | D:\work\vscode\project-c | 未监控 · 空闲
```

## 11. 多 Session 路由

QQ 中发送：

```text
/switch
```

Bot 会同时展示所有目标目录内的 Session、完整所在目录，以及前台、后台、运行中和
未监控状态。`/switch` 只改变 QQ 普通文本和语音的默认路由，不会停止旧 Session，
当前或目标 Session 正在运行时也可以切换前台。

常用命令：

```text
/sessions
/switch <Session短码>
/send <Session短码> <文本>
/cancel <Session短码>
/detach <Session短码>
```

Bridge 自动维护最近活跃的 5 个 Binding。前台、活动 Turn、排队消息、待执行命令、
Pending 审批和 Pending 澄清不会被 LRU 淘汰；若 5 个槽位全部受保护，新的 `/switch`
或 `/send` 会明确失败。`/detach` 只对安全空闲的后台 Session 生效。

短码会跨 Adapter 重启保持稳定。单页最多 25 个 Session，最多 4 页；非目标目录的
Session 不会显示，也不能通过旧按钮切换。Assistant 过程段会在形成后立即显示来源短码
与标题，且不会在最终回复中重复；审批、澄清和最终回复还会附加独立的前台切换按钮。
这些通知都不会自动抢占当前前台。

同一个精确工作区出现第二个活动 Turn 时，Bridge 会警告潜在文件/Git 索引冲突，但不会
阻断执行。有写操作的并发任务应使用独立 Git worktree。

## 12. QQ 命令

```text
普通文本
QQ 私聊语音
/ask <文本>
/send <Session短码> <文本>
/sessions
/switch
/switch <Session短码>
/detach <Session短码>
/allow <审批码>
/deny <审批码>
/answer <问题码> <文本>
/cancel
/cancel <Session短码>
/notify
/notify <approval_only|compact|full>
/status
/help
```

## 13. 通知配置

Owner 可以直接在 QQ 查询或切换，切换会立即生效并持久化：

```text
/notify
/notify approval_only
/notify compact
/notify full
```

- `approval_only`：不通知工具状态；仅在工具需要审批时发送审批，并保留完整 Assistant 过程段和最终回复。
- `compact`：只在工具完成或取消后通知一次，审批始终通知。
- `full`：通知工具全部状态变化。
- Turn 执行时显示 QQ 官方“正在输入”状态。
- 等待审批或澄清时暂停输入状态。
- QQ 回答澄清后，Host 的本端确认只更新状态，不再误报“已由另一端处理”；PC 端回答仍通知 QQ。

本机也可以编辑默认配置：

```toml
[ahp]
tool_notification_mode = "compact"
typing_indicator_enabled = true
typing_duration_seconds = 60
typing_refresh_seconds = 45
```

直接编辑 TOML 后需要重启 Bridge；通过 `/notify` 切换不需要。

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
4. IPC v2 为 Bridge/Adapter 多 Binding 契约，必须由同一次安装成对升级，不能混用
   新 Bridge 与旧 Adapter。
5. 重新执行安装脚本，并向 `-AhpWorkspace` 传入要保留的完整目标目录列表。
6. 重载 VS Code。
7. 验证 `qq-bridge status` 中：
   - `qq_gateway.state = connected`
   - `ahp.adapter.state = connected`
   - `ahp.bindings` 中每个被监控 Binding 的 `state = bound`
   - `ahp.foreground_binding_id` 指向预期前台
   - `pending_commands = 0`
   - `pending_approvals = 0`、`pending_inputs = 0`（无待处理交互时）

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

检查 Gateway、Adapter、全部 bindings、foreground binding、pending interaction 和
pending projection 数量。

### 找不到 Session

- 确保使用独立 Agents Window，而不是普通 Chat 面板。
- 确保 Session 完成过至少一轮消息。
- 确保 Session 工作目录与 `ahp.shared_workspaces` 中某个目标目录精确一致。
- 不要只配置共同父目录；每个实际 Session 目录都需要单独加入目标列表。
- 运行 `qq-bridge ahp-sessions`，确认 Adapter 已发现该 Session 及其 `workspace_uris`。
- 新增目录后若使用的是 `qq-bridge add-workspace`，需要重启 Bridge/Adapter。

### Session 无法切换或加入监控

- 前台切换不要求 Session 空闲。若失败，先确认短码仍存在且 Session 工作区仍与可信目录
  精确匹配。
- 配置变更、Session 目录变更、按钮过期或重复点击后，应重新发送 `/switch` 生成菜单。
- 如果提示容量不足，说明 5 个槽位全部被前台、活动 Turn、排队消息或 Pending 交互保护；
  等待任务结束，或对安全空闲的后台 Session 使用 `/detach <短码>`。
- 使用 `/status` 确认所有 Binding 最终为 `bound` 且 `pending_commands = 0`。

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
Host 并恢复所有已监控 Session。进行中的 Turn 不会自动重放。
