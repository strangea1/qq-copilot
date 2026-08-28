# QQ Copilot AHP Bridge

本项目让 PC 上的 VS Code Agents Window 与手机 QQ Bot 作为两个客户端，共享同一个
VS Code Agent Host Protocol（AHP）Session。

完整安装、升级、回滚和故障排查见 [部署指南](docs/deployment.md)；协议与安全设计见
[远程交互设计](docs/qq-copilot-remote-design.md)。

## 架构

```text
VS Code Agents Window ───────────────┐
                                     ├── VS Code Agent Host（权威会话）
QQ Bot ↔ Rust Bridge ↔ TS Adapter ───┘
             │
             ├── SQLite 事件/审批/outbox
             └── Windows Credential Manager
```

| 组件 | 职责 |
| --- | --- |
| `qq-bridge.exe` | QQ Gateway/OpenAPI、Owner 身份、SQLite、审批仲裁、消息投递和 Adapter 监督 |
| `ahp-adapter` | 连接 VS Code 本地 AHP Named Pipe，订阅并控制绑定的 Session/Chat |
| VS Code Agent Host | 唯一权威会话、工具策略和执行状态 |
| QQ Bot | 手机端共享对话、审批、澄清回答和取消 |
| QQ Copilot AHP Status | VS Code 状态栏显示 Gateway、Adapter、绑定和待补发数量 |

旧的 `copilot-qq-hook.exe`、`qq-mcp.exe` 和 QQ Remote Supervisor Agent 仍随安装保留，
但 AHP 模式会禁用其用户级入口，防止两套链路重复发送或审批。

## 已实现

- PC 和 QQ 的用户消息进入同一个 AHP Chat。
- 完整 Assistant 消息同步到两端；不转发隐藏 reasoning。
- 工具调用只遵循 VS Code Agent Host 原生审批策略。
- 工具执行前审批和工具结果复核同时显示在 PC 与 QQ。
- QQ Keyboard 按钮支持“批准一次”“拒绝”，并保留 `/allow`、`/deny` 文本兜底。
- Boolean 和单选澄清问题使用 QQ 按钮；多选使用逗号分隔文本；自由文本直接回复。
- PC 或 QQ 首个有效审批/回答生效，终态广播给另一端。
- Agent 运行中收到的新消息通过 AHP queued message 串行执行。
- PC 或 QQ 均可取消当前 Turn。
- `/sessions` 以文本列出共享工作区 Session；`/switch` 显示全部 Session
  按钮并在空闲时切换绑定。
- Session 编号由 Bridge 稳定分配，目录刷新不会改变。
- Turn 运行期间 QQ 显示官方 `msg_type=6`“正在输入”状态，每 45 秒续期，
  完成、取消、失败或等待审批/澄清时停止。
- Host/Adapter 重连恢复 Session 订阅；Host 实例更换时未决操作 fail-closed。
- 脱敏事件和未送达 projection 保留 30 天。
- QQ 实时投递失败后，在 Owner 下一条 QQ 消息的被动回复中补发。
- AHP/Legacy 模式一键切换。

## 重要边界

- 首版使用 **VS Code 托管 Agent Host**；VS Code 完全退出后 QQ 不能继续执行。
- 只绑定一个本地 AHP Session 和一个 QQ Owner。
- 同一工作区可以有多个 Session，但任一时刻只绑定一个；运行中、存在排队消息或
  Pending 交互时禁止切换。
- AHP 直接连接针对 VS Code 1.135 的实际协议方言验证。
- 当前 VS Code 1.135 使用 AHP `1.0.0`，对应 vendored revision
  `f770e26b8483de59050e8de71b65a20efdab62d4`。
- npm 上同名 `@microsoft/agent-host-protocol@0.8.0` 不是精确源码；项目通过
  `scripts/vendor-ahp-client.ps1` 生成固定 revision 的本地 tarball。
- VS Code 自动更新后，如果出现未知协议、Snapshot 或 Action，相关 Host 降为只读，
  QQ 不能继续发消息、审批或取消，直到兼容性修复。
- AHP Host 重启后，正在执行的 Turn、审批和澄清请求按失败处理，不自动重放。
- QQ 端只提供单次批准；PC 端仍可使用 VS Code 原生 Session Allow。
- UAC、密码、MFA、系统弹窗和工具认证 Secret 不通过 QQ 处理。
- AHP 是 VS Code 快速演进的接口。本项目固定并验证了 VS Code 1.135 的协议 revision；
  升级 VS Code 后应重新执行兼容性验收。

## 前置条件

- Windows 当前标准用户。
- VS Code 1.135 或经过兼容测试的后续版本。
- Node.js 24.18 或更高版本。
- Rust 1.89 或更高版本（仅源码构建需要）。
- QQ Bot 已开通：
  - `GROUP_AND_C2C_EVENT (1 << 25)`
  - `INTERACTION (1 << 26)`
  - C2C 主动消息
  - Markdown
  - 自定义 Keyboard（邀请制）

没有自定义 Keyboard 权限时，Bridge 自动降级到纯文本 `/allow`、`/deny`。

## 构建与安装

首次构建精确 AHP 客户端：

```powershell
.\scripts\vendor-ahp-client.ps1
```

验证并安装：

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets

npm ci --prefix adapter
npm run typecheck --prefix adapter
npm test --prefix adapter

npm ci --prefix vscode-extension
npm run typecheck --prefix vscode-extension

.\scripts\install.ps1 `
  -Workspace "C:\test","C:\path\to\other-allowed-workspace" `
  -AhpWorkspace "C:\test"
```

详细步骤和首次 QQ/Session 绑定流程见 [部署指南](docs/deployment.md)。

安装位置：

```text
%LOCALAPPDATA%\Programs\CopilotQQBridge
%LOCALAPPDATA%\CopilotQQBridge\config.toml
%USERPROFILE%\.vscode\extensions\guoyu-local.qq-copilot-ahp-status-0.1.0
```

安装目录和配置目录 ACL 会限制到当前用户。

## QQ 配置与绑定

在 `config.toml` 设置 AppID：

```toml
[qq]
app_id = "你的 AppID"
app_secret_source = "credential_manager"
approval_buttons_enabled = true
intents = 100663296
```

交互式保存 AppSecret：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" store-secret
```

AppSecret 只进入 Windows Credential Manager，不写入 TOML、命令行或工作区。

首次启动：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" run
```

然后在 QQ 发送控制台显示的一次性绑定码：

```text
/bind <code>
```

## 创建并绑定共享 Session

1. 运行 `code --agents`。
2. 在独立 Agents Window 中创建 Copilot Session。
3. 选择配置的 `ahp.shared_workspace`。
4. 发送并完成至少一轮消息。
5. 本机列出 Session：

```powershell
qq-bridge ahp-sessions
```

6. 按精确 endpoint ID 和 Session URI 绑定：

```powershell
qq-bridge ahp-bind `
  --endpoint <endpoint-id> `
  --session <session-uri>
```

Adapter 会订阅 Session 和默认 Chat。不能按标题模糊绑定。

## QQ 使用方式

```text
普通文本                 发送到共享对话
/ask <文本>              即使 Agent 正等待澄清，也排队为新消息
/sessions                以文本列出共享工作区内可切换的 Session
/switch                  显示全部 Session 按钮并切换
/switch <编号>           文本兜底，仅在当前 Session 空闲时切换
/allow <审批码>          单次批准
/deny <审批码>           拒绝
/answer <问题码> <文本>  显式回答澄清
/cancel                  取消当前 Turn
/status                  查看 Gateway、Adapter、Session 和待补发状态
/help
```

当 Agent 正等待澄清时，普通文本优先作为当前问题的答案。审批永远要求按钮或显式
`/allow`、`/deny`，普通文本不会被解释为批准。

每次 `/switch` 会生成有效期有限的一次性按钮 token；旧菜单、重复点击、Session
工作区变化以及运行中的 Turn/排队消息/Pending 交互都会拒绝切换。单个 Keyboard
最多 25 个 Session；最多使用 4 条被动回复展示 100 个 Session。

本机也可以使用完整 URI 切换：

```powershell
qq-bridge ahp-sessions
qq-bridge ahp-bind --endpoint <endpoint-id> --session <session-uri>
```

QQ 端只显示稳定短编号，不暴露或要求输入长 Session URI。Adapter 目录短暂断开或
重启不会改变编号。

## 工具通知模式

默认精简模式：

```toml
[ahp]
tool_notification_mode = "compact"
typing_indicator_enabled = true
typing_duration_seconds = 60
typing_refresh_seconds = 45
```

- `compact`：仅工具完成/取消后通知一次；需要审批时立即发送审批。
- `full`：发送 streaming、running、completed、cancelled 等所有状态变化。

修改后重启 Bridge。

## QQ 输入状态

QQ 官方 C2C `msg_type=6` 输入状态用于表示绑定 Chat 正在处理 Turn。默认持续 60 秒，
Bridge 每 45 秒续期。发送完整 Assistant 回复时 QQ 客户端会自动取消该状态。

当 Agent 等待审批或澄清输入时，typing 会暂停；任一端完成交互后恢复。该状态不是
文本消息，不进入对话历史，也不计入工具通知模式。

## VS Code 状态栏

状态栏显示：

- `QQ AHP 已连接`
- `QQ AHP 未就绪`
- `QQ 待补发 N`
- `QQ AHP 离线`

点击状态栏可查看 Gateway、Adapter、Session binding、命令和 projection 数量。

安装或切换模式后运行：

```text
Developer: Reload Window
```

## AHP 与 Legacy 切换

启用 AHP：

```powershell
.\scripts\switch-vscode-integration.ps1 -Mode Ahp
```

回滚 Hooks/MCP：

```powershell
.\scripts\switch-vscode-integration.ps1 -Mode Legacy
```

切换脚本会：

- 备份或恢复用户级 `qq-copilot` MCP Server。
- 备份或恢复 QQ Remote Supervisor Agent。
- 安装或移除 AHP 状态栏扩展。
- 只停止受管安装目录下的 Bridge、MCP 和 Adapter 进程。
- 重启 Bridge。

## 登录后启动

```powershell
.\scripts\register-startup.ps1
```

计划任务只启动 Bridge。Bridge 在 AHP 模式下监督 Adapter，并在 Adapter 异常退出后重启。

## 开发验证

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo build --release --locked

npm run typecheck --prefix adapter
npm test --prefix adapter
npm run build --prefix adapter

npm run typecheck --prefix vscode-extension
npm run build --prefix vscode-extension
```

当前测试覆盖：SQLite 迁移、Owner 身份、事件幂等、Session 绑定、命令租约、Host
更换 fail-closed、双端审批、按钮重放、选择按钮、离线补发、通知模式、AHP reducer、
Named Pipe WebSocket、Session/Chat hydration 和 final 去重。
