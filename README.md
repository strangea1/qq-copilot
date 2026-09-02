# QQ Copilot AHP Bridge

本项目让 PC 上的 VS Code Agents Window 与手机 QQ Bot 作为两个客户端，同时共享和
控制多个 VS Code Agent Host Protocol（AHP）Session，并支持从 QQ 在预授权的本地或
Remote SSH 目标上创建、绑定和启动新的 Session。

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
| `ahp-adapter` | 连接 VS Code Editor Host 或本地/Remote SSH Standalone Host，同时订阅并控制多个 Session/Chat |
| VS Code Agent Host | 唯一权威会话、工具策略和执行状态 |
| QQ Bot | 手机端共享对话、审批、澄清回答和取消 |
| QQ Copilot AHP Status | VS Code 状态栏显示 Gateway、Adapter、绑定和待补发数量 |

旧的 `copilot-qq-hook.exe`、`qq-mcp.exe` 和 QQ Remote Supervisor Agent 仍随安装保留，
但 AHP 模式会禁用其用户级入口，防止两套链路重复发送或审批。

## 已实现

- PC 和 QQ 的用户消息进入同一个 AHP Chat。
- QQ `/new`、`/new advanced` 可在预授权目标上创建新 Session、自动绑定并提交首条任务。
- 每个完整 Assistant 响应段形成后立即同步到 QQ；过程段不会在最终回复中重复，不转发隐藏 reasoning。
- 工具调用只遵循 VS Code Agent Host 原生审批策略。
- 工具执行前审批和工具结果复核同时显示在 PC 与 QQ。
- QQ Keyboard 按钮支持“批准一次”“拒绝”，并保留 `/allow`、`/deny` 文本兜底。
- Boolean 和单选澄清问题使用 QQ 按钮；多选使用逗号分隔文本；自由文本直接回复。
- PC 或 QQ 首个有效审批/回答生效；QQ 提交后的 Host 确认不重复提示，PC 端处理仍通知 QQ。
- Agent 运行中收到的新消息通过 AHP queued message 串行执行。
- PC 或 QQ 均可取消当前 Turn。
- `/sessions` 同时列出所有预授权目标的前台、后台和未监控 Session，并标注完整目录、
  `local` / `ssh:<alias>` 与在线/离线缓存状态；`/switch` 只改变 QQ 普通消息的前台路由，
  不会停止其他 Session。
- `/send <编号> <文本>` 可在不改变前台的情况下定向发送；`/cancel <编号>` 可取消
  指定后台 Turn；`/detach <编号>` 可安全停止监控空闲后台 Session。
- Bridge 自动监控最近活跃的 5 个 Session。LRU 只淘汰非前台且无活动 Turn、排队消息、
  待执行命令、审批或澄清输入的 Session。
- Session 编号由 Bridge 稳定分配，目录刷新不会改变。
- 本地默认 Standalone Host 按需启动/复用；Linux Remote SSH 目标通过 Windows OpenSSH
  `ssh.exe` 按需启动/复用远端默认 Host。
- 本地与远端目标都必须先在电脑上注册并通过 VS Code Workspace Trust 确认后才能被 QQ 选择。
- Turn 运行期间 QQ 显示官方 `msg_type=6`“正在输入”状态，每 45 秒续期，
  完成、取消、失败或等待审批/澄清时停止。
- Host/Adapter 重连恢复 Session 订阅；Host 实例更换时未决操作 fail-closed。
- 脱敏事件和未送达 projection 保留 30 天。
- QQ 实时投递失败后，在 Owner 下一条 QQ 消息的被动回复中补发。
- QQ 私聊语音使用事件自带的 ASR 结果作为普通共享会话输入；敏感控制命令仍要求文字或按钮。
- AHP/Legacy 模式一键切换。

## 重要边界

- 只绑定一个 QQ Owner；Adapter 最多同时维护 5 个跨 Editor、Local Standalone 和
  Remote SSH Host 的 Session Binding，其中一个是 QQ 前台 Session，其余继续在后台运行。
- QQ 只能选择电脑端预注册的精确目标；不能从手机输入任意主机或路径。
- Editor Host 随相应 VS Code 窗口退出；Local Standalone Host 可在 VS Code 关闭时继续
  执行。Remote SSH 需要 Windows PC 在线、已登录且 Bridge 正在运行。
- 前台切换允许目标 Session 正在运行。若 5 个槽位全部被前台、活动 Turn、排队消息或
  Pending 交互保护，新建、切换或定向发送会明确拒绝，而不是强制中断现有任务。
- Standalone Host 共享 Host 级工具与用户级自定义；无编辑器客户端附着时会明确提示
  editor-client tools 不可用。
- 同一精确工作区允许多个 Turn 并发，但 Bridge 会发送冲突警告；有写操作的并发任务
  应使用独立 Git worktree，避免文件和 Git 索引互相覆盖。
- AHP 直接连接针对 VS Code 1.136 的实际协议方言验证。当前 VS Code 1.136 宣告
  AHP `0.9.0`，其类型和 reducer 对应 vendored revision
  `a0bc67f840788f816c9b44bb1325181cb4c4661d`，并应用仓库中的 VS Code 1.136 registry overlay。
- Local Standalone/Remote SSH Host 仍必须宣告 vendored 客户端支持的协议版本；未知版本
  必须 fail-closed，不能手工放宽版本列表。
- npm 上同名 `@microsoft/agent-host-protocol@0.8.0` 不是精确源码；项目通过
  `scripts/vendor-ahp-client.ps1` 生成固定 revision 的本地 tarball。
- VS Code 自动更新后，如果出现未知协议、Snapshot 或 Action，相关 Host 降为只读，
  QQ 不能继续发消息、审批或取消，直到兼容性修复。
- AHP Host 重启后，正在执行的 Turn、审批和澄清请求按失败处理，不自动重放。
- QQ 端只提供单次批准；PC 端仍可使用 VS Code 原生 Session Allow。
- UAC、密码、MFA、系统弹窗和工具认证 Secret 不通过 QQ 处理。
- AHP 是 VS Code 快速演进的接口。本项目固定并验证了 VS Code 1.136 的协议 revision；
  升级 VS Code 后应重新执行兼容性验收。

## 前置条件

- Windows 当前标准用户。
- VS Code 1.136 或经过兼容测试的后续版本；Standalone/Remote SSH 还要求 CLI Host
  宣告当前 vendored 客户端支持的协议版本。
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
npm run build --prefix adapter

npm ci --prefix vscode-extension
npm run typecheck --prefix vscode-extension
npm test --prefix vscode-extension
npm run build --prefix vscode-extension

.\scripts\install.ps1 `
  -Workspace "C:\test","C:\src\another-project" `
  -AhpWorkspace "C:\test","C:\src\another-project"
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
voice_input_enabled = true
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

## 配置与注册目标

`bridge.workspace_roots` 是本地文件操作的安全允许根目录；`ahp.authorized_targets`
是 QQ 可以选择、创建和切换 Session 的精确目标列表。兼容迁移后，本地目标仍会同步写回
`ahp.shared_workspaces`，但新增推荐使用注册脚本并等待 VS Code Workspace Trust 回报：

```toml
[ahp]
[[ahp.authorized_targets]]
kind = "local"
path = 'C:\test'

[[ahp.authorized_targets]]
kind = "ssh"
alias = "devbox"
path = "/home/user/project"
user = "user"
host = "devbox.internal"
port = 22
host_key_fingerprints = ["SHA256:..."]
```

本地目标注册：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\register-local-target.ps1" `
  -Workspace "C:\src\project-a"
```

脚本要求 Bridge 正在运行且 AHP 已配置，并只会在所有 Binding 均无活动 Turn、无排队
消息、无待审批/澄清且无待执行 Adapter 命令时重启。它会规范化路径、去重，并同时更新
安全根目录和目标目录。

Remote SSH 目标注册：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\register-remote-target.ps1" `
  -SshAlias "devbox" `
  -Workspace "/home/user/project"
```

注册会固定 Linux SSH Host 公钥指纹；指纹变化时先在电脑端移除旧目标，再显式重新注册，
QQ 端不能接受或绕过主机身份变化。

移除目标：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\remove-target.ps1" `
  -LocalWorkspace "C:\src\project-a"
```

目标采用精确匹配，不会隐式包含子目录。Remote 目标只接受现有 `~/.ssh/config` Host alias，
并以 `(ssh alias, canonical POSIX path)` 作为身份；若 host/user/port 解析结果变化，则必须重新注册。

## 从 QQ 创建并绑定新 Session

主流程：

```text
/new
  -> 选择目标按钮
  -> 发送首条任务文本
  -> 自动创建 Session、绑定并提交首条任务
```

高级流程：

```text
/new advanced
  -> 选择目标按钮
  -> 选择模型（若 Host 暴露）
  -> 选择审批模式（若 Host 暴露）
  -> 发送首条任务文本
```

Quick mode 只使用 Host 默认值；Advanced 只暴露 Host `resolveSessionConfig` 中可安全映射的
模型与审批模式。若 Host 还要求其他无默认的字段，则 Bridge 会拒绝并要求回到 PC 创建。

## 继续查看或切换已有 Session

`/sessions` 会列出所有预授权目标内的 Session，并标明所在 `local` 或 `ssh:<alias>`、完整
路径以及在线/离线缓存状态。对缓存的远端 Session，`/switch` / `/switch <编号>` 会先自动
重连、刷新并校验精确目标，再执行切换。

电脑端 `ahp-sessions` / `ahp-bind` 仍可用于调试或手工恢复：

```powershell
qq-bridge ahp-sessions
qq-bridge ahp-bind --endpoint <endpoint-id> --session <session-uri>
```

## QQ 使用方式

```text
/new                     选择目标并创建新 Session
/new advanced            额外选择模型/审批模式后创建新 Session
普通文本                 发送到前台 Session
QQ 语音                  使用内置 ASR 发送到前台 Session，或作为 /new 首条任务
/ask <文本>              即使 Agent 正等待澄清，也排队为新消息
/send <编号> <文本>      定向发送到指定 Session，不改变前台
/sessions                列出前台、后台和未监控 Session
/switch                  显示全部 Session 的前台切换按钮
/switch <编号>           切换 QQ 前台；其他 Session 继续运行
/detach <编号>           安全停止监控空闲后台 Session
/allow <审批码>          单次批准
/deny <审批码>           拒绝
/answer <问题码> <文本>  显式回答澄清
/cancel                  优先取消 /new 向导；否则取消前台 Session 的当前 Turn
/cancel <编号>           取消指定 Session 的当前 Turn
/notify                  查看当前通知模式
/notify <模式>           即时切换 approval_only、compact 或 full
/status                  查看 Gateway、Adapter、Session、通知模式和待补发状态
/help
```

当 Agent 正等待澄清时，普通文本优先作为当前问题的答案。审批永远要求按钮或显式
`/allow`、`/deny`，普通文本不会被解释为批准。

语音识别结果同样只作为普通输入、澄清回答或 `/new` 首条任务，不会执行 `/new`、
`/allow`、`/deny`、`/cancel` 或 `/switch` 等控制命令。QQ 事件未提供 ASR 结果时，
Bot 会提示重新录制或改发文字；
Bridge 不下载或长期保存语音文件。

每次 `/switch` 会生成有效期有限的一次性按钮 token；旧菜单、重复点击、Session
目录变化会 fail-closed。前台切换本身不要求当前或目标 Session 空闲；如果目标尚未被
监控，Bridge 会先安全分配 Binding。只有 5 个槽位全部受保护时才拒绝加入。
单个 Keyboard 最多 25 个 Session；最多使用 4 条被动回复展示 100 个 Session。

Assistant 过程段、审批、澄清和最终回复均带 `[短码 · 标题]` 来源标签。通知不会自动抢占
QQ 前台；仅当来源不是当前 QQ 前台 Session 时，审批、澄清和最终回复才附加独立的
“切换到该 Session”按钮，过程段不附加按钮。普通文本只会自动回答前台 Session 的待澄清
问题；`/answer <问题码>`、审批码和按钮始终路由回产生通知的原 Session。

示例：

```text
AHP Sessions:
* APYZB | Clone and deploy qq-copilot | D:\work\vscode\qq-copilot | 当前
  AZ49U | 接入GitHub Copilot功能实现 | D:\work\vscode\CodexPlusPlus | 可切换
```

本机也可以使用完整 URI 切换：

```powershell
qq-bridge ahp-sessions
qq-bridge ahp-bind --endpoint <endpoint-id> --session <session-uri>
```

QQ 端显示稳定短编号、标题和所在目录，不暴露或要求输入长 Session URI。Adapter 目录
短暂断开或重启不会改变编号。

## 工具通知模式

在 QQ 中查看或即时切换：

```text
/notify
/notify approval_only
/notify compact
/notify full
```

切换仅允许 Owner 操作，会立即影响后续事件并写入配置，无需重启。三种模式分别为：

- `approval_only`：不发送工具状态通知；需要审批时仍发送审批，完整 Assistant 过程段和最终回复仍照常发送。
- `compact`：仅工具完成/取消后通知一次；需要审批时立即发送审批。
- `full`：发送 streaming、running、completed、cancelled 等所有工具状态变化。

也可以在本机配置默认值：

```toml
[ahp]
tool_notification_mode = "compact"
typing_indicator_enabled = true
typing_duration_seconds = 60
typing_refresh_seconds = 45
```

直接编辑 TOML 后需要重启 Bridge；通过 `/notify` 切换则不需要。

## QQ 输入状态

QQ 官方 C2C `msg_type=6` 输入状态按 Session/Turn 独立跟踪。默认持续 60 秒，Bridge
每 45 秒续期。过程回复不会中断输入状态；发送对应 Session 的最终 Assistant 回复时
QQ 客户端会自动取消该状态。

当 Agent 等待审批或澄清输入时，typing 会暂停；任一端完成交互后恢复。该状态不是
文本消息，不进入对话历史，也不计入工具通知模式。

## VS Code 状态栏

状态栏显示：

- `QQ AHP 已连接`
- `QQ AHP 未就绪`
- `QQ 待补发 N`
- `QQ AHP 离线`

点击状态栏可查看 Gateway、Adapter、Binding 数量、活动 Turn、待交互、命令和
projection 数量。

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

当前测试覆盖：SQLite 单 Binding 到多 Binding 迁移、多目标目录过滤、前台切换、
5-Session LRU 与受保护容量拒绝、定向发送/取消/解绑、来源审批和输入路由、同工作区并发
警告、Owner 身份、事件幂等、命令租约、Host 更换 fail-closed、按钮重放、离线补发、
通知模式、Assistant response-part 边界/最终去重、输入终态来源、AHP reducer、
Named Pipe WebSocket 和多 Binding Session/Chat hydration。
