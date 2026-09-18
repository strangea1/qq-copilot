# QQ Copilot AHP Bridge

本项目让 PC 上的 VS Code Agents Window 与手机 QQ Bot 作为两个客户端，同时共享和
控制多个 VS Code Agent Host Protocol（AHP）Session，支持从 QQ 发现、绑定和切换
预授权本地或 Remote SSH 目标上的已有 Session。

完整安装、升级、回滚和故障排查见 [部署指南](docs/deployment.md)；协议与安全设计见
[远程交互设计](docs/qq-copilot-remote-design.md)。

Session 创建、GitHub 登录、模型和审批模式选择均在电脑端完成；QQ 只操作已有 Session。

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
| QQ Copilot AHP Status | VS Code 状态栏显示运行状态，并报告工作区信任状态 |

旧的 `copilot-qq-hook.exe`、`qq-mcp.exe` 和 QQ Remote Supervisor Agent 仍随安装保留，
但 AHP 模式会禁用其用户级入口，防止两套链路重复发送或审批。

## 已实现

- PC 和 QQ 的用户消息进入同一个 AHP Chat。
- QQ 可发现和切换电脑端创建的 Session，并向已有 Session 发送任务。
- 每个完整 Assistant 响应段形成后立即同步到 QQ；过程段不会在最终回复中重复，不转发隐藏 reasoning。
- 工具调用只遵循 VS Code Agent Host 原生审批策略。
- 工具执行前审批和工具结果复核同时显示在 PC 与 QQ。
- QQ Keyboard 按钮支持“批准一次”“拒绝”，并保留 `/allow`、`/deny` 文本兜底。
- Boolean 和单选澄清问题使用 QQ 按钮；多选使用逗号分隔文本；自由文本直接回复。
- PC 或 QQ 首个有效审批/回答生效；QQ 提交后的 Host 确认不重复提示，PC 端处理仍通知 QQ。
- Agent 运行中收到的新消息通过 AHP queued message 串行执行。
- PC 或 QQ 均可取消当前 Turn。
- `/sessions` 和 `/switch` 共用最近候选列表，默认按 LRU 保留 10 个；前台、运行中、
  排队或待处理会话始终保留。列表分行显示标题、状态、完整目录、`local` / `ssh:<alias>`、
  在线/离线缓存和最近使用时间。精简只影响 QQ 展示，不删除历史或停止监控；
  `/switch` 只改变 QQ 普通消息的前台路由，不会停止其他 Session。
- `/send <编号> <文本>` 可在不改变前台的情况下定向发送；`/cancel <编号>` 可取消
  指定后台 Turn；`/detach <编号>` 可安全停止监控空闲后台 Session。
- Bridge 自动监控最近活跃的 5 个 Session。LRU 只淘汰非前台且无活动 Turn、排队消息、
  待执行命令、审批或澄清输入的 Session。
- Session 编号由 Bridge 稳定分配，目录刷新不会改变。
- 本地默认 Standalone Host 按需启动/复用；Linux Remote SSH 目标通过 Windows OpenSSH
  `ssh.exe` 在 Adapter 启动后后台发现并复用远端默认 Host。
- 本地与远端目标都必须先在电脑上注册并通过 VS Code Workspace Trust 确认后才能被 QQ 选择。
- Turn 运行期间 QQ 显示官方 `msg_type=6`“正在输入”状态，每 45 秒续期，
  完成、取消、失败或等待审批/澄清时停止。
- Host/Adapter 重连恢复 Session 订阅；Host 实例更换时未决操作 fail-closed。
- 脱敏事件和未送达 projection 保留 30 天。
- QQ 实时投递失败后，在 Owner 下一条 QQ 消息到达时单独补发；同一事件批次使用稳定
  幂等键，成功或投递结果不确定后都不会再次发送。
- QQ 私聊语音使用事件自带的 ASR 结果作为普通共享会话输入；敏感控制命令仍要求文字或按钮。
- AHP/Legacy 模式一键切换。

## 重要边界

- 只绑定一个 QQ Owner；Adapter 最多同时维护 5 个跨 Editor、Local Standalone 和
  Remote SSH Host 的 Session Binding，其中一个是 QQ 前台 Session，其余继续在后台运行。
- QQ 只能选择电脑端预注册的精确目标；不能从手机输入任意主机或路径。
- Editor Host 随相应 VS Code 窗口退出；Local Standalone Host 可在 VS Code 关闭时继续
  执行。Remote SSH 需要 Windows PC 在线、已登录且 Bridge 正在运行。
- 前台切换允许目标 Session 正在运行。若 5 个槽位全部被前台、活动 Turn、排队消息或
  Pending 交互保护，切换或定向发送会明确拒绝，而不是强制中断现有任务。
- Standalone Host 共享 Host 级工具与用户级自定义；无编辑器客户端附着时会明确提示
  editor-client tools 不可用。
- QQ 不提供 GitHub 凭据 Broker，也不代替电脑端登录或授权。
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
- VS Code 1.136 的 Editor endpoint 宣告并协商 `0.9.0`；`code-tunnel agent host`
  的 standalone 注册表宣告 `0.1.0`，但实际 wire 仍严格协商 `0.9.0`。Adapter 只对这一
  已验证组合应用元数据映射，不把 `0.1.0` 当作 wire 协议。

## 前置条件

- Windows 当前标准用户。
- VS Code 1.136 或经过兼容测试的后续版本；Standalone/Remote SSH 必须匹配已审计的
  registry-to-wire 版本组合，未知组合继续 fail-closed。
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
是 QQ 可以查看和切换 Session 的精确目标列表。兼容迁移后，本地目标仍会同步写回
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

脚本要求 Bridge 和 Adapter 正在运行且支持授权热更新。它会规范化路径、去重，并同时
更新安全根目录和目标目录，等待两端确认生效；追加授权无需重启，也无需等待活动任务、
审批或澄清结束。

Remote SSH 目标注册：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\register-remote-target.ps1" `
  -SshAlias "devbox" `
  -Workspace "/home/user/project"
```

注册会固定 Linux SSH Host 公钥指纹；指纹变化时先在电脑端移除旧目标，再显式重新注册，
QQ 端不能接受或绕过主机身份变化。

### 追加授权热更新

追加同一 SSH alias 下的新目录时，保留 host/user/port/公钥指纹不变，即可复用已有
Host、SSH 隧道和 Binding。Adapter 会立即按新授权重新发布已缓存的 Session 目录，
不会为了新增目录断开或重绑正在运行的 Session。新增其他主机时，后台独立建立发现连接。

Bridge 在 Adapter 命令轮询时检查配置变化，仅接受授权目标与本地安全根的追加；
重复项和顺序调整不产生新的授权版本。手工保存配置后可显式等待应用确认：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe" `
  --config "$env:LOCALAPPDATA\CopilotQQBridge\config.toml" reload-targets
```

该命令仅在 Adapter 已确认当前授权版本且仍连接时成功；超时或不支持热更新会明确失败，
不会退回自动重启。状态中的 `ahp.authorization` 提供 Bridge/Adapter 版本及最后一次错误。
无效 TOML、移除目录、修改 SSH 身份/指纹或其他非热更新配置都会保留上一份有效授权，
并要求修正配置或在空闲后重启；删除文件中的条目不会即时撤销运行中的授权。

注册脚本默认确认热更新。`-SkipReloadConfirmation` 仅跳过显式确认，不阻止后台轮询更新；
旧参数 `-SkipRestart` 保留为此参数的别名。旧版 Bridge/Adapter 必须先成对升级。

移除目标：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\remove-target.ps1" `
  -LocalWorkspace "C:\src\project-a"
```

目标采用精确匹配，不会隐式包含子目录。Remote 目标只接受现有 `~/.ssh/config` Host alias，
并以 `(ssh alias, canonical POSIX path)` 作为身份；若 host/user/port 解析结果变化，则必须重新注册。

## 继续查看或切换已有 Session

先在电脑端创建并登录 Session，再从 QQ 使用 `/sessions` 和 `/switch <编号>`。
`/new`（包括 advanced）已移除；旧命令和创建按钮会被拒绝，不会作为任务发送到前台。

`/sessions` 和 `/switch` 每次都从全部预授权目标中选取同一批最近候选，默认保留 10 个。
LRU 同时考虑 PC 端的 Session 修改时间，以及 QQ 切换、发消息、取消和处理交互等实际
使用记录；浏览列表、自动发现和重连快照不会刷新使用顺序，记录跨重启保存。
前台、运行中、排队、待执行命令、审批或澄清会话始终保留，其余按最近使用补足到 10 个；
受保护会话本身超过 10 个时全部保留。前台置顶，其余按最近使用排序。

较旧会话仅从 QQ 候选列表收起，不删除 Agent Host Session、聊天历史、稳定短码或 Binding。
仍可通过 `/switch <原编号>`、`/send <原编号> <文本>` 使用，或在 PC 上继续该会话，之后会
重新进入最近候选。电脑端 `qq-bridge ahp-sessions` 仍返回完整目录。

候选数量可通过 `[ahp]` 中的 `session_list_limit = 10` 配置（1–100）；修改此项需要在空闲时
重启 Bridge/Adapter，不属于授权目标追加热更新。10 个候选和 5 个监控槽位是不同的限制。
列表保留完整路径、主机和在线/离线缓存状态。对缓存的远端 Session，切换时会先自动
重连、刷新并校验精确目标，再执行切换。

Adapter 会在后台连接所有已注册的 SSH 目标并持续订阅 Host Session 目录，因此电脑端创建的
Session 也会出现在列表中。相同 SSH alias 的多个授权目录共享 Host/本地隧道，每个 Session
按其精确目录匹配授权目标；不会自动授权子目录、创建 Session 或切换 QQ 前台。空闲 SSH
目录连接会保留，失败按 1 秒起步、最多 60 秒的退避独立重试，不阻塞本地启动、QQ 命令轮询
或其他远端。每 30 秒检查连接状态；正在使用的 Host 不会被发现任务替换。Adapter 停止时
停止重试，等待已有受超时限制的连接操作清理，仅关闭本地 SSH 隧道，不终止远端 Host。
SSH 身份、公钥指纹与已审计协议版本检查保持不变；本地 Standalone Host 仍仅按需启动。

电脑端 `ahp-sessions` / `ahp-bind` 仍可用于调试或手工恢复：

```powershell
qq-bridge ahp-sessions
qq-bridge ahp-bind --endpoint <endpoint-id> --session <session-uri>
```

## QQ 使用方式

```text
普通文本                 发送到前台 Session
QQ 语音                  使用内置 ASR 发送到前台 Session
/ask <文本>              即使 Agent 正等待澄清，也排队为新消息
/send <编号> <文本>      定向发送到指定 Session，不改变前台
/sessions                显示最近候选，保留前台、活动和待处理 Session
/switch                  显示同一批候选的“编号 + 标题”切换按钮
/switch <编号>           切换 QQ 前台；其他 Session 继续运行
/detach <编号>           安全停止监控空闲后台 Session
/allow <审批码>          单次批准
/deny <审批码>           拒绝
/answer <问题码> <文本>  显式回答澄清
/cancel                  取消前台 Session 的当前 Turn
/cancel <编号>           取消指定 Session 的当前 Turn
/notify                  查看当前通知模式
/notify <模式>           即时切换 approval_only、compact 或 full
/status                  查看 Gateway、Adapter、Session、通知模式和待补发状态
/help
```

当 Agent 正等待澄清时，普通文本优先作为当前问题的答案。审批永远要求按钮或显式
`/allow`、`/deny`，普通文本不会被解释为批准。

语音识别结果同样只作为普通输入或澄清回答，不会执行
`/allow`、`/deny`、`/cancel` 或 `/switch` 等控制命令。QQ 事件未提供 ASR 结果时，
Bot 会提示重新录制或改发文字；
Bridge 不下载或长期保存语音文件。

每次 `/switch` 会生成有效期有限的一次性按钮 token；旧菜单、重复点击、Session
目录变化会 fail-closed。前台切换本身不要求当前或目标 Session 空闲；如果目标尚未被
监控，Bridge 会先安全分配 Binding。只有 5 个槽位全部受保护时才拒绝加入。
单个 Keyboard 最多 25 个 Session；最多使用 4 条被动回复展示 100 个 Session。
受保护候选超过这一 QQ 按钮上限时会明确提示使用原编号切换，不会为满足限制删除会话或中断任务。

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

直接编辑上述通知/输入状态配置后需要重启 Bridge；通过 `/notify` 切换通知模式则不需要。
授权列表的追加使用独立的热更新机制。

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
