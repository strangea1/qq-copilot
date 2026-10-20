# 部署指南

本文说明如何在 Windows 上从源码构建、安装并运行 QQ Copilot AHP Bridge。

## 1. 部署模型

生产模式支持两类 Host：

```text
VS Code Editor Host / Local Standalone Host / Remote SSH Standalone Host
QQ Bot ↔ qq-bridge ↔ AHP Adapter
```

- Local Standalone Host 可在 VS Code 关闭时继续运行。
- Remote SSH 目标通过 Windows `ssh.exe` 连接并复用远端默认 Standalone Host。
- Editor Host 随相应 VS Code 窗口退出。
- QQ 与 PC 最多同时共享 5 个已监控的 Agent Host Session；一个作为 QQ 前台，其余可继续
  后台运行。Session 创建和 GitHub 登录在电脑端完成。
- Bridge、Adapter 和状态栏扩展安装在 Agent 可编辑工作区之外。
- 旧 Hooks/MCP 链只作为回滚模式。

## 2. 系统要求

- Windows 10/11，推荐使用非管理员标准用户。
- VS Code 1.142，或已完成兼容测试的后续版本。
- Local Standalone/Remote SSH 还要求 `code agent host` 匹配当前 vendored 客户端已
  审计的 registry-to-wire 版本组合；未知组合会 fail-closed。
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

VS Code 1.142.0 的 `state/protocol/.ahp-version` 锁定的 AHP 类型/reducer revision 是：

```text
13277064ffa20cceab1bfc04ba876a1b181b6e80
```

Editor endpoint 在注册表和握手中均使用 `1.1.0`。客户端支持已审计的
`1.1.0`、`1.0.0`、`0.10.0` 和 `0.9.0`，不隐式接受未来 `1.x`。
VS Code 1.142 与旧版 1.136 的 `code-tunnel agent host` 均在 standalone 注册表中
使用 `0.1.0` 元数据。Adapter 仅在已审计的 wire 集合内握手，确认 1.142 的 `1.1.0`
或旧版的 `0.9.0`，保留原始 advertised 值，并将确认结果写入内存中的
`wireProtocolVersion`。后续连接只提供该版本，未知组合拒绝连接，
不会向 Host 提供 `0.1.0` wire。
本地 Host 使用 `agent host --new-instance --foreground`，由 Adapter 持有精确进程生命周期；
默认 detach/reuse 模式不能用于受管目标。同一目标的后续连接操作复用 retained Host，
避免替换 instance 使其他 Session 断线；本地 owned Host
通过精确 instance ID 清理，Remote SSH 只关闭本地 tunnel。

仓库的
`vendor/ahp-vscode-1.142.patch` 在上游基线之外增加已审计的 `1.1.0` 和 VS Code 的
`0.10.0` 协议 offer，
并包含生成后的固定 tarball。需要重新生成时运行：

```powershell
.\scripts\vendor-ahp-client.ps1 -Force
```

预期 SHA-256：

```text
c59b0bddbbf99c79e0f92977dae024ea962c96491594f0de06e92a10dc447f39
```

固定 tarball 是 `microsoft-agent-host-protocol-1.0.0.tgz`；包版本不是 wire 版本，
不要用 npm registry 中同名包替换经审计的源码。

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
npm test --prefix vscode-extension
```

## 6. 安装

示例同时配置两个安全根目录，并写入 `code` / `ssh` 可执行路径：

```powershell
.\scripts\install.ps1 `
  -Workspace "C:\test","C:\src\another-project" `
  -AhpWorkspace "C:\test","C:\src\another-project"
```

`-Workspace` 设置安全允许根目录；`-AhpWorkspace` 仍可兼容写入本地目标列表，但新的目标授权
推荐在安装完成后通过注册脚本执行 Trust 流程。安装器会把
`ahp.code_executable`（原生 `code-tunnel.exe`）、`ahp.code_launcher`
（用于打开 Trust UI）和 `ahp.ssh_executable` 一并写入配置。

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

升级后执行 **Developer: Reload Window**。状态扩展不再提供 Session 认证 Broker、
启用/撤销认证命令或相关设置；VS Code 自己管理的 GitHub 登录不受影响。

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

## 10. 注册本地与 Remote SSH 目标

本地目标必须在电脑端完成注册，并等待状态扩展回报当前精确文件夹已 Trusted：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\register-local-target.ps1" `
  -Workspace "C:\src\project-a"
```

目标目录按精确路径匹配；只添加共同父目录不会展示其子目录中的 Session。脚本要求
Bridge/Adapter 正在运行且已配置 AHP。追加目标通过授权版本同步热更新，不会重启，
活动 Turn、排队消息或 Pending 审批/澄清不会阻止追加。

Remote SSH 目标要求现有 `~/.ssh/config` Host alias、`BatchMode=yes` 可用、只接受 Linux
远端，并会校验 `code agent host` / `code agent endpoints`、远端规范路径和 Linux SSH
Host 公钥指纹。指纹变化后必须在电脑端重新注册：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\register-remote-target.ps1" `
  -SshAlias "devbox" `
  -Workspace "/home/user/project"
```

若同一 alias 的指纹与已授权记录不同，注册会在运行任何远端 `code agent` 命令前拒绝。
确认主机更换无误后，先用 `remove-target.ps1` 删除旧授权，再重新注册并完成 Trust。

移除目标：

```powershell
& "$env:LOCALAPPDATA\Programs\CopilotQQBridge\scripts\remove-target.ps1" `
  -LocalWorkspace "C:\src\project-a"
```

注册脚本默认先确认运行版本支持热更新，保存授权后再次等待 Adapter 确认。
同一 SSH alias 的新增目录复用已有连接，host/user/port/指纹必须完全一致。
配置文件变化由 Adapter 命令轮询驱动检查，可用下面的命令立即请求并等待确认：

```powershell
qq-bridge reload-targets
qq-bridge reload-targets --timeout-seconds 30
```

默认等待两个命令轮询周期加 10 秒；成功表示 Bridge/Adapter 授权版本一致，不保证新增
主机的网络连接已经完成。已有主机下新目录的缓存 Session 会重新发布，无需重连。
`status` 中 `ahp.authorization` 可用于诊断未确认版本或配置错误。

此版本仅支持追加目标/安全根。删除授权、修改 SSH 身份/指纹和其他非热更新配置仍需要
在空闲后重启；热更新拒绝此类修改并保留上一份有效授权。注册脚本的
`-SkipReloadConfirmation`（兼容别名 `-SkipRestart`）只跳过显式等待，不禁用后台更新。
配置无效或 Adapter 不支持热更新时，不会自动重启或报告已经生效。

## 11. 连接已有 Session

先在电脑端创建 Session 并完成登录、模型和审批模式配置。QQ 不再支持 `/new` 或
`/new advanced`，请通过 `/sessions` 查看，再使用 `/switch <编号>` 选择。

升级必须先等待所有 Turn、命令及交互结束，并停止旧 Bridge/Adapter 后成对安装。
数据库迁移会终结遗留创建/销毁命令并移除旧向导状态，不删除已有 Session 或对话。
历史命令与销毁保护记录仅为安全兼容保留，不再提供创建或销毁入口。
已明确记录从未派发的销毁命令在迁移终结后不会阻止重新绑定；已派发或缺少历史租约记录、
无法确认执行结果的销毁命令仍保留保护，避免重新操作可能正在销毁的 Session。

发现或重连时若 Host 要求认证，请在电脑端完成登录。本次失败的控制连接会被清理，
不会因重试累积连接或阻止已有共享隧道的后续恢复。

QQ `/sessions` 的示例输出：

```text
【会话列表】
显示 3/3 个 · 常规保留 10 个
监控 2/5 个 · 前台置顶，其余按最近使用排序

1. [APYZB] Clone and deploy qq-copilot
前台 · 空闲 · 在线
位置：local · D:\work\vscode\qq-copilot
最近使用：09-18 16:00

2. [AZ49U] 接入 GitHub Copilot 功能实现
后台 · 运行中 · 在线
位置：local · D:\work\vscode\CodexPlusPlus
最近使用：09-18 15:58

3. [A7K2P] 新任务
未监控 · 空闲 · 离线/缓存
位置：ssh:devbox · /home/user/project-c
最近使用：09-17 10:30
```

候选默认按 LRU 精简为最近使用的 10 个；前台、运行中、排队或待命令/审批/澄清的会话
不被收起，受保护会话超过 10 个时全部保留。列表会说明收起数量。
这只精简 QQ 展示，不删除 PC 会话、历史或监控绑定。原短码仍可用于切换、发送消息；
PC 上继续旧会话后也会重新进入候选。浏览菜单、自动发现和重连快照不算使用。
QQ 实际使用记录会持久化，并与 Host 的修改时间共同决定 LRU；最近使用时间按 PC 本地时区显示。

可在现有 `[ahp]` 配置中设置 `session_list_limit = 10`（1–100），空闲时重启
Bridge/Adapter 后生效。该项不属于授权追加热更新；它不改变最多 5 个监控 Binding 的限制。
电脑端 `qq-bridge ahp-sessions` 不受候选数量限制。

## 12. 多 Session 路由与切换

QQ 中发送：

```text
/switch
```

Bot 使用与 `/sessions` 一致的最近候选，分行展示标题、完整目录、`local` / `ssh:<alias>`、
在线/离线缓存和运行状态；按钮同时显示稳定短码和简短标题。`/switch` 只改变 QQ 普通文本和语音
的默认路由，不会停止旧 Session，当前或目标 Session 正在运行时也可以切换前台。对于尚未
监控或仅缓存的远端 Session，Adapter 会先连接精确目标、刷新目录并安全分配 Binding。

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
Session 不会显示，也不能通过旧按钮切换。受保护候选超过 100 个按钮的 QQ 限制时，使用
`/switch <原短码>`，不会强制清理活动会话。Assistant 过程段会在形成后立即显示来源短码
与标题，且不会在最终回复中重复；仅当来源不是当前 QQ 前台 Session 时，审批、澄清和
最终回复才会附加独立的前台切换按钮。这些通知都不会自动抢占当前前台。

同一个精确工作区出现第二个活动 Turn 时，Bridge 会警告潜在文件/Git 索引冲突，但不会
阻断执行。有写操作的并发任务应使用独立 Git worktree。

## 13. QQ 命令

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

直接编辑上述通知/输入状态配置后需要重启 Bridge；通过 `/notify` 切换通知模式不需要。
授权列表追加使用独立的热更新机制。

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
- 确保 Session 工作目录与 `ahp.authorized_targets`（本地目标会同步写回 `ahp.shared_workspaces`）中的某个目标精确一致。
- 不要只配置共同父目录；每个实际 Session 目录都需要单独加入目标列表。
- 运行 `qq-bridge ahp-sessions`，确认 Adapter 已发现该 Session 及其 `workspace_uris`。
- 新增目录后运行 `qq-bridge reload-targets` 确认授权版本已同步，无需重启。
  若命令不存在，请先在空闲时成对升级 Bridge/Adapter，而不要在活动任务中强制重启。

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
- Owner 下一次发送 QQ 消息时，Bridge 会单独补发遗漏内容。每个事件批次最多进行一次
  QQ 投递；成功或结果不确定后即终止补发，后续消息和 Bridge 重启不会重复发送。

### VS Code 关闭后 QQ 无法继续

这是编辑器托管 Agent Host 模式的预期边界。重新打开 VS Code 后，Adapter 会发现新
Host 并恢复所有已监控 Session。进行中的 Turn 不会自动重放。
