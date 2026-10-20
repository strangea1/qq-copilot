# 本地 Codex CLI 适配

本功能默认关闭，可与现有 VS Code AHP Sessions 同时使用，仍共享最多 5 个
Binding 和一个 QQ 前台。首期仅支持本地 Windows 上的 OpenAI Codex CLI，不支持
Remote SSH Codex。

## 工作方式与前提

Adapter 启动自己持有的 `codex app-server --listen stdio://` 子进程，
通过官方 JSON-RPC 协议列出电脑端已有的非归档 thread。选择后恢复原 thread ID，
不会创建或克隆 thread，也不向 QQ 重放旧聊天记录。

这不是附着到任意已经运行的 Codex 终端。恢复前应先结束原终端中的任务，并停止从
该终端发送消息；不要从终端和 QQ 并发写同一个 thread。协议没有跨独立进程的排他锁，
Bridge 只能拒绝当前 app-server 明确报告为 active 的 thread。

1. 在电脑端安装原生 Windows
   [Codex CLI 0.163.0](https://github.com/openai/codex/releases/tag/rust-v0.163.0)，
   并确认原生可执行文件的 `--version` 返回 `codex-cli 0.163.0`。
   目前只接受此已审计版本，其他版本明确拒绝连接。
2. 使用 Codex 自己的电脑端登录流程完成登录，并在所需工作目录创建 thread。
   Bridge 不读取、展示或转发登录凭据，不继承 VS Code 的 GitHub 登录。
3. 使用现有 `add-workspace.ps1` 将 **thread 的精确工作目录** 注册为本地可信目标，
   并在 VS Code 中显式确认工作区信任。只信任父目录不会授权子目录；
   SSH 目标也不会被当成本地 Codex 授权。

模型、沙箱和审批模式由原有 Codex 配置及 thread 设置决定。恢复请求不覆盖这些设置，
也不为方便 QQ 操作自动放宽权限。

## 启用、禁用

安装包含此功能的 Bridge 和 Adapter 后，用实际原生可执行文件路径启用：

```powershell
$Bridge = "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe"
& $Bridge configure-codex --executable "C:\Tools\Codex\codex.exe"
if ($LASTEXITCODE -ne 0) { throw "Failed to configure Codex" }
```

如果从 npm 安装 Codex，需要定位 npm 包实际携带的原生 `.exe`；不能传入
`codex.cmd`、PowerShell 脚本或 shell 命令。Bridge 只保存该绝对路径到
`ahp.codex_executable`，不执行电脑端登录。

禁用：

```powershell
& $Bridge configure-codex --disable
if ($LASTEXITCODE -ne 0) { throw "Failed to disable Codex" }
```

两种操作都需要在 **所有 Binding、活动 Turn、排队消息、审批和澄清输入空闲后**，
通过现有受管流程重启 Bridge/Adapter。命令不会自动重启，也不会中断当前 Agent。
提供方配置不属于 `reload-targets` 的热更新范围；已有可信列表的追加热更新仍然支持，
Codex 下次目录刷新（通常 15 秒以内）即使用新的授权列表，无需重启 app-server。

## QQ 使用方式

```text
/sessions
/switch <编号>
/send <编号> <文本>
/cancel <编号>
/answer <问题码> <答案>
/detach <编号>
```

- Codex 标题以 `[Codex]` 开头，Host 标识为 `codex-local`。
- 切换后普通 QQ 文本发往该 thread；`/send` 不改变前台。
- 每个已关闭的 Assistant 文本项只通知一次；过程回复和最终回复复用现有通知链路，
  不发送 reasoning。最终回复如果尚无最终阶段标记，会在 Turn 结束时完成投递。
- 活动 Turn 期间的新消息排队，成功完成后顺序执行。取消、失败或连接丢失不重放任务；
  取消/失败会清除尚未执行的本地排队消息。
- 命令执行和文件修改审批通过既有 QQ 按钮或 `/allow`、`/deny` 回答，
  仅支持批准一次/拒绝，不支持扩大到整个 Session 的授权或在 QQ 编辑工具参数。
- 支持单个非敏感的自由文本或单选问题，问题码固定路由回来源 Binding。
- `/detach` 仅释放空闲 thread 的订阅，不删除 Codex 历史。

## 错误与限制

- Codex 不可用、版本不匹配、协议错误或进程退出会明确记录错误并将其目录置为离线，
  不影响 AHP 连接。排查可先在电脑端检查原生 CLI 版本和登录状态。
- app-server 重启意味着新的 Host 实例。旧 Turn/审批/回答不可继续使用，也不自动
  重放消息；在新目录出现后重新 `/switch`。
- 密码、MFA、凭据刷新、多问题表单、同一工具的并发审批以及暂未支持的权限/MCP
  请求会被拒绝，并提示改在电脑端 Codex 处理；不会自动同意或把敏感输入转到 QQ。
- 每个 Binding 最多排队 100 条消息。目录最多读取 100 页、每页 100 条，
  最多接受 1,000 个可信 thread；重复游标或超限会报错，而不是静默截断目录。
- 请求超时会关闭受管 app-server，避免对结果不确定的写操作自动重试。
  子进程通过 stdio 通信，不开放网络监听，也不接管其他 Codex 进程。

测试覆盖协议收发、工作区精确授权、双提供方共存、消息/审批/回答的跨 Binding
隔离、取消、队列和断线。真实模型任务仍需要电脑端已有可用的 Codex 登录与 thread；
未登录时，成功启动或列目录不代表模型执行已经通过端到端验收。
