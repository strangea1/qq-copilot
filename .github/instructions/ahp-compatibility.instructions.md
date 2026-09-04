---
description: Preserve the audited VS Code Agent Host Protocol compatibility boundary.
applyTo: "adapter/**,vendor/**,scripts/vendor-ahp-client.ps1"
---

# AHP compatibility

- VS Code 1.136 advertises AHP `0.9.0`, but its protocol tree is not the
  published `v0.9.0` tag. It uses upstream revision
  `a0bc67f840788f816c9b44bb1325181cb4c4661d` plus the registry changes in
  `vendor/ahp-vscode-1.136.patch`.
- VS Code 1.136 `code-tunnel agent host` writes standalone registry metadata
  version `0.1.0`, while `initialize` identifies the wire server as `0.9.0`
  accepting `^0.9.0`. Preserve `0.1.0` as the advertised registry value and
  use the audited `wireProtocolVersion: 0.9.0` override only for managed
  standalone entries. Never offer `0.1.0` as a wire protocol.
- Start local managed Hosts with `agent host --new-instance --foreground`.
  The default command detaches/reuses a supervisor, so launcher exit is not a
  Host-liveness signal and does not provide an owned lifecycle.
- The VS Code 1.136 standalone root advertises `copilotcli` and `claude`;
  mobile Copilot creation must prefer `copilotcli`. A newly created Copilot
  Session has deferred backing and is absent from `listSessions` until its
  first message. Confirm the exact client-chosen `ahp-session:/<uuid>` by
  subscribing to it, then establish an `AhpCore` provisional binding before
  acknowledging creation. Transfer that binding to the Bridge bind command,
  or close it on failure/timeout; a temporary summary alone does not prevent
  Host garbage collection. Never treat the initial empty listing as creation
  failure.
- Reuse a retained managed Host for later prepare/create/dispose operations on
  the same target. Replacing its instance invalidates every existing binding.
  Cleanup local owned instances with `agent kill --instance-id` and verify the
  registry entry disappears; Remote SSH cleanup closes only the local tunnel.
- The vendored npm package intentionally retains package version `0.8.0`.
  Regenerate it with `scripts/vendor-ahp-client.ps1 -Force` and update the
  lockfile through npm; do not substitute the npm registry tarball.
- Never make a Host appear compatible by only adding a protocol string or
  removing either version gate. Upgrade the audited types/reducer and adapt
  wire behavior first.
- AHP 0.9 represents `chat/error` as a durable error response part and supports
  `chat/turnResume`. Normalize legacy top-level errors before snapshots/actions
  reach reducers or observers, and keep `ChatTurnResume` in the chat action
  allowlist.
- Editor Session snapshots can exceed 8 MiB for long histories. The audited
  named-pipe transport accepts up to 32 MiB; keep the bound and its Windows
  transport regression test. Do not remove the limit or lower it below the
  real Host probe size.
- Validate changes with adapter typecheck, tests, build, and a real Host probe.
  The Editor probe must show advertised/selected `0.9.0`; the VS Code 1.136
  standalone probe must show registry-advertised `0.1.0` and wire-selected
  `0.9.0`.
