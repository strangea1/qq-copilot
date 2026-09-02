---
description: Preserve the audited VS Code Agent Host Protocol compatibility boundary.
applyTo: "adapter/**,vendor/**,scripts/vendor-ahp-client.ps1"
---

# AHP compatibility

- VS Code 1.136 advertises AHP `0.9.0`, but its protocol tree is not the
  published `v0.9.0` tag. It uses upstream revision
  `a0bc67f840788f816c9b44bb1325181cb4c4661d` plus the registry changes in
  `vendor/ahp-vscode-1.136.patch`.
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
- Validate changes with adapter typecheck, tests, build, and a real Host probe.
  The probe must show both advertised and selected protocol `0.9.0`.
