---
description: Preserve the audited VS Code Agent Host Protocol compatibility boundary.
applyTo: "adapter/**,vendor/**,scripts/vendor-ahp-client.ps1"
---

# AHP compatibility

- VS Code 1.142.0 advertises AHP `1.1.0`. Its `.ahp-version` pins upstream
  revision `13277064ffa20cceab1bfc04ba876a1b181b6e80`. Generate types, reducers
  and client from that revision with `vendor/ahp-vscode-1.142.patch`, which
  adds audited offers `1.1.0` and `0.10.0` to baselines `1.0.0` and `0.9.0`.
  Keep exact advertisement/handshake gates; do not implicitly accept future
  caret-compatible `1.x` versions.
- VS Code 1.136 and 1.142 `code-tunnel agent host` both write standalone
  registry metadata `0.1.0`, but negotiate wire `0.9.0` and `1.1.0` respectively.
  Negotiate only audited dialects for this metadata alias, then record the
  selected `wireProtocolVersion` in the managed entry. Reconnect with that exact
  version, preserving Host identity. Editor/core connections offer only their
  expected wire version. Never offer `0.1.0` as a wire protocol.
- Start local managed Hosts with `agent host --new-instance --foreground`.
  The default command detaches/reuses a supervisor, so launcher exit is not a
  Host-liveness signal and does not provide an owned lifecycle.
- QQ only operates existing Sessions created and authenticated on the PC.
  Do not reintroduce mobile Session creation, model selection or a credential
  Broker. Retired command strings in migrations only preserve upgrade safety.
- Reuse a retained managed Host for later discovery/reconnect operations on
  the same target. Replacing its instance invalidates every existing binding.
  Cleanup local owned instances with `agent kill --instance-id` and verify the
  registry entry disappears; Remote SSH cleanup closes only the local tunnel.
- The vendored npm package has package version `1.0.0`, not wire version `1.1.0`.
  Regenerate it with `scripts/vendor-ahp-client.ps1 -Force` and update the
  lockfile through npm; do not substitute the npm registry tarball.
- Never make a Host appear compatible by only adding a protocol string or
  removing either version gate. Upgrade the audited types/reducer and adapt
  wire behavior first.
- AHP 0.9 represents `chat/error` as a durable error response part and supports
  `chat/turnResume`. Normalize legacy top-level errors before snapshots/actions
  reach reducers or observers, and keep `ChatTurnResume` in the chat action
  allowlist.
- Keep the provider mirror allowlists in sync with audited session/chat actions,
  including chat order, MCP background requests, background work, canvas catalogs,
  changesets, movability, and read/archive flags. Unknown actions still fail closed.
- Editor Session snapshots can exceed 8 MiB for long histories. The audited
  named-pipe transport accepts up to 32 MiB; keep the bound and its Windows
  transport regression test. Do not remove the limit or lower it below the
  real Host probe size.
- Validate changes with adapter typecheck, tests, build, and a real Host probe.
  The VS Code 1.142 Editor probe must show advertised/selected `1.1.0` and
  successfully hydrate an existing session/default chat without dispatching.
  The VS Code 1.142 standalone probe must show registry `0.1.0` and wire `1.1.0`;
  preserve legacy `0.9.0` negotiation and reject unaudited versions.
