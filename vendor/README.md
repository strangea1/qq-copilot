# Vendored Agent Host Protocol client

`microsoft-agent-host-protocol-1.0.0.tgz` is generated from:

- Repository: <https://github.com/microsoft/agent-host-protocol>
- Revision: `13277064ffa20cceab1bfc04ba876a1b181b6e80`
- VS Code 1.142.0 commit: `6d62b1d5dace93a33b3a6d3eca1317bd5e15bcb1`
- VS Code 1.142 registry overlay: `ahp-vscode-1.142.patch`
- License: MIT, included in `LICENSE.microsoft-agent-host-protocol`
- SHA-256:
  `c59b0bddbbf99c79e0f92977dae024ea962c96491594f0de06e92a10dc447f39`

VS Code's `state/protocol/.ahp-version` pins this revision. Its generated types,
reducers and client speak AHP `1.1.0`. The package version is `1.0.0`, independent
of the wire version. The overlay adds the audited `1.1.0` offer and VS Code's
`0.10.0` legacy compatibility dialect alongside upstream's `1.0.0` and `0.9.0`
baselines. Both advertisement and handshake gates remain exact allowlists;
future `1.x` versions are not implicitly trusted.

The `0.1.0` standalone registry metadata is shared by VS Code 1.136 and 1.142.
Managed connections negotiate only audited wire dialects and pin the confirmed
version for retained connections. Editor and core connections offer only their
expected wire version. Registry `0.1.0` is never offered as a wire version.

Regenerate the tarball with:

```powershell
.\scripts\vendor-ahp-client.ps1 -Force
```

The script applies the overlay before TypeScript generation and replaces the
tarball only after a successful build. Do not replace it solely on the basis of
the npm package version.
