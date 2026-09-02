# Vendored Agent Host Protocol client

`microsoft-agent-host-protocol-0.8.0.tgz` is generated from:

- Repository: <https://github.com/microsoft/agent-host-protocol>
- Revision: `a0bc67f840788f816c9b44bb1325181cb4c4661d`
- VS Code 1.136 registry overlay: `ahp-vscode-1.136.patch`
- License: MIT, included in `LICENSE.microsoft-agent-host-protocol`
- SHA-256:
  `575eef7a2a166b08b804c56768cc727c65cf8be0e6d080fb2381affed8495185`

The package version remains `0.8.0`, but its generated types and reducer plus
the checked-in registry overlay match the AHP `0.9.0` dialect advertised by
VS Code 1.136. The npm registry tarball with the same package version predates
this revision and is not wire-compatible enough for this project.

Regenerate the tarball with:

```powershell
.\scripts\vendor-ahp-client.ps1 -Force
```

The script applies the overlay before TypeScript generation and replaces the
tarball only after a successful build. Do not replace it solely on the basis of
the npm package version.
