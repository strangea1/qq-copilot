# Vendored Agent Host Protocol client

`microsoft-agent-host-protocol-0.8.0.tgz` is generated from:

- Repository: <https://github.com/microsoft/agent-host-protocol>
- Revision: `f770e26b8483de59050e8de71b65a20efdab62d4`
- License: MIT, included in `LICENSE.microsoft-agent-host-protocol`
- SHA-256:
  `d17e139368c0c9d97a86abe68ac2d1f111b5215710fab7106fa5aa907dcb17b0`

The package version remains `0.8.0`, but its generated AHP `1.0.0` wire types
match the revision vendored by VS Code 1.135. The npm registry tarball with the
same package version predates this revision and is not wire-compatible enough
for this project.

Regenerate the tarball with:

```powershell
.\scripts\vendor-ahp-client.ps1
```

Do not replace it solely on the basis of the npm package version.
