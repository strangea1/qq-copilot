# Mobile Session Creation Handoff

- Status: unresolved
- Observed: 2026-09-04, VS Code 1.136.0
- Scope: QQ `/new advanced` against the authorized local target `C:\test`

This document records the evidence gathered during the real mobile creation
test. It intentionally excludes OAuth tokens, QQ identities, full Session
UUIDs, endpoint tokens, connection tokens, and raw logs.

## 1. User-visible failures

The test exposed four related defects:

1. `/new advanced` offered an approval-mode picker but no model picker.
2. The first task failed immediately with:

   ```text
   Execution failed: Error: Session was not created with authentication info or custom provider
   ```

3. The failed Session remained visible in QQ as `A8U26 · test`.
4. `/new` was initially rejected while another monitored Session had an active
   Turn. This contradicts the intended multi-Session behavior: unrelated
   Sessions must be allowed to run concurrently.

## 2. What was actually created

`A8U26` is not only a stale Bridge row. The standalone Agent Host created an
`ahp-session:/...` resource, the Adapter established its default Chat
subscription, Bridge created a Binding, and both the bind command and first
message command were acknowledged.

The resource is nevertheless unusable:

- `createSession` created a deferred Session shell.
- The first `chat/turnStarted` was accepted.
- The Host emitted `chat/error` about 41 ms later with
  `errorType = authentication`.
- A second message to the same Session reproduced the same error in about
  6 ms.
- No `dispose_session` command followed the failed Turn, so the Session,
  Binding, catalog entry, short code, and managed Host remained visible.

The correct description is therefore: **the AHP resource was created and
bound, but the Copilot provider backing was never authenticated or
materialized successfully**.

## 3. Confirmed protocol evidence

### 3.1 Standalone protocol negotiation works

The compatibility work preceding this test is valid and must not be reverted:

- standalone registry metadata advertises `0.1.0`;
- the Host negotiates AHP wire protocol `0.9.0`;
- the Adapter preserves the advertised value and uses the audited
  `wireProtocolVersion = 0.9.0` mapping;
- Local Host startup, exact-instance cleanup, deferred Session confirmation,
  provisional Binding, and large Snapshot handling passed real Host probes.

See [AHP compatibility instructions](../.github/instructions/ahp-compatibility.instructions.md).

### 3.2 Authentication is missing

The standalone root Snapshot advertised the `copilotcli` provider with:

- an empty `models` array;
- required protected resource `https://api.github.com`;
- authorization server `https://github.com/login/oauth`;
- required scopes including `read:user` and `user:email`.

No AHP `authenticate` request occurred before `createSession` or the first
Turn. The create request contained only:

```json
{
  "autoApprove": "autoApprove",
  "isolation": "folder",
  "mode": "interactive"
}
```

VS Code's AHP implementation supports an initial-authentication resolver and
sends the standard `authenticate` request after `initialize`. The Agent Host
keeps accepted credentials in its authentication service and replays them to
providers. The QQ Adapter currently implements neither credential acquisition
nor this authentication handshake.

Do not solve this by reading or logging VS Code secret storage directly.
Authentication needs an explicit credential-broker design with consent,
refresh, revocation, and fail-closed handling.

Relevant implementation:

- [managed target connection](../adapter/src/managed-target.ts)
- [vendored AHP client](../vendor/)
- [VS Code status extension](../vscode-extension/)

### 3.3 Model discovery uses the wrong source

The Adapter currently searches `resolveSessionConfig.schema.properties` for
names or labels matching `model`:

- [resolveSessionConfig mapping](../adapter/src/managed-target.ts)
- [supportedField](../adapter/src/managed-target.ts)

The observed schema contained `isolation`, `autoApprove`, `permissions`, and
`mode`, but no model property. Consequently `/new advanced` skipped directly
to approval selection.

AHP exposes models through `AgentInfo.models`, not through the generic Session
configuration schema. In this test `AgentInfo.models` was empty because the
provider had not been authenticated. Model support therefore depends on:

1. authenticating the required protected resource;
2. observing the post-auth root-agent/model refresh;
3. presenting the selected `SessionModelInfo`;
4. carrying the resulting `ModelSelection` on the first user message/Turn
   according to the audited AHP types.

The current Bridge `send_message` command and `queueUserText` abstraction only
carry text. They must be extended before a model selection can be applied
correctly. Do not insert a guessed model key into `createSession.config`.

## 4. Why rollback did not happen

The creation workflow treats an acknowledged `send_message` with
`disposition = started` as successful:

- [run_ahp_creation_workflow](../src/service.rs)
- [creation failure handler](../src/service.rs)
- [turn error normalization](../adapter/src/event-normalizer.ts)

That acknowledgement proves only that the Host accepted the dispatch. It does
not prove that provider execution started successfully. The workflow clears
the creation wizard and reports success before observing the immediate
`turn_failed` event. Because the workflow already returned successfully, its
rollback handler is not invoked.

The implementation must correlate the first-task command result's Turn ID with
the corresponding AHP terminal event. The required policy decision is:

- either keep the creation transaction open until that Turn completes; or
- define a shorter materialization/readiness boundary that treats immediate
  provider/authentication failure as transactional failure while allowing
  long-running valid Turns to continue.

Whichever policy is chosen, a first-task authentication failure must:

1. detach and remove the new Binding;
2. dispose the exact Host Session;
3. publish an authoritative removal tombstone;
4. remove the catalog entry and short code;
5. restore the previous foreground only if no newer foreground intent exists;
6. retain and report explicit cleanup errors instead of claiming success.

## 5. Multi-Session creation requirement

The current global guard in
[ahp_creation_block_reason](../src/service.rs) rejects `/new` whenever any
Binding has:

- an active Turn;
- a queued message;
- a pending interaction; or
- any pending Adapter command.

This is incompatible with the supported multi-Binding architecture.
`/new` must be allowed while unrelated Sessions are running.

Replace the global activity guard with scoped safety checks:

- only one `/new` wizard/transaction at a time;
- sufficient protected Binding capacity remains;
- the selected target is authorized;
- no conflicting managed operation is already changing the same target;
- the new Session has its own Binding and command ordering;
- unrelated active Turns, queues, approvals, inputs, and per-Binding commands
  do not block creation.

## 6. Current production artifact

At handoff time:

- QQ Gateway and Adapter are connected;
- Editor `0.9.0 -> 0.9.0` and Local Standalone `0.1.0 -> 0.9.0` are connected;
- pending commands and projections are zero;
- the failed test Session `A8U26` remains bound on `C:\test`;
- the creation wizard is clear.

Do not delete database rows manually. Clean `A8U26` through the managed
`unbind -> dispose_session -> removal tombstone -> Host prune` path, then
verify the Session is absent from both `ahp-sessions` and
`code-tunnel agent endpoints`.

## 7. Work that is already complete

The working tree also contains validated fixes that should be preserved:

- delta processing no longer deep-clones full Chat history for every token;
- streaming text uses chunk accumulation;
- pending state Snapshots are coalesced without dropping semantic events;
- named-pipe payload limit is a bounded 32 MiB for large histories;
- Session catalog publication is connected-only and deterministically
  deduplicated;
- managed Hosts are reused per target and cleaned by exact instance ID;
- deferred Sessions are held by confirmation and provisional subscriptions;
- offline replay uses persistent at-most-once batches and was verified with a
  real QQ two-trigger test;
- the integration switch script can start from a stopped Bridge.

Validation completed before this handoff:

- 99 Rust tests;
- strict Clippy with warnings denied;
- Rust release build;
- 41 Adapter tests;
- Adapter typecheck and build;
- real Editor and standalone protocol probes;
- production Gateway, Adapter, Host, and Binding health checks.

## 8. Recommended implementation order

1. Design and implement the secure AHP authentication broker.
2. Wait for authenticated `AgentInfo.models` and implement the model picker.
3. Extend the first-message command and AHP dispatch with `ModelSelection`.
4. Remove the global active-Turn creation restriction and add scoped
   concurrency tests.
5. Correlate the first Turn with creation and implement failure rollback.
6. Dispose the existing `A8U26` artifact through the supported path.
7. Repeat the real QQ test:
   - run an existing Session concurrently;
   - `/new advanced`;
   - select `C:\test`;
   - select a real advertised model;
   - select approval mode;
   - submit a harmless first task;
   - verify the new Session executes successfully;
   - force one first-Turn failure and verify no Session remains.

## 9. Security invariants

- OAuth tokens must never traverse QQ.
- Tokens must not be stored in SQLite, config files, logs, command payloads,
  test fixtures, or Git.
- Use only resources and scopes advertised by the Host.
- Authentication failure is fail-closed.
- Never treat an empty model list as permission to invent or hard-code a
  model.
- Preserve exact target authorization, foreground-intent ordering, command
  leases, and authoritative removal semantics.
