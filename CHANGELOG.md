# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-18

First release. `dsh-dag-orchestrator` gives the DeepSeek Harness **resumable
multi-task parallel DAG orchestration**: strictly-validated static DAG specs,
a tick-driven reconcile engine, `node:sqlite` persistence with a per-run
event hash chain, crash reconciliation on apply, and one DAG task node = one
programmatic native-subagent delegation via `ctx.subagents`. The scheduler
core is a narrowing port of task-weaver's `packages/scheduler/` (migration
map: DESIGN §9); the plugin pattern follows dsh-worktrees /
dsh-plugin-subagents. This release carries milestones M1 + M2 + M3 in full
(20 tasks, T01–T18 + T19/T24).

### Documentation (2026-08-18) — README rewrite

- Both READMEs (`README.md` / `README.zh.md`) rewritten in plain language,
  still section-for-section aligned. New shape: what the plugin gives you →
  why (serial single-subagent jobs, restart loses the plot) → the five
  tools → install from a checkout with per-step explanations and an
  expected result → quick start with a minimal spec and the plan/tick/
  status loop → crash recovery in user terms → the task-spec fields that
  matter → trimmed configuration table → troubleshooting table →
  references & credits (task-weaver scheduler, `ctx.subagents`,
  dsh-session-query-sqlite). Internal vocabulary (M1/M2/M3, task ids,
  semver prerelease arcana, test counts) was dropped from the READMEs.

### CI (2026-08-18)

- CI flake fixed: `boundedRace`'s settle timer is no longer `unref()`'d.
  With only a hanging attempt in flight, the unref'd timer let the event
  loop drain mid-`tick()` — quiet CI runners died inside the await with
  the promise still pending ("Promise resolution is still pending but the
  event loop has already resolved", ubuntu/macos legs). A ref'd timer for
  the duration of the bounded wait is the correct contract; reproduced
  deterministically in a bare process before the fix. 514/514 locally.
- Added gitleaks secret scanning: `.github/workflows/gitleaks.yml` (full
  history scan on every push/PR) and `.pre-commit-config.yaml` for local
  commits; matches the gateway-provider repo's setup.
### Added

**Five-tool family** (all new global-layer names — no official takeovers, no
preset adaptation, zero host patches; every tool `isConcurrencySafe: false`
except read-only `dag_status`):

- `dag_plan` — strict WorkflowSpec validation (aggregated `dag.*` error
  codes with precise paths), run + tasks persisted in one transaction with
  a `run.created` event, an inline first tick (maxRounds 1) so progress is
  visible immediately, and `resume:true` re-attaching to a same-name
  non-terminal run (default loud `dag.name_exists`).
- `dag_status` — projection query: all-runs summary or per-run detail at
  `summary | tasks | attempts | events` (hash-chain tail window, default 50,
  per-task filter).
- `dag_tick` — the bounded multi-round reconcile pump: clamped `max_rounds`
  (default 4, max 16) and `settle_ms` (default 10000, max 60000) as a
  WHOLE-CALL budget that never resets per round (the 16×60s self-spin
  guard), a two-straight-zero-settle-rounds early exit, and the tick
  summary with the `waiting_on` classification
  (`nothing | in_flight_attempts | approval | external`) plus `next_hint`.
- `dag_control` — run level `pause` (admission closes, in-flight drains to
  paused) / `resume` / `stop` (non-running tasks cancel in the control tx;
  in-flight attempts abort outside it and land cancelled on the next tick,
  which also aggregates the run); node level `retry_task` (failed |
  blocked(upstream_failed) | blocked(merge_conflicted) → immediate
  retry_wait + `task.retry_requested` manual event — NOT billed against
  the spec `retryOn` budget; revives a failed run) and `cancel_task`
  (pending/ready/queued/blocked; running tasks must be stopped at run
  level). Illegal source states fail loud; re-sending an action already in
  effect is an idempotent re-affirm.
- `dag_approve` — decides a parked approval gate in ONE transaction
  (decision + `approval.decided` event), never touches the task (the next
  tick's `reconcileApprovals` promotes approved→succeeded / rejected→failed
  `dag.policy_denied`); echoes the gate's `approval_prompt` and a
  `next_hint`; loud `dag.already_decided` on repeat decisions.

**Engine** (the ten-step single reconcile round, a narrowing of
task-weaver's 14-step tick):

- promoteReady / propagateDownstream / buildQueue / finalizeRunIfDone /
  drainToPaused ported from readiness.ts / scheduler-driver.ts /
  scheduler-loop.ts with the three hard invariants inherited and enforced:
  every attempt commits its terminal in its OWN transaction (no batched
  sibling terminals), ready evaluation + state updates share one
  transaction while dispatch and all I/O stay outside, and every projection
  change emits its event in the same transaction.
- claimTask with the TOCTOU fix: transaction re-read + double CAS
  (ready→queued→running, two version bumps, both events) + attempt row +
  `attempt.claimed` in one tx.
- shouldRetry ported: exponential backoff + jitter, retryOn policy-key
  filtering (`transient_network | permanent | internal`; timeout-class
  failures map to `transient_network`), manual/recovery retries marked and
  exempt from the budget.
- Failure propagation: a failed `block_downstream` task blocks its
  succeeded-condition downstreams `blocked(upstream_failed)`;
  `failurePolicy: isolate` opts out.
- Auto-dispatch result mapping (§4.5): completed + declared-output gate
  (`dag.missing_output` / `dag.output_schema_violated`), error → transient
  `dag.agent_error`, timer-first aborts → `dag.attempt_timeout`,
  control-plane aborts → cancelled semantics, max-tokens / refusal →
  permanent, infra rejections → `dag.infra` (not retryable, loud).
- The optional `autoTickMs` Timer (default 0 = off): the no-dispatch
  reconcile over every non-terminal run — approvals, harvesting,
  promotion, propagation, finalization, pause-draining, plus the idempotent
  stop-abort sweep. Dispatch NEVER happens from the Timer (no live agent in
  a Timer context — the honest dies-with-host boundary); the interval is
  `unref`'d, its callback never throws into the host, and teardown clears
  it before the store closes.

**Persistence** (`lib/dag-store.js`, the single sqlite outlet):

- `node:sqlite` single database (WAL, `busy_timeout 5000`, `application_id`
  ownership magic, `user_version 1`); the file is created exclusive `wx`
  0600 with 0700 parent directories; a foreign or wrong-version database
  refuses to open loud; `:memory:` supported.
- Seven tables (runs / tasks / attempts / events / approvals / outputs),
  fully parameterized prepared statements, `BEGIN IMMEDIATE` transactions
  with nested-tx rejection.
- Per-run event hash chain: `hash = sha256(prev ∥ canonical)` computed
  inside the same transaction (canonical JSON = sorted keys, no spaces);
  full-chain verification on load; a mismatch parks that run failed +
  `recovery.chain_broken` without affecting other runs.

**Spec validation** (`lib/spec-validate.js`):

- zod strictObject at every level over the §7.1 crop table; structural
  rules: unique ids, dependency existence / self / cycle (three-color DFS),
  `task://` input reachability against declared outputs, the kind field
  matrix (approval: no delegation fields, no retry > 1; agent: prompt
  required; merge: ≥1 worktree-declaring succeeded upstream), one output
  per task, worktree/cwd conflict rejection, stable `dag.*` codes with
  precise paths, aggregated reporting.
- The bridge boundary is structural (O2): `permission_mode` /
  `reasoning_effort` and any backend outside `native|spawn|fork` are
  rejected with the dedicated `dag.bridge_unsupported` code.
- Dependency content gates (T18): optional `gate: {artifact, expect,
  value?}` with the five finite boolean operators (`exists | not_exists |
  contains | not_contains | equals`), value-required/forbidden pairing, and
  artifact-reachability — fail-closed when an evaluator is unavailable.
- `specHash`: normalized (defaults filled) + canonical JSON + sha256,
  sharing the store's canonical serializer so the two hashes cannot drift.

**Execution layer** (`lib/executor.js`, native binding):

- One task node = one programmatic subagent delegation
  (`ctx.subagents.start(backend ?? 'spawn', request)`) hung off the live
  pumping agent; the engine holds the reflected `run.result` promise and
  harvests on tick — dispatch never awaits the result.
- Full §4.2 request surface: label, prompt, parent, signal,
  agentOptions (provider/model/maxTokens), persona, outputSchema, cwd,
  maxDepth (with `assertSubagentMaxDepth` — the single whitelisted
  `@deepseek-ai/dsh-subagent` import — asserted before every dispatch),
  per-attempt `AbortController` + timeout timer with abort-source
  discrimination (timer-first → timeout, control-first → cancelled).
- Prompt assembly (§7.4): upstream outputs inlined as DATA between explicit
  boundary markers; a per-input byte cap (default 32 KiB,
  `inputInlineLimitBytes`) fails loud `dag.input_too_large` instead of
  truncating.
- Anti-injection filter floor: every task subagent dispatches with
  `toolFilter deny [dag_plan, dag_status, dag_tick, dag_control,
  dag_approve, subagent, subagent_fork]`; spec `deny` entries are appended
  (a task can only add denials), spec `allow` passes through, and
  `delegation: true` is the one sanctioned narrowing (it removes only the
  two `subagent*` entries — the dag_* control plane can never be lifted).
- Dispatch-time cwd gate (red line 9): an explicit `task.cwd` must be
  absolute, exist, and realpath inside the run's base-cwd subtree ∪
  `config.allowedRoots` (symlink-hardened on both ends); denial = permanent
  `dag.cwd_denied`, no subagent started. `requireWorkspaceRegistration:
  true` fails CLOSED for every explicit cwd (no registry channel exists —
  the switch must not degrade into a no-op). `request.cwd` is forwarded
  natively (verified against `@deepseek-ai/dsh-subagent` rc.6's
  `resolveChildCwd`).

**Approval gates** (M2, `kind: approval`):

- The three-branch port of runApprovalTask: already-approved → success;
  rejected → permanent `dag.approval_rejected`; otherwise insert the
  pending approval and PARK in one transaction (attempt failed +
  `approval_pending` reason + task blocked(approval_pending) + events),
  slot released outside. The park is excluded from ready re-evaluation;
  `dag_approve` + the next tick's reconcile is its only exit.

**Crash reconciliation** (apply-time, strictly before tool registration):

- Per-run chain verification (non-terminal mismatch → failed +
  `recovery.chain_broken`; terminal-run mismatches are audit warns only).
- Never-dispatched claims (state `claimed`, or `running` with no
  child_session) auto-fail `recovery.no_dispatch` and immediately
  auto-retry — the ONE automatic action the bounded policy allows.
- Dispatched orphans (`running` with a child_session) park
  `attempt.orphaned` + task failed + `recovery.action_requested` carrying
  the `child_session` for manual inspection; no auto-success, no invented
  rollbacks.
- Orphan outputs rows (produced by a non-terminal attempt) surface as an
  audit warn list, never a load blocker. `pausing` runs keep their intent.

**dsh-worktrees composition** (M3):

- The opportunistic engine seam (`lib/worktrees-seam.js`): probes
  `ctx.get('worktreesEngine')` at apply and re-probes on every use (never
  `inject` — an optional seam must not block load); a face is admitted
  only when both `getMergeQueue()` and `getWorktreeService()` are
  functions; absence degrades loud at the consumer
  (`dag.worktrees_unavailable`), never silently. Agent-only DAGs are
  unaffected. NOTE: the provider-side service exposure is not yet
  implemented in dsh-worktrees; the consumer-side contract of record is
  the `WorktreesEngineFace` JSDoc.
- Worktree-declaring tasks (`worktree: {task, baseRef?}`): create-or-reuse
  a worktree BEFORE dispatch (origin `'dag'`, correlationId = attemptId);
  the path replaces task.cwd as the subagent cwd; the record id is stamped
  into `attempt.dispatched` for merge-source wiring; terminal states never
  delete the worktree (lifecycle belongs to the worktrees plugin);
  create/lock trouble maps to transient `dag.worktree_create_failed`.
- The merge executor (`kind: merge`, the DrainOutcome five-state mapping):
  succeeded → succeeded with `integratedCommit` in outputs; no_changes →
  succeeded (empty integration is legal); conflicted → PARKED
  blocked(merge_conflicted) with conflictFiles + retained worktrees — a
  human `worktree_queue resolve/retry` then `dag_control retry_task` is
  its only re-run edge; failed → transient `dag.merge_failed` (retry
  policy decides); queued/queued_ahead → transient re-poll backoff that
  never exhausts the retry budget.
- The verify contract gate (T18): `verify: {expectOutput, expectStatus}`
  evaluated at terminal-commit time against THIS attempt's structured
  output (`evaluateVerifyGate` direct port, receipt source rebound to the
  outputs table); a miss synthesizes permanent `dag.verify_gate_failed`;
  no declaration → evidence `none_declared`, gate skipped.

**Assembly, config, discipline**:

- `apply(ctx, config)` in the fixed order: zod-strict config → the
  dsh-tools dual-instance self-check (Symbol probe with dedupe-fix
  guidance) → store open → crash reconciliation → executor / admission /
  engine assembly → tool registration per the `register` switches →
  teardown effect (dispose in-flight → engine sweep → store close,
  idempotent) → the optional autoTick effect. Always returns `undefined`.
- Config surface (zod strict, unknown keys loud): `register.{plan,status,
  tick,control,approve}` (default all true), `dbPath` (default
  `~/.dsh/dag-orchestrator/dag.db`, `~` expanded, `:memory:` supported),
  `defaultMaxRunningAgents` (4), `defaultQueueCapacity` (16),
  `inputInlineLimitBytes` (32768), `autoTickMs` (0), `allowedRoots` ([]),
  `requireWorkspaceRegistration` (false).
- `npm run lint` mechanically enforces the core disciplines:
  `node:sqlite` / `DatabaseSync` only in `lib/dag-store.js`; `db.exec(`
  arguments allowlisted to module-constant PRAGMA/DDL;
  `@deepseek-ai/dsh-subagent` imports whitelisted to
  `assertSubagentMaxDepth` in `lib/executor.js` only (positive + negative
  test cases pin the checks).
- GitHub Actions CI (`.github/workflows/ci.yml`): macOS/Ubuntu × Node
  22.13/24 running `npm ci` → `npm run lint` → `npm test`. The three
  `@deepseek-ai` peers are registry-published and lockfile-pinned, so a
  bare runner installs them from npm (verified clean-room at the time of
  that milestone: 479/479 + lint clean with no live harness; the suite
  has since grown — 493 at 0.1.0 close, 508 after the post-release audit
  below). Windows is deferred until node:sqlite is validated on a Windows
  runner (noted in the workflow).

### Fixed

(M1/M2/M3 review batches landed during development, folded into 0.1.0.)

- The settle budget is a WHOLE-CALL budget consumed across rounds and never
  reset (closing the 16×60s self-spin window pinned by a dedicated test).
- `classifyWaitingOn` distinguishes a merge-conflict park (`external`,
  with a worktree_queue → retry_task hint) from an approval park
  (`approval`) and from upstream-blocked waits (`external`).
- The no-dispatch autoTick runs promoteReady too — without it a Timer pass
  that promoted a gate but never re-evaluated its downstream could see
  "only dead-blocked tasks" and finalize a healthy run failed
  permanently (dead-block dead-lock, reproduced then fixed).
- stop's abort sweep is re-armed on every reconcile pass (oneRound and
  autoTick), so a straggler dispatched across the stop is swept too.
- Task-level control actions validate the RUN state first: a failed run
  stays actionable (retry_task revives it), while cancelling/cancelled/
  succeeded runs refuse loud `dag.invalid_run_state` before task
  validation.
- `retryOn: ['timeout']` is rejected with migration guidance at validation
  time — it was a dead key (`failureTypeToPolicyKey` maps timeout-class
  failures to `transient_network`).
- `blocked(upstream_blocked)` is refused for retry_task (the upstream has
  not settled; re-arming the victim cannot clear it) with guidance to
  retry the upstream instead.
- **M3 final review M-A (isolation):** the worktree reuse scope is narrowed
  to DESIGN §11.3's authorization — the SAME task's re-dispatch. Plan time
  now rejects two tasks declaring the same `worktree.task` slug
  (`dag.worktree_slug_conflict`, one error per later declaration), and the
  runtime reuse probe only reuses an active record whose `correlationId`
  (the create-time attemptId) belongs to the dispatching task's own
  attempt history (`taskAttemptIds` in the dispatch ctxInfo). A foreign or
  ownership-unprovable record (missing `correlationId` — older services)
  is conservatively NOT reused: the dispatch falls through to create,
  which surfaces an occupied slug loud (transient
  `dag.worktree_create_failed`) instead of two parallel subagents
  silently sharing one checkout. The seam contract records the optional
  `correlationId` field on `findActiveByTask` records.
- **M3 final review M-B (verify gate on merge):** a merge task declaring
  `verify` is now actually gated. The verify completion gate previously
  hung only on `harvestSettled` (agent attempts), while merge attempts
  terminal-commit inside dispatchLoop — the declaration validated clean
  and was silently skipped (red line 1's class). The merge executor's
  success path (succeeded | no_changes) now evaluates the same
  `evaluateVerifyGate` before `commitTerminalAndRelease`, with the §7.3
  receipt substitution (receipt = the pending integratedCommit output;
  its status view is the `integratedCommit`). A miss synthesizes permanent
  `dag.verify_gate_failed` through the same terminal machinery (a passing
  contract stamps `verifyStatus: 'pass'` on `attempt.succeeded`); the
  conflicted-park / failed / queued paths are non-success and untouched.
- **M3 review m-1 (seam contract unification):** `lib/worktrees-seam.js`'s
  `enqueue` JSDoc previously wrote the DAG passing
  `repoKey/repoRoot/sourceBranch/sourceHead` — contradicted by the
  `lib/executors/merge.js` contract (the DAG passes only four keys
  `worktreeId`/`integrationBranch`/`origin:'dag'`/`correlationId`; the
  provider resolves the git facts server-side). The seam JSDoc is unified
  to the merge.js version (safer: the DAG never learns git facts), and the
  canonical consumer contract anchor is the merge.js module header.
- **M3 review m-3 (composition root-alignment guidance):** the README (en
  + zh) worktrees-composition sections now call out that a real
  composition requires aligning dsh-worktrees' `worktreeRoot` (default
  `~/.dsh/worktrees/`, outside the repo subtree) with this plugin's
  `allowedRoots` — otherwise an engine-provided worktree path is denied by
  the cwd gate and the task spins on transient `dag.worktree_create_failed`.
- **M3 review m-5 (queued payload single key):** the merge executor's
  `queued_ahead` payload count key is now `queued_ahead` ONLY — the
  camelCase `queuedAhead` compatibility branch was removed and the
  snake_case family made the contract (mirrored in the merge.js module
  header and the seam JSDoc); test fixtures updated accordingly.

### Known issues and design notes (from the milestone reviews)

Open items are accepted at 0.1.0 with their mitigations; none block the
release. The final item is a note, not an open issue.

1. **Cross-plugin shape drift (m-2, integration risk):** the real
   dsh-worktrees merge queue's shapes differ from this plugin's consumer
   contract of record — its `drain()` returns `{drained}` / `{blockedBy}`
   rather than the five-state `DrainOutcome`, `enqueue` takes seven params
   (not the four-key DAG shape), `create` requires a `repoKey`, it has no
   `findActiveByTask`, and it does not `ctx.provide('worktreesEngine')`.
   The provider-side adapter is a **dsh-worktrees-side TODO**; the
   consumer contract is anchored at `lib/executors/merge.js`'s module
   header (and mirrored in `lib/worktrees-seam.js`'s `WorktreesEngineFace`
   JSDoc). Until that adapter lands, merge/worktree tasks degrade loud
   (`dag.worktrees_unavailable`) and agent-only DAGs are unaffected.
2. **`lastSucceededWorktreeId` is O(sources × events) (m-4, performance):**
   the merge executor's source resolution scans every `attempt.dispatched`
   event for each worktree-declaring upstream. Fine at the current scale
   (dozens of sources × hundreds of events); revisit with an index or a
   cached last-succeeded probe before the event volume grows materially.
3. **`verify.expectOutput` lacks plan-time reachability validation:** unlike
   `inputs` (producer must be an upstream that declares the output), the
   verify block's `expectOutput` is only checked at gate time — a typo'd
   name yields `dag.verify_gate_failed` (missing) at run time rather than a
   precise plan-time error. Plan-time wiring is a future hardening item.
4. **Kind-matrix dead fields on merge tasks:** the spec kind matrix accepts
   `prompt` / `cwd` / `worktree` / delegation fields on `kind: merge`
   tasks, where the merge executor silently ignores them (the executor
   consumes only dependsOn/merge/outputs/retry). Not a red-line-1 defect
   (verify M-B fixed the one field with execution semantics), but the
   matrix could reject them for the same strictness. Future hardening.
5. **Design note — merge verify receipt view (closed, reviewer-endorsed):**
   for merge tasks the verify receipt's status view is the
   `integratedCommit` string (the persisted row keeps its original shape),
   so "pin verify on an expected commit" is a satisfiable capability.
   Independently re-reviewed and endorsed (the alternatives — plan-time
   rejection, shape regexes, or auto-passing no_changes — were all strictly
   worse). Recorded as a deliberate narrowing of the ported verify-gate
   receipt semantics; DESIGN §7.3 is updated accordingly. Not an open
   issue — the review classified it handled; this note exists so the
   semantic narrowing is visible in the release record.

### Fixed (post-0.1.0 audit batch)

- **P2 — `dag_plan` spec argument type + string-form tolerance (true-machine
  integration):** the `spec` parameter was declared `type:'json'`, which
  compiles to an annotation-only wire schema with NO type constraint;
  glm-5.3 + newapi gateways serialize an unconstrained object argument as a
  JSON STRING, arriving at execute as a string →
  `dag.schema_invalid — Expected object, received string` (smoke-tested
  15/15, isolated-reproduced). Two layers:
  1. **Schema:** `spec` is now `type:'object', additionalProperties: true`
     (a real object constraint every tool-calling gateway honors; the open
     additionalProperties keeps the WorkflowSpec face open — its strict
     down-field validation stays zod's job in `lib/spec-validate.js`). Only
     `dag_plan` carried an object-typed input — the sibling tools
     (dag_tick/dag_status/dag_approve/dag_control) take scalar parameters
     only, so none were affected.
  2. **Execute defense:** a string-form spec is `JSON.parse`'d by the new
     pure `parseSpecArg` helper before strict validation; a malformed string
     is kept raw so the strict path reports `dag.schema_invalid` with its
     original message (validation strength unchanged). Defense for any other
     model/gateway with the same stringification behavior.
  DESIGN §8.1 rationale updated. New tests: the typed face rejects a
  string-form spec (`ToolArgsError`), `parseSpecArg` round-trips a legal
  string and passes a malformed string through, and the malformed-string →
  `dag.schema_invalid` fallback pins the容错 semantics.

- **P1 — deny floor vs `tools.restrict()` unknown-name throw (config-face
  self-destruct):** `DEFAULT_TASK_FILTER` unconditionally denied seven
  names, but the harness `tools.restrict()` (reached inside the subagent
  child-creation window via the subagents plugin's
  `applyChildComposition`) throws on ANY allow/deny name the child scope
  cannot see. With a `dag_*` register switch off (`register: {approve:
  false}`) or on a host without the subagents plugin's delegation tools,
  EVERY dispatch failed inside the creation window → transient
  `dag.dispatch_failed` → the retry budget burned on a permanent config
  fact → the whole DAG unusable. Fixed in two complementary layers, both
  preserving red line 5 (a REGISTERED `dag_*` tool can never be lifted;
  spec `deny` only appends; `delegation: true` still removes only the two
  `subagent*` names):
  1. apply-time deterministic trim — the deny base drops `dag_*` entries
     whose `register` switch is off (computed once per executor from the
     resolved config);
  2. dispatch-time registry intersection — `apply()` now passes the host
     `ctx.tools` face into the executor, and each dispatch intersects the
     merged filter (floor + spec `deny` + spec `allow`) with
     `tools.view(execAgent).restrictableNames` (the pumping agent's
     restrictable set, a superset of the child's since the child joins the
     parent's preset standing scope). An unregistered name is vacuous to
     deny, so dropping it opens nothing. A ctx without a usable view face
     (fake ctx, unseen host shape) degrades to the un-intersected floor.
  Regression tests cover both layers, the assembly path (a real
  `dag_plan` dispatch under `register: {approve: false}` asserts the
  dispatched filter contains no unregistered name), the allow/deny
  intersection, and that the floor cannot be lifted by spec `allow`.
- **P2 — `backend: 'native'` ghost value:** spec-validate admits
  `native`, but the harness subagent runtime registers only the two
  in-process provider names `spawn`/`fork` — a raw `native` hit
  `NO_PROVIDER` on every dispatch and burned transient retries. The
  executor now maps the alias at dispatch (`native` → `spawn`, the same
  native channel `dsh-plugin-subagents` resolves its own
  `backend: 'native'` to); `spawn`/`fork` pass through; the spec value
  itself is kept verbatim (specHash stability preserved). The
  `dag.bridge_unsupported` message now documents the alias, and README
  (en+zh) note it in the field table.
- **P2/P3 — stale metadata:** `lib/engine.js`'s step-1 comment no longer
  calls `reconcileApprovals` an "M1 no-op placeholder" (T12 landed);
  `lib/ready-evaluator.js`'s `READY_BLOCKED_CODES` comment no longer calls
  the two `dependency_gate_*` codes "M3 placeholders" (T18 activated
  them); the CHANGELOG's clean-room count is annotated as a historical
  snapshot (479 at that milestone → 493 at 0.1.0 → 508 now). Test counts
  in README/README.zh/AGENTS updated to 508.
- **P3 — silent discarding of spec-authored allow/deny names (zero
  feedback):** the P1 dispatch-time registry intersection trimmed floor AND
  spec-authored filter names alike with no signal. A spec typo'ing a tool
  name (or naming a tool this host does not register) had its `allow`/`deny`
  entry vanish silently while the task ran on — against the plugin's
  "never a guess, always loud" style. The intersection now attributes each
  dropped name to its source: a FLOOR name trimmed stays silent (the tool
  objectively does not exist in this deployment), while a spec hand-written
  `allow`/`deny` name dropped for the same reason `logger.warn`s once per
  dispatch — including the task id, the dropped-name list, and a pointer to
  check against the host's actually-registered tool names. It is a warning,
  deliberately NOT a throw (a throw here would re-introduce the P1
  dispatch-window failure). No logger wired → the warning is skipped
  silently. The README toolFilter floor sections (en+zh) now note the
  degradation window: on a host where the tool registry cannot be probed,
  an unregistered spec-authored name may error at subagent creation time.
  New tests: a spec with an unregistered `allow` name dispatches
  successfully AND warns (injected fake logger asserted); an unregistered
  spec `deny` warns while the registered name stays; a dropped FLOOR name
  stays completely silent.
- Docs: README.md/README.zh.md add the `toolFilter` floor semantics
  (bilingual, section-aligned) and the `native` alias note in the
  `backend` field row; SECURITY.md documents the floor trim (a trim never
  removes an entry for a registered tool); DESIGN §4.2 records both audit
  revisions at their anchor lines.

Suite: 514/514 green (511 prior + 3 new P3 warning tests), lint clean, zero network.

### Docs (2026-08-18, README completion ahead of the GitHub publish)

README.md + README.zh.md, updated together (section-for-section aligned):

- **Compatibility banner** (both languages): compatible with DSH
  `0.1.0-rc.7` (npm latest) and `0.1.0-rc.6`; `peerDependencies:
  ^0.1.0-rc.6` satisfies rc.7 under semver (same-version-tuple prerelease
  rule; `0.1.1-rc.x` would not). Verified against rc.7: `defineTool` /
  `TOOL_RUNTIME_SCHEDULER` exports intact, the new `DefineToolOptions`
  fields all optional, 514/514 green with peers linked to rc.7.
- **Corrected the stale worktrees-seam status** (sequence ④ + the M3
  section, en+zh): dsh-worktrees' provider-side `worktreesEngine` service
  face IS implemented (`ctx.provide('worktreesEngine', …)` over the same
  service/queue singletons, `lib/engine-face.js`) — the old "not yet
  implemented / until it lands" text predated that landing and is removed;
  the loud `dag.worktrees_unavailable` degradation is now stated as the
  absent-face case only.
- **Documented the `request.cwd` runtime dependency honestly**: the
  official `SubagentStartRequest` has no `cwd` field, and the unpatched
  rc.6/rc.7 runtime drops a per-call cwd (worktree isolation and explicit
  `task.cwd` are silently ineffective). The verified remediation is stated
  in sequence ④, the Install section, the `cwd` field note, and the M3
  section: install dsh-plugin-subagents and run its `patches/install.sh`
  (both patches apply verbatim onto rc.7 anchors); re-run it after every
  dsh upgrade. The prior "probe VERIFIED against rc.6" claim (which
  mistook patch-level forwarding for native support) is superseded by
  this behavior-level statement.
- **Cross-repo links absolutified** for the standalone GitHub repos
  (dsh-plugin-subagents / dsh-worktrees →
  `https://github.com/Luck9Star/<repo>`; the old `../` relative paths 404
  outside the monorepo layout).
- **Test count refreshed**: 508 → 514 (the suite grew; both language
  versions).
