// spec-validate — WorkflowSpec subset validation (DESIGN §7.1/§7.2, task T04).
//
// Ported (narrowed) from task-weaver packages/contracts/src/workflow-spec.ts:
//   * structural boundary: zod strictObject full table (§7.1 crop table) —
//     unknown keys rejected at every level;
//   * cross-task semantics: duplicate ids, dependency existence/self/cycle,
//     inputs DataRef reachability + declared outputs, kind field matrix;
//   * errors: stable `dag.*` codes + JSON path strings, never thrown —
//     the caller decides (source L326-341 shape, `dag.` prefix);
//   * specHash: normalized value (defaults filled) → canonical JSON
//     (sorted keys, no spaces — the SAME canonicalJson the event hash-chain
//     uses, imported from dag-store so the two hashes cannot drift) → sha256.
//
// M1 surface decisions (task brief T04):
//   * kind accepts agent|approval|merge at VALIDATION level (runtime gates
//     approval/merge later — contract integrity, mirroring the source);
//   * dependsOn.gate is REOPENED at T18 (M3): the optional
//     `gate: {artifact, expect, value?}` block validates strictly now
//     (five finite boolean operators — NOT a scripting surface) plus the
//     semantic value-required/value-forbidden pair and the
//     artifact-producer-must-be-upstream rule (source workflow-spec.ts
//     L140-155 / L541-584);
//   * bridge delegation surface is structurally gated: `permission_mode` /
//     `reasoning_effort` keys and unknown `backend` values are rejected with
//     `dag.bridge_unsupported` (O2 precondition);
//   * the §7.2 example carries a `merge: {integrationBranch}` block, so the
//     task schema accepts that optional block (shape only; M3 runtime).
//
// Discipline: plain JS ESM, no I/O, zero network; validateSpec NEVER throws
// on bad input data (it returns {ok:false, errors}); specHash throws LOUD on
// an invalid spec (hashing an unvalidated spec is a programming error).

import { createHash } from 'node:crypto'
import { z } from 'zod'

import { canonicalJson } from './dag-store.js'

// ---------------------------------------------------------------------------
// Stable error codes — `dag.` namespace (clients branch on code, not message)
// ---------------------------------------------------------------------------

/** Stable machine-readable codes (transcribed from DESIGN §7.2 / task T04). */
export const DAG_SPEC_ERROR_CODES = Object.freeze({
  schemaInvalid: 'dag.schema_invalid',
  unsupportedVersion: 'dag.unsupported_version',
  unknownField: 'dag.unknown_field',
  duplicateTaskId: 'dag.duplicate_task_id',
  unknownDependency: 'dag.unknown_dependency',
  selfDependency: 'dag.self_dependency',
  cycleDetected: 'dag.cycle_detected',
  inputNotReachable: 'dag.input_not_reachable',
  outputNotDeclared: 'dag.output_not_declared',
  kindFieldMismatch: 'dag.kind_field_mismatch',
  promptRequired: 'dag.prompt_required',
  tooManyOutputs: 'dag.too_many_outputs',
  bridgeUnsupported: 'dag.bridge_unsupported',
  /** T18: a content gate operator needs `value` but the spec omits it (source `workflow.gate_value_required`). */
  gateValueRequired: 'dag.gate_value_required',
  /** T18: an existence gate operator is set but `value` is also present (source `workflow.gate_value_forbidden`). */
  gateValueForbidden: 'dag.gate_value_forbidden',
  /** T18: the gate's `artifact` producer is not a direct/transitive upstream of the gate's own edge (inputs rule family). */
  gateArtifactNotReachable: 'dag.gate_artifact_not_reachable',
  /**
   * T16: a task declares BOTH `worktree` and `cwd`. The worktree branch
   * REPLACES task.cwd (the engine-provided worktree path is the effective
   * cwd, DESIGN §4.6/§11.3), so a co-declared cwd is a contradiction the
   * runtime would silently ignore — rejected with its own dedicated code
   * instead of the generic kind_field_mismatch (the conflict is between
   * two cwd sources, not a kind-matrix violation).
   */
  worktreeCwdConflict: 'dag.worktree_cwd_conflict',
  /**
   * T17: a merge task's succeeded-condition upstreams include NO
   * worktree-declaring task — the merge executor would have nothing to
   * integrate (its source set is exactly those upstreams' worktrees).
   * Rejected at PLAN time as a防呆 guard; the runtime re-checks and fails
   * loud `dag.merge_source_missing` for hand-seeded stores.
   */
  mergeSourceMissing: 'dag.merge_source_missing',
  /**
   * M3 review M-A: two tasks declare the same `worktree.task` slug. DESIGN
   * §11.3 scopes worktree reuse to the SAME task's re-dispatch ("retry_task
   * 重派发时…复用") — a shared slug would make two PARALLEL tasks race for
   * one checkout (and the second create would lose on the slug-derived
   * branch anyway). The slug is task-scoped naming, not a sharing primitive:
   * uniqueness is enforced structurally at plan time so the hazard never
   * reaches dispatch.
   */
  worktreeSlugConflict: 'dag.worktree_slug_conflict',
})

/**
 * @typedef {object} SpecError
 * @property {string} code    Stable `dag.*` code.
 * @property {string} [path]  Dot/bracket path into the spec ('tasks[3].dependsOn[0].taskId').
 * @property {string} message Human-readable detail (never branched on).
 */

/**
 * @typedef {object} SpecValidationOk
 * @property {true} ok
 * @property {object} value Normalized spec (defaults filled, task order kept).
 */

/** @typedef {SpecValidationOk | {ok: false, errors: SpecError[]}} SpecValidationResult */

// ---------------------------------------------------------------------------
// Grammar constants (DESIGN §7.1)
// ---------------------------------------------------------------------------

/** Task id — source L181: `[a-z][a-z0-9-]{0,62}`. */
const TASK_ID_RE = /^[a-z][a-z0-9-]{0,62}$/
/** Logical input URI — `task://<producer>/<name>` (§7.1 inputs row). */
const TASK_URI_RE = /^task:\/\/([^/]+)\/([^/]+)$/
/** Absolute POSIX path (project.root / task cwd — plugin hosts are POSIX). */
const ABS_PATH_RE = /^\//
/** Delegation backends this plugin can bind (§4.2; bridge CLIs are out). */
const BACKEND_VALUES = ['native', 'spawn', 'fork']
/**
 * Keys that target the BRIDGE delegation surface. They are not in the strict
 * task schema, so zod reports them as unrecognized keys — mapped to
 * `dag.bridge_unsupported` instead of `dag.unknown_field` so the O2 structural
 * gate fails with the actionable code.
 */
const BRIDGE_ONLY_KEYS = ['permission_mode', 'reasoning_effort']
/** Dependency conditions (no expression language — §7.1 dependsOn row). */
const DEPENDENCY_CONDITIONS = ['succeeded', 'completed']
/**
 * T18: the dependency-gate operator domain — a FINITE set of five boolean
 * checks over an upstream output's content, NOT a scripting surface (source
 * workflow-spec.ts L141-146 "Finite set of boolean checks").
 */
const GATE_EXPECT_VALUES = ['exists', 'not_exists', 'contains', 'not_contains', 'equals']
/**
 * Gate `artifact` refs are the same task:// URIs as inputs (§7.1) — the
 * source's `artifact://{producer}/{name}` DataRef renamed to this plugin's
 * outputs-table addressing.
 */
const TASK_URI_SHAPE_RE = /^task:\/\//
/** expect operators that REQUIRE a value (source L551-554). */
const GATE_VALUE_REQUIRED_EXPECTS = new Set(['contains', 'not_contains', 'equals'])
/** expect operators that FORBID a value (source L576). */
const GATE_VALUE_FORBIDDEN_EXPECTS = new Set(['exists', 'not_exists'])
/**
 * This plugin's retryOn POLICY-key domain (M8 fix, narrowed from the M1
 * shape): `shouldRetry` compares `failureTypeToPolicyKey(failureType)`
 * against retryOn, and that mapping sends BOTH 'transient' and 'timeout'
 * failures to the 'transient_network' key — a 'timeout' entry in retryOn
 * was therefore a DEAD key that could never match anything (M1 review M8).
 * Declaring the actual key domain keeps specs honest: a timeout-class
 * failure retries under 'transient_network'. (Task brief chose the enum
 * narrowing over the dual-key engine match: "声明意图与实际键域一致".)
 */
const RETRY_POLICY_KEYS = ['transient_network', 'permanent', 'internal']
/** Task kinds (M1 runtime: agent only; validation keeps all three). */
const KINDS = ['agent', 'approval', 'merge']
/**
 * Fields an approval task must not carry (§7.2 kind matrix): the flat
 * delegation fields + the M3 worktree/verify blocks. `outputs` is checked
 * separately (array presence, not undefined-ness).
 */
const APPROVAL_FORBIDDEN_FIELDS = [
  'backend', 'model', 'provider', 'persona', 'toolFilter',
  'cwd', 'maxTokens', 'maxDepth', 'delegation', 'worktree', 'verify',
]

// ---------------------------------------------------------------------------
// zod schemas — the §7.1 crop table, field by field
// ---------------------------------------------------------------------------

/**
 * ObjectJsonSchema light check (§7.1 outputs row): the schema must be an
 * object rooted at `type === 'object'`. Everything else (properties,
 * required, additionalProperties, …) passes through untouched — it is handed
 * to SubagentStartRequest.outputSchema verbatim at dispatch time.
 */
const objectJsonSchema = z.object({ type: z.literal('object') }).passthrough()

const absPathString = (field) =>
  z.string().regex(ABS_PATH_RE, `${field} must be an absolute path`)

/**
 * One dependsOn edge. STRICT; T18 reopens the optional M3 `gate` block
 * (five finite boolean operators, task:// artifact ref, optional value).
 */
const dependencySchema = z.strictObject({
  taskId: z.string().min(1),
  condition: z.enum(DEPENDENCY_CONDITIONS),
  gate: z.strictObject({
    artifact: z.string().regex(TASK_URI_RE, 'gate.artifact must be a task://<producer>/<name> URI'),
    expect: z.enum(GATE_EXPECT_VALUES, {
      errorMap: () => ({
        message: `gate.expect must be one of the five finite boolean operators (${GATE_EXPECT_VALUES.join('|')}) — there is no scripting surface`,
      }),
    }),
    value: z.string().optional(),
  }).optional(),
})

const retrySchema = z.strictObject({
  maxAttempts: z.number().int().min(1).default(1),
  backoffMs: z.number().int().min(0).default(1000),
  maxBackoffMs: z.number().int().min(0).default(60000),
  jitterRatio: z.number().min(0).max(1).default(0.25),
  // errorMap carries the M8 migration guidance: 'timeout' is a dead key
  // (failureTypeToPolicyKey maps timeout-class failures to
  // 'transient_network'), so the rejection must SAY where to go.
  retryOn: z.array(z.enum(RETRY_POLICY_KEYS, {
    errorMap: () => ({
      message: `retryOn entries must be policy keys (${RETRY_POLICY_KEYS.join('|')}); timeout-class failures retry under 'transient_network' (failureTypeToPolicyKey maps 'timeout' → 'transient_network')`,
    }),
  })).default([]),
})

/**
 * Task — strict object over the §7.1 crop table. Defaults that participate in
 * normalization (specHash input): dependsOn/inputs/outputs [], timeoutMs
 * 1_800_000, priority 0, failurePolicy 'block_downstream'. `retry` keeps the
 * source semantics: no task-level default; inner defaults fill only when the
 * block is present.
 *
 * Per-task refinements (need parsed data, single-object scope):
 *   * backend value gate → `dag.bridge_unsupported`;
 *   * outputs.length ≤ 1 → `dag.too_many_outputs` (MVP: one output per task).
 */
const taskSchema = z.strictObject({
  id: z.string().regex(TASK_ID_RE, 'task id must match ^[a-z][a-z0-9-]{0,62}$'),
  kind: z.enum(KINDS),
  prompt: z.string().optional(),
  dependsOn: z.array(dependencySchema).default([]),
  inputs: z.array(
    z.string().regex(TASK_URI_RE, 'input must be a task://<producer>/<name> URI'),
  ).default([]),
  outputs: z.array(z.strictObject({
    name: z.string().min(1),
    schema: objectJsonSchema,
    required: z.boolean().optional(),
  })).default([]),
  // Flat delegation fields (§7.1 agentProfile re-shaping; §2.3 param surface).
  backend: z.string().optional(), // value gated in superRefine (bridge_unsupported)
  model: z.string().min(1).optional(),
  provider: z.string().min(1).optional(),
  persona: z.string().min(1).optional(),
  toolFilter: z.strictObject({
    allow: z.array(z.string()).optional(),
    deny: z.array(z.string()).optional(),
  }).optional(),
  cwd: absPathString('cwd').optional(),
  maxTokens: z.number().int().positive().optional(),
  maxDepth: z.number().int().min(0).optional(),
  delegation: z.boolean().optional(),
  retry: retrySchema.optional(),
  timeoutMs: z.number().int().positive().default(1_800_000),
  priority: z.number().int().min(-100).max(100).default(0),
  failurePolicy: z.enum(['block_downstream', 'isolate']).default('block_downstream'),
  concurrencyKey: z.string().min(1).optional(),
  // M3 fields: validated at shape level now; the engine fails loud at runtime.
  verify: z.strictObject({
    expectOutput: z.string().min(1),
    expectStatus: z.string().min(1),
  }).optional(),
  approval: z.strictObject({
    action: z.string().min(1),
    prompt: z.string().optional(),
  }).optional(),
  worktree: z.strictObject({
    task: z.string().min(1),
    baseRef: z.string().min(1).optional(),
  }).optional(),
  // §7.2 example block for merge tasks (shape only; M3 derives the source).
  merge: z.strictObject({
    integrationBranch: z.string().min(1),
  }).optional(),
}).superRefine((task, ctx) => {
  if (task.backend !== undefined && !BACKEND_VALUES.includes(task.backend)) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['backend'],
      message: `backend '${task.backend}' is not supported; this plugin binds native subagents only (expected one of ${BACKEND_VALUES.join('|')})`,
      params: { dagCode: DAG_SPEC_ERROR_CODES.bridgeUnsupported },
    })
  }
  if (task.outputs.length > 1) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['outputs'],
      message: `task '${task.id}' declares ${task.outputs.length} outputs; at most 1 output per task is supported in the MVP subset`,
      params: { dagCode: DAG_SPEC_ERROR_CODES.tooManyOutputs },
    })
  }
})

/**
 * Root — strictObject per §7.1: `labels` and every other unlisted root key are
 * rejected. `limits` defaults to `{}` parse-through so the normalized value
 * always carries both numbers (4/16).
 */
const specSchema = z.strictObject({
  version: z.literal(1),
  name: z.string().min(1).max(80),
  description: z.string().max(2000).optional(),
  project: z.strictObject({
    root: absPathString('project.root').optional(),
    baseRef: z.string().min(1).optional(),
  }).optional(),
  limits: z.strictObject({
    maxRunningAgents: z.number().int().min(1).max(32).default(4),
    queueCapacity: z.number().int().min(1).max(1024).default(16),
  }).default({}),
  tasks: z.array(taskSchema).min(1).max(256),
})

// ---------------------------------------------------------------------------
// zod issue → {code, path, message} mapping
// ---------------------------------------------------------------------------

/**
 * Render a zod issue path as a dot/bracket string:
 * ['tasks', 3, 'dependsOn', 0, 'taskId'] → 'tasks[3].dependsOn[0].taskId'.
 * Returns undefined for root-level issues (no path segment).
 *
 * @param {ReadonlyArray<string | number>} segments
 * @returns {string | undefined}
 */
function formatPath(segments) {
  if (!segments || segments.length === 0) return undefined
  let out = ''
  for (const segment of segments) {
    if (typeof segment === 'number') out += `[${segment}]`
    else if (out === '') out += segment
    else out += `.${segment}`
  }
  return out
}

/** Append a string key to a (possibly undefined) base path. */
function appendKey(base, key) {
  return base === undefined ? key : `${base}.${key}`
}

/**
 * Map a ZodError into the unified error shape. Three mappings:
 *   1. `unrecognized_keys` → one error PER key (bridge-only keys get
 *      `dag.bridge_unsupported`, everything else `dag.unknown_field`);
 *   2. custom issues tagged with `params.dagCode` → that stable code
 *      (superRefine is the only place that sets it);
 *   3. everything else (invalid_type / invalid_string / too_small / too_big /
 *      invalid_enum_value / invalid_literal / …) → `dag.schema_invalid`.
 *
 * @param {z.ZodError} zerr
 * @returns {SpecError[]}
 */
function mapZodIssues(zerr) {
  /** @type {SpecError[]} */
  const errors = []
  for (const issue of zerr.issues) {
    if (issue.code === 'unrecognized_keys') {
      const base = formatPath(issue.path)
      for (const key of issue.keys ?? []) {
        if (BRIDGE_ONLY_KEYS.includes(key)) {
          errors.push({
            code: DAG_SPEC_ERROR_CODES.bridgeUnsupported,
            path: appendKey(base, key),
            message: `field '${key}' targets the bridge delegation surface and is not supported; this plugin binds native subagents only`,
          })
        } else {
          errors.push({
            code: DAG_SPEC_ERROR_CODES.unknownField,
            path: appendKey(base, key),
            message: `unknown field '${key}'`,
          })
        }
      }
      continue
    }
    const taggedCode = issue.params?.dagCode
    errors.push({
      code: typeof taggedCode === 'string' ? taggedCode : DAG_SPEC_ERROR_CODES.schemaInvalid,
      path: formatPath(issue.path),
      message: issue.message,
    })
  }
  return errors
}

// ---------------------------------------------------------------------------
// Semantic pass (runs on the parsed value, after the shape succeeds)
// ---------------------------------------------------------------------------

/**
 * Transitive control-dependency upstream closure (direct + ancestors via
 * dependsOn; self never included). Ported from source L463-477.
 *
 * @param {string} taskId
 * @param {Map<string, readonly string[]>} adj id → known upstream ids
 * @returns {Set<string>}
 */
function upstreamClosure(taskId, adj) {
  const seen = new Set()
  const stack = [...(adj.get(taskId) ?? [])]
  while (stack.length > 0) {
    const up = stack.pop()
    if (seen.has(up)) continue
    seen.add(up)
    for (const next of adj.get(up) ?? []) {
      if (!seen.has(next)) stack.push(next)
    }
  }
  return seen
}

/**
 * Three-color DFS cycle detection. The adjacency is the DOWNSTREAM map built
 * exactly like lib/critical-path.js builds it (dep.taskId → [consumer ids]);
 * cycle existence is orientation-independent, and the shared construction
 * keeps the two modules agreeing on edge direction.
 *
 * Returns the cycle as an id path (v … u v) when one exists, else null.
 * Ported from source L905-936.
 *
 * @param {readonly string[]} order task ids in spec order (DFS start order)
 * @param {Map<string, readonly string[]>} adj downstream adjacency
 * @returns {string[] | null}
 */
function detectCycle(order, adj) {
  const WHITE = 0
  const GRAY = 1
  const BLACK = 2
  const color = new Map(order.map((id) => [id, WHITE]))
  const stack = []
  let found = null

  const dfs = (u) => {
    if (found) return
    color.set(u, GRAY)
    stack.push(u)
    for (const v of adj.get(u) ?? []) {
      const c = color.get(v)
      if (c === GRAY) {
        // back edge u → v: cycle is v … u v.
        const start = stack.indexOf(v)
        found = [...stack.slice(start), v]
        return
      }
      if (c === WHITE) dfs(v)
      if (found) return
    }
    stack.pop()
    color.set(u, BLACK)
  }

  for (const id of order) {
    if (color.get(id) === WHITE) {
      dfs(id)
      if (found) return found
    }
  }
  return null
}

/**
 * Cross-task semantic checks a structural schema cannot express.
 * Mirrors the source's validateSemantics, narrowed to this plugin's rules.
 *
 * @param {{ tasks: Array<Record<string, unknown>> }} value parsed spec
 * @returns {SpecError[]}
 */
function validateSemantics(value) {
  /** @type {SpecError[]} */
  const errors = []
  const tasks = value.tasks
  const idIndex = new Map() // id → first spec index

  // --- duplicate task id ----------------------------------------------------
  tasks.forEach((task, i) => {
    if (idIndex.has(task.id)) {
      errors.push({
        code: DAG_SPEC_ERROR_CODES.duplicateTaskId,
        path: `tasks[${i}].id`,
        message: `duplicate task id '${task.id}' (first declared at tasks[${idIndex.get(task.id)}])`,
      })
    } else {
      idIndex.set(task.id, i)
    }
  })

  // --- M3 review M-A: worktree.task slug uniqueness (whole spec) ------------
  // DESIGN §11.3 scopes reuse to the SAME task's re-dispatch; a duplicate
  // slug across tasks would hand the second task the first's ACTIVE
  // worktree (isolation break) or lose the create race on the slug-derived
  // branch. Same declaration family as duplicate ids: first declaration
  // wins, every later one gets its own error at its own path.
  const slugIndex = new Map() // worktree.task slug → first spec index
  tasks.forEach((task, i) => {
    const slug = task.worktree?.task
    if (slug === undefined) return
    if (slugIndex.has(slug)) {
      errors.push({
        code: DAG_SPEC_ERROR_CODES.worktreeSlugConflict,
        path: `tasks[${i}].worktree.task`,
        message: `duplicate worktree slug '${slug}' (first declared by task '${tasks[slugIndex.get(slug)].id}' at tasks[${slugIndex.get(slug)}]) — the slug names ONE task's worktree (reuse is same-task retry semantics, DESIGN §11.3); give each worktree task a unique slug`,
      })
    } else {
      slugIndex.set(slug, i)
    }
  })

  // --- dependency existence + self-reference --------------------------------
  for (let ti = 0; ti < tasks.length; ti++) {
    const task = tasks[ti]
    const deps = task.dependsOn ?? []
    for (let di = 0; di < deps.length; di++) {
      const dep = deps[di]
      const path = `tasks[${ti}].dependsOn[${di}].taskId`
      if (dep.taskId === task.id) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.selfDependency,
          path,
          message: `task '${task.id}' depends on itself`,
        })
      } else if (!idIndex.has(dep.taskId)) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.unknownDependency,
          path,
          message: `task '${task.id}' depends on unknown task '${dep.taskId}'`,
        })
      }
    }
  }

  // --- cycle detection (downstream map, critical-path construction) ---------
  const downstreams = new Map()
  for (const task of tasks) {
    for (const dep of task.dependsOn ?? []) {
      if (idIndex.has(dep.taskId) && dep.taskId !== task.id) {
        const arr = downstreams.get(dep.taskId) ?? []
        arr.push(task.id)
        downstreams.set(dep.taskId, arr)
      }
    }
  }
  const cycle = detectCycle(tasks.map((t) => t.id), downstreams)
  if (cycle) {
    errors.push({
      code: DAG_SPEC_ERROR_CODES.cycleDetected,
      path: `tasks[${idIndex.get(cycle[0])}]`,
      message: `dependency cycle detected: ${cycle.join(' → ')}`,
    })
  }

  // --- inputs: producer reachable + same-name output declared ---------------
  const upstreamAdj = new Map()
  for (const task of tasks) {
    upstreamAdj.set(
      task.id,
      (task.dependsOn ?? [])
        .map((d) => d.taskId)
        .filter((id) => idIndex.has(id) && id !== task.id),
    )
  }
  const outputsByTask = new Map(tasks.map((t) => [t.id, new Set(t.outputs.map((o) => o.name))]))

  for (let ti = 0; ti < tasks.length; ti++) {
    const task = tasks[ti]
    const upstream = upstreamClosure(task.id, upstreamAdj)
    for (let ii = 0; ii < task.inputs.length; ii++) {
      const uri = task.inputs[ii]
      const path = `tasks[${ti}].inputs[${ii}]`
      const match = TASK_URI_RE.exec(uri)
      // Shape already guaranteed by zod; guard anyway (never trust re-exec).
      if (!match) continue
      const producer = match[1]
      const name = match[2]
      if (!idIndex.has(producer)) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.inputNotReachable,
          path,
          message: `task '${task.id}' references unknown producer '${producer}' in '${uri}'`,
        })
      } else if (!outputsByTask.get(producer).has(name)) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.outputNotDeclared,
          path,
          message: `producer '${producer}' does not declare an output named '${name}'`,
        })
      } else if (!upstream.has(producer)) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.inputNotReachable,
          path,
          message: `producer '${producer}' is not a direct or transitive upstream of task '${task.id}'`,
        })
      }
    }
  }

  // --- T18: dependency gates (source workflow-spec.ts L541-584) ------------
  // The gate is a FINITE boolean check: the three content-comparison
  // operators need a `value`, the two existence operators forbid one, and
  // the artifact's producer must be a direct/transitive upstream of the
  // declaring task (the inputs rule family — a gate reading a non-upstream
  // output would race the producer instead of ordering after it).
  for (let ti = 0; ti < tasks.length; ti++) {
    const task = tasks[ti]
    const upstream = upstreamClosure(task.id, upstreamAdj)
    for (let di = 0; di < (task.dependsOn ?? []).length; di++) {
      const gate = task.dependsOn[di]?.gate
      if (gate === undefined) continue
      const base = `tasks[${ti}].dependsOn[${di}].gate`
      if (GATE_VALUE_REQUIRED_EXPECTS.has(gate.expect)
        && (gate.value === undefined || gate.value.length === 0)) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.gateValueRequired,
          path: `${base}.value`,
          message: `dependency gate on task '${task.id}' uses expect='${gate.expect}' but provides no 'value'`,
        })
      }
      if (GATE_VALUE_FORBIDDEN_EXPECTS.has(gate.expect) && gate.value !== undefined) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.gateValueForbidden,
          path: `${base}.value`,
          message: `dependency gate on task '${task.id}' uses expect='${gate.expect}' but provides a 'value' (forbidden for existence checks)`,
        })
      }
      const match = TASK_URI_RE.exec(gate.artifact)
      // Shape already guaranteed by zod; guard anyway (never trust re-exec).
      if (!match) continue
      const producer = match[1]
      const name = match[2]
      const upstreamOfEdge = task.dependsOn[di].taskId
      if (!idIndex.has(producer)) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.gateArtifactNotReachable,
          path: `${base}.artifact`,
          message: `dependency gate on task '${task.id}' references unknown producer '${producer}' in '${gate.artifact}'`,
        })
      } else if (!outputsByTask.get(producer).has(name)) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.outputNotDeclared,
          path: `${base}.artifact`,
          message: `gate producer '${producer}' does not declare an output named '${name}'`,
        })
      } else if (producer !== upstreamOfEdge || !upstream.has(producer)) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.gateArtifactNotReachable,
          path: `${base}.artifact`,
          message: `gate artifact producer '${producer}' must be the dependency's own upstream task '${upstreamOfEdge}' of task '${task.id}' (a gate reads the output of the edge it decorates)`,
        })
      }
    }
  }

  // --- kind field matrix (§7.2) ---------------------------------------------
  for (let ti = 0; ti < tasks.length; ti++) {
    const task = tasks[ti]
    const base = `tasks[${ti}]`
    if (task.kind === 'agent') {
      if (task.prompt === undefined || task.prompt.trim().length === 0) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.promptRequired,
          path: `${base}.prompt`,
          message: `agent task '${task.id}' requires a non-empty 'prompt'`,
        })
      }
      // T16: cwd is owned by the worktree when one is declared (the
      // engine-provided path REPLACES task.cwd at dispatch — DESIGN §4.6
      // M3 / §11.3), so a co-declared cwd is a contradiction the runtime
      // would silently ignore. Fail at plan time with the dedicated code.
      if (task.worktree !== undefined && task.cwd !== undefined) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.worktreeCwdConflict,
          path: `${base}.cwd`,
          message: `task '${task.id}' declares both 'worktree' and 'cwd' — a worktree-declaring task's cwd is the engine-provided worktree path; drop the 'cwd' field`,
        })
      }
    } else if (task.kind === 'approval') {
      if (task.approval === undefined) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.kindFieldMismatch,
          path: `${base}.approval`,
          message: `approval task '${task.id}' requires an 'approval' block`,
        })
      }
      // approval default and max retry.maxAttempts is 1 (source L206).
      if (task.retry !== undefined && task.retry.maxAttempts > 1) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.kindFieldMismatch,
          path: `${base}.retry.maxAttempts`,
          message: `approval task '${task.id}' must not set retry.maxAttempts > 1 (got ${task.retry.maxAttempts})`,
        })
      }
      for (const field of APPROVAL_FORBIDDEN_FIELDS) {
        if (task[field] !== undefined) {
          errors.push({
            code: DAG_SPEC_ERROR_CODES.kindFieldMismatch,
            path: `${base}.${field}`,
            message: `approval task '${task.id}' must not set '${field}' (approvals are not subagent delegations)`,
          })
        }
      }
      if (task.outputs.length > 0) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.kindFieldMismatch,
          path: `${base}.outputs`,
          message: `approval task '${task.id}' must not declare outputs`,
        })
      }
    } else if (task.kind === 'merge') {
      if ((task.dependsOn ?? []).length === 0) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.kindFieldMismatch,
          path: `${base}.dependsOn`,
          message: `merge task '${task.id}' requires at least one dependsOn upstream`,
        })
      }
      // T17 source rule (stronger than "≥1 upstream"): the merge
      // executor's source set is exactly the succeeded-condition upstreams
      // that DECLARE a worktree (DESIGN §11.1 input mapping). None →
      // nothing to integrate; reject at plan time (防呆) with the same
      // stable code the runtime uses for the hand-seeded-store case.
      const sourceEdges = (task.dependsOn ?? [])
        .map((dep, di) => ({ dep, di }))
        .filter(({ dep }) => dep.condition === 'succeeded')
      const hasWorktreeSource = sourceEdges.some(({ dep }) => {
        const upstream = tasks.find((t) => t.id === dep.taskId)
        return upstream !== undefined && upstream.worktree !== undefined
      })
      if (sourceEdges.length > 0 && !hasWorktreeSource) {
        errors.push({
          code: DAG_SPEC_ERROR_CODES.mergeSourceMissing,
          path: `${base}.dependsOn`,
          message: `merge task '${task.id}' has no worktree-declaring task among its succeeded-condition upstreams — a merge integrates its upstreams' worktrees (declare worktree: {task} on at least one source task)`,
        })
      }
      if (sourceEdges.length === 0) {
        // Only completed-condition upstreams: nothing can ever satisfy a
        // merge source under the (succeeded + worktree) rule — same code,
        // crisper message.
        errors.push({
          code: DAG_SPEC_ERROR_CODES.mergeSourceMissing,
          path: `${base}.dependsOn`,
          message: `merge task '${task.id}' declares no succeeded-condition dependency — its sources are exactly the succeeded upstreams that declare a worktree`,
        })
      }
    }
  }

  return errors
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/**
 * Validate (and normalize) a WorkflowSpec subset document.
 *
 * NEVER throws on bad spec data — returns `{ok:false, errors}` and lets the
 * caller decide (dag_plan surfaces the list verbatim). Pipeline mirrors the
 * source: version precheck → strict structural parse → cross-task semantics.
 *
 * The returned `value` on success is the NORMALIZED spec: defaults filled
 * (limits 4/16, dependsOn/inputs/outputs [], timeoutMs 1_800_000, priority 0,
 * failurePolicy 'block_downstream', retry inner defaults when the block is
 * present), task order preserved, and fully detached from the input object
 * (deep-cloned — mutating the input afterwards cannot corrupt the value).
 *
 * @param {unknown} spec
 * @returns {SpecValidationResult}
 */
export function validateSpec(spec) {
  // Unsupported version gets its stable code BEFORE the generic literal
  // mismatch would surface as schema_invalid (source L637-655 pattern).
  if (spec !== null && typeof spec === 'object' && !Array.isArray(spec)
    && 'version' in spec && spec.version !== 1) {
    return {
      ok: false,
      errors: [{
        code: DAG_SPEC_ERROR_CODES.unsupportedVersion,
        path: 'version',
        message: `unsupported spec version ${JSON.stringify(spec.version)}; this plugin supports version 1 only`,
      }],
    }
  }

  const parsed = specSchema.safeParse(spec)
  if (!parsed.success) {
    return { ok: false, errors: mapZodIssues(parsed.error) }
  }

  const semantic = validateSemantics(parsed.data)
  if (semantic.length > 0) {
    return { ok: false, errors: semantic }
  }

  // zod builds fresh containers, but passthrough leaves (output JSON schemas)
  // keep input references — deep-clone so the normalized value is immutable
  // with respect to later input mutation.
  return { ok: true, value: structuredClone(parsed.data) }
}

/**
 * Stable content hash of a spec: validate → normalized value → canonical JSON
 * (keys sorted, no whitespace, undefined keys dropped) → sha256 hex.
 * Key order at every level does not affect the hash; content does.
 *
 * Throws LOUD on an invalid spec — hashing an unvalidated spec is a caller
 * programming error, not a data condition.
 *
 * @param {unknown} spec
 * @returns {string} 64-char lowercase hex
 */
export function specHash(spec) {
  const result = validateSpec(spec)
  if (!result.ok) {
    throw new Error(
      `specHash: refusing to hash an invalid spec (${result.errors.map((e) => e.code).join(', ')})`,
    )
  }
  return createHash('sha256').update(canonicalJson(result.value), 'utf8').digest('hex')
}
