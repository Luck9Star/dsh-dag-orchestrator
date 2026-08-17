// dag_approve — the approval decision entry (DESIGN §8.5, TASKS.md T12).
//
// THIN and STATELESS like its siblings: the tool validates the pre-state
// (task exists + blocked(approval_pending) + a pending approval row for
// it), runs ONE store transaction — decideApproval + `approval.decided`
// event — and returns the §8.5 summary. It NEVER touches the task
// projection (red line 2: tools never write state; the next dag_tick's
// reconcileApprovals promotes blocked→succeeded / blocked→failed
// policy_denied). Idempotence is loud: deciding an already-decided
// approval → `dag.already_decided`; approving a task that is not parked
// → `dag.invalid_task_state`.
//
// Error contract mirrors the sibling tools: `dag_approve: <code> —
// <detail>` with a stable `dag.*` code on the thrown Error.

import { defineTool } from '@deepseek-ai/dsh-tools'

/** Parse a blocked_reason JSON blob's code (engine.js's helper, mirrored). */
function blockedReasonCode(reason) {
  if (typeof reason !== 'string') return undefined
  try {
    return JSON.parse(reason).code
  } catch {
    return undefined
  }
}

/** Tool-layer loud error with a stable code. */
function fail(code, detail) {
  const error = new Error(`dag_approve: ${code} — ${detail}`)
  error.code = code
  throw error
}

/**
 * Register the dag_approve tool. deps = { engine, store, config }. Returns
 * undefined.
 *
 * @param {{tools: {register: Function}}} ctx plugin context (fake in tests)
 * @param {{engine?: object, store: object, config?: object}} deps
 */
export function registerDagApprove(ctx, deps) {
  const { store } = deps
  if (!store || typeof store.findRun !== 'function' || typeof store.findTasks !== 'function'
    || typeof store.findApprovalsByTask !== 'function' || typeof store.decideApproval !== 'function') {
    throw new Error('dag_approve: requires deps.store with findRun/findTasks/findApprovalsByTask/decideApproval')
  }

  ctx.tools.register(defineTool({
    name: 'dag_approve',
    description: 'Decide a pending DAG approval gate: the task must be parked blocked(approval_pending) with a pending approval (created when the engine reached the approval node). '
      + 'This records the decision ONLY — it never changes the task; the next dag_tick promotes it (approve → task succeeded and downstream dispatches; reject → task failed dag.policy_denied and downstream blocks). '
      + 'Typical sequence (approval gate): dag_tick → waiting_on: approval; relay approval_prompt to the user; dag_approve(decision:"approve"|"reject", note: user rationale); dag_tick to promote.',
    parameters: {
      run_id: {
        type: 'string',
        required: true,
        description: 'The run holding the approval gate.',
      },
      task_id: {
        type: 'string',
        required: true,
        description: 'The approval-kind task to decide.',
      },
      decision: {
        type: 'string',
        required: true,
        enum: ['approve', 'reject'],
        description: 'approve = the gate opens (task succeeds on the next tick); reject = permanent policy denial.',
      },
      note: {
        type: 'string',
        description: 'Recorded with the decision (recommended: the user rationale).',
      },
    },
    output: {
      // §8.5 shape. The stored state value is 'approved'/'rejected'; the
      // tool face echoes the caller's decision wording verbatim.
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 2) }],
    },
    isConcurrencySafe: () => false,
    async execute(args) {
      if (store.findRun(args.run_id) === null) {
        fail('dag.run_not_found', args.run_id)
      }

      // Task pre-state: must be parked blocked(approval_pending).
      const task = store.findTasks(args.run_id).find((t) => t.task_id === args.task_id)
      if (task === undefined) {
        fail('dag.task_not_found', `task ${JSON.stringify(args.task_id)} not found in run ${JSON.stringify(args.run_id)}`)
      }
      const code = blockedReasonCode(task.blocked_reason)
      if (task.state !== 'blocked' || code !== 'approval_pending') {
        fail(
          'dag.invalid_task_state',
          `task ${JSON.stringify(args.task_id)} is ${task.state}${code !== undefined ? `(${code})` : ''} — dag_approve accepts only a parked approval gate (blocked(approval_pending))`,
        )
      }

      // The pending approval row for this task. (Task-id scoping keeps the
      // pre-state and the decision on the same object even if a run
      // legitimately carries several gates.)
      const rows = store.findApprovalsByTask(args.run_id, args.task_id)
      const pending = rows.filter((row) => row.state === 'pending')
      if (pending.length === 0) {
        // Parked but no pending row: either an already-DECIDED approval
        // (decided, promotion pending the next tick — §8.5's idempotence
        // contract says already_decided, NOT invalid_task_state) or a
        // hand-seeded park with no approval row at all.
        const decided = rows.find((row) => row.state === 'approved' || row.state === 'rejected')
        if (decided !== undefined) {
          fail('dag.already_decided', `approval ${decided.approval_id} of task ${JSON.stringify(args.task_id)} is already ${decided.state} — call dag_tick to promote it`)
        }
        fail(
          'dag.invalid_task_state',
          `task ${JSON.stringify(args.task_id)} is parked approval_pending but has no approval row (no gate to decide)`,
        )
      }
      const approval = pending[0]

      const storedDecision = args.decision === 'approve' ? 'approved' : 'rejected'
      // ONE tx: the decision CAS + its event (invariant #6). decideApproval
      // losing the pending CAS → the caller-facing already_decided (the
      // throw rolls the tx back; a lost CAS wrote nothing anyway).
      store.tx(() => {
        const verdict = store.decideApproval(approval.approval_id, storedDecision, args.note)
        if (!verdict.ok) {
          fail('dag.already_decided', `approval ${approval.approval_id} of task ${JSON.stringify(args.task_id)} is already ${verdict.approval?.state ?? 'decided'} — call dag_tick to promote it`)
        }
        store.insertEvent(args.run_id, {
          type: 'approval.decided',
          taskId: args.task_id,
          payload: {
            approvalId: approval.approval_id,
            decision: storedDecision,
            note: args.note ?? null,
            action: approval.action,
          },
        })
      })

      // §8.5 summary — task_state stays 'blocked' (the promotion belongs to
      // the next tick); approval_prompt echoes the gate's declared prompt
      // for the conversation. The SPEC is the prompt's source of truth
      // (the approvals DDL has no prompt column; the row's note belongs to
      // the decision rationale, which COALESCE may have just overwritten).
      let prompt = null
      try {
        const spec = JSON.parse(store.findRun(args.run_id).spec_json)
        const specTask = (spec.tasks ?? []).find((t) => t.id === args.task_id)
        if (typeof specTask?.approval?.prompt === 'string') prompt = specTask.approval.prompt
      } catch {
        prompt = null
      }
      const result = {
        kind: 'approve',
        run_id: args.run_id,
        task_id: args.task_id,
        decision: args.decision,
        task_state: task.state,
      }
      if (prompt !== null) result.approval_prompt = prompt
      result.next_hint = 'call dag_tick to promote the decided approval'
      return result
    },
  }))
}
