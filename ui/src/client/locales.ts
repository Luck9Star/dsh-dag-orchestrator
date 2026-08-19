/**
 * dag-view copy: zh-first dictionaries with an English fallback, selected
 * by the document language. Kept dependency-free (no dsh locale service)
 * so the conversation-tab registration (shell-resolved label via
 * ctx.locale / resolveSlotLabel) and the mounted React tree share one
 * tiny lookup. zh is the key-set source of truth; en mirrors it completely.
 *
 * Placeholders are simple `{name}` tokens replaced by `t()` (no formatter
 * library). Known slots: `{t}` (already-formatted time), `{n}` (count),
 * `{name}` (entity name), `{code}` / `{message}` (error envelope).
 */

/** Locale ids used by this surface. */
export type Locale = 'zh' | 'en'

/** zh dictionary (key-set source of truth). */
export const zh = {
  // --- chrome (conversation tab; keep these keys stable) --------------------
  'app.title': 'DAG 编排',
  'tab.label': 'DAG',
  'section.sessionRuns': '本会话的运行',
  'section.allRuns': '全部运行',

  // --- view titles ---------------------------------------------------------
  'view.runs': '运行列表',
  'view.graph': 'DAG 图',
  'view.taskDetail': '任务详情',
  'view.events': '事件流',
  'view.logs': '子代理日志',
  'view.outputs': '任务输出',

  // --- tab labels ----------------------------------------------------------
  'tab.graph': '图',
  'tab.detail': '详情',
  'tab.events': '事件',
  'tab.outputs': '输出',
  'tab.logs': '日志',

  // --- empty states --------------------------------------------------------
  'empty.runs': '还没有运行',
  'empty.graph': '当前运行没有可绘制的任务',
  'empty.taskDetail': '选择一个任务查看详情',
  'empty.events': '暂无事件',
  'empty.outputs': '暂无输出',
  'empty.logs': '暂无日志',
  'empty.noSelection': '尚未选择运行',
  'empty.noAttempts': '暂无尝试',
  'empty.noEvents': '暂无事件',
  'empty.noOutputs': '暂无输出',
  'empty.tab': '本会话还没有 DAG 运行',
  'empty.tabTitle': '还没有任何 DAG 运行',
  'empty.tabBody': 'DAG 运行是把一个多步任务建模为任务图：dag_plan 提交任务图，dag_tick 驱动并行子代理执行，dag_status 核对终局。在本对话里调用 dag_plan，运行的图、任务详情、事件流与日志就会出现在这个页签。',
  'empty.tabHint': '数据也可经宿主路由 /dag-view/* 读取。',

  // --- lifecycle states (task / run / attempt, as UI labels) ---------------
  'state.pending': '等待',
  'state.ready': '就绪',
  'state.running': '运行中',
  'state.succeeded': '成功',
  'state.failed': '失败',
  'state.blocked': '阻塞',
  'state.cancelled': '已取消',
  'state.retry_wait': '重试等待',
  'state.queued': '排队中',
  'state.pausing': '暂停中',
  'state.paused': '已暂停',
  'state.cancelling': '取消中',
  'state.claimed': '已认领',
  'state.orphaned': '已孤立',

  // --- loading / error (generic + per view) --------------------------------
  'loading': '加载中…',
  'loading.runs': '正在加载运行列表…',
  'loading.graph': '正在加载 DAG 图…',
  'loading.taskDetail': '正在加载任务详情…',
  'loading.events': '正在加载事件…',
  'loading.outputs': '正在加载输出…',
  'loading.logs': '正在加载日志…',
  'error.prefix': '错误',
  'error.code': '错误码',
  'error.unavailable': 'dsh-dag-orchestrator 未加载',
  'error.badRequest': '请求格式错误',
  'error.internal': '内部错误',
  'error.transport': '网络请求失败',
  'error.withCode': '错误 {code}：{message}',
  'error.runs': '加载运行列表失败',
  'error.graph': '加载 DAG 图失败',
  'error.taskDetail': '加载任务详情失败',
  'error.events': '加载事件失败',
  'error.outputs': '加载输出失败',
  'error.logs': '加载日志失败',

  // --- buttons -------------------------------------------------------------
  'action.refresh': '刷新',
  'action.back': '返回',
  'action.copy': '复制',
  'action.copied': '已复制',
  'action.viewLog': '查看日志',
  'action.loadOlder': '加载更早',

  // --- graph legend --------------------------------------------------------
  'legend.title': '图例',

  // --- count segments (formatCounts-compatible labels) ---------------------
  'count.ok': '成功',
  'count.failed': '失败',
  'count.blocked': '阻塞',
  'count.running': '运行中',
  'count.pending': '等待',
  'count.cancelled': '已取消',

  // --- logs ----------------------------------------------------------------
  'logs.fallback': '实时日志需要当前会话为该 DAG 的父会话；此处显示已存储的尝试摘要。',
  'logs.loadOlder': '加载更早的消息',

  // --- task / attempt detail labels ----------------------------------------
  'detail.attempts': '尝试记录',
  'detail.attemptsCount': '尝试次数',
  'detail.prompt': '提示词',
  'detail.inputs': '输入',
  'detail.blockedReason': '阻塞原因',
  'detail.retryNotBefore': '重试不早于',
  'detail.lastStopReason': '上次停止原因',
  'detail.kind': '类型',
  'detail.model': '模型',
  'detail.provider': '提供方',
  'detail.ordinal': '序号',
  'detail.state': '状态',
  'detail.failure': '失败信息',
  'detail.backend': '后端',
  'detail.startedAt': '开始时间',
  'detail.childSession': '子会话',
  'detail.stopReason': '停止原因',
  'detail.taskId': '任务 ID',
  'detail.attemptId': '尝试 ID',
  'detail.runId': '运行 ID',
  'detail.name': '名称',
  'detail.description': '描述',

  // --- refresh indicator ---------------------------------------------------
  // `{t}` is a pre-formatted relative time (see format.relativeTime).
  'refresh.updated': '更新于 {t}',
  'refresh.indicator': '已更新',

  // --- task kinds ----------------------------------------------------------
  'kind.agent': '代理',
  'kind.approval': '审批',
  'kind.merge': '汇合',
} satisfies Record<string, string>

/** The dag-view namespace key union. */
export type DagViewKey = keyof typeof zh

/** Full dictionary shape. Mirrors every zh key. */
export type Dict = { readonly [K in DagViewKey]: string }

/** English dictionary, checked complete against the zh key set. */
export const en = {
  'app.title': 'DAG Orchestration',
  'tab.label': 'DAG',
  'section.sessionRuns': 'Runs in this conversation',
  'section.allRuns': 'All runs',

  'view.runs': 'Runs',
  'view.graph': 'DAG Graph',
  'view.taskDetail': 'Task Detail',
  'view.events': 'Events',
  'view.logs': 'Subagent Logs',
  'view.outputs': 'Task Outputs',

  'tab.graph': 'Graph',
  'tab.detail': 'Detail',
  'tab.events': 'Events',
  'tab.outputs': 'Outputs',
  'tab.logs': 'Logs',

  'state.pending': 'Pending',
  'state.ready': 'Ready',
  'state.running': 'Running',
  'state.succeeded': 'Succeeded',
  'state.failed': 'Failed',
  'state.blocked': 'Blocked',
  'state.cancelled': 'Cancelled',
  'state.retry_wait': 'Retry wait',
  'state.queued': 'Queued',
  'state.pausing': 'Pausing',
  'state.paused': 'Paused',
  'state.cancelling': 'Cancelling',
  'state.claimed': 'Claimed',
  'state.orphaned': 'Orphaned',

  'empty.runs': 'No runs yet',
  'empty.graph': 'This run has no tasks to draw',
  'empty.taskDetail': 'Select a task to see its detail',
  'empty.events': 'No events',
  'empty.outputs': 'No outputs',
  'empty.logs': 'No logs',
  'empty.noSelection': 'No run selected',
  'empty.noAttempts': 'No attempts',
  'empty.noEvents': 'No events',
  'empty.noOutputs': 'No outputs',
  'empty.tab': 'No DAG runs in this conversation yet',
  'empty.tabTitle': 'No DAG runs yet',
  'empty.tabBody': 'A DAG run models a multi-step task as a task graph: dag_plan submits the graph, dag_tick drives the parallel subagents, dag_status verifies the end state. Call dag_plan in this conversation and the run\'s graph, task detail, events, and logs appear in this tab.',
  'empty.tabHint': 'The same data is also readable through the /dag-view/* host routes.',

  'loading': 'Loading…',
  'loading.runs': 'Loading runs…',
  'loading.graph': 'Loading DAG graph…',
  'loading.taskDetail': 'Loading task detail…',
  'loading.events': 'Loading events…',
  'loading.outputs': 'Loading outputs…',
  'loading.logs': 'Loading logs…',
  'error.prefix': 'Error',
  'error.code': 'Error code',
  'error.unavailable': 'dsh-dag-orchestrator is not loaded',
  'error.badRequest': 'Malformed request',
  'error.internal': 'Internal error',
  'error.transport': 'Request failed',
  'error.withCode': 'Error {code}: {message}',
  'error.runs': 'Failed to load runs',
  'error.graph': 'Failed to load the DAG graph',
  'error.taskDetail': 'Failed to load task detail',
  'error.events': 'Failed to load events',
  'error.outputs': 'Failed to load outputs',
  'error.logs': 'Failed to load logs',

  'action.refresh': 'Refresh',
  'action.back': 'Back',
  'action.copy': 'Copy',
  'action.copied': 'Copied',
  'action.viewLog': 'View log',
  'action.loadOlder': 'Load older',

  'legend.title': 'Legend',

  'count.ok': 'ok',
  'count.failed': 'failed',
  'count.blocked': 'blocked',
  'count.running': 'running',
  'count.pending': 'pending',
  'count.cancelled': 'cancelled',

  'logs.fallback': 'Live logs require the current session to be the DAG parent session; showing the stored attempt summary instead.',
  'logs.loadOlder': 'Load older messages',

  'detail.attempts': 'Attempts',
  'detail.attemptsCount': 'Attempts count',
  'detail.prompt': 'Prompt',
  'detail.inputs': 'Inputs',
  'detail.blockedReason': 'Blocked reason',
  'detail.retryNotBefore': 'Retry not before',
  'detail.lastStopReason': 'Last stop reason',
  'detail.kind': 'Kind',
  'detail.model': 'Model',
  'detail.provider': 'Provider',
  'detail.ordinal': 'Ordinal',
  'detail.state': 'State',
  'detail.failure': 'Failure',
  'detail.backend': 'Backend',
  'detail.startedAt': 'Started',
  'detail.childSession': 'Child session',
  'detail.stopReason': 'Stop reason',
  'detail.taskId': 'Task ID',
  'detail.attemptId': 'Attempt ID',
  'detail.runId': 'Run ID',
  'detail.name': 'Name',
  'detail.description': 'Description',

  'refresh.updated': 'updated {t}',
  'refresh.indicator': 'Updated',

  'kind.agent': 'Agent',
  'kind.approval': 'Approval',
  'kind.merge': 'Merge',
} satisfies Dict

/** Both locales, keyed for `getDict`. */
export const dict: { zh: Dict; en: Dict } = { zh, en }

/** Return the dictionary for an explicit locale. */
export function getDict(locale: Locale): Dict {
  return dict[locale]
}

/** Active dictionary, picked by the document language at call time. */
export function dictionary(): Dict {
  const lang = typeof document !== 'undefined' ? document.documentElement.lang : 'zh'
  return lang.toLowerCase().startsWith('en') ? dict.en : dict.zh
}

/** Translate a key with optional `{name}` template params. */
export function t(key: DagViewKey, params?: Record<string, string>): string {
  let text: string = dictionary()[key]
  if (params !== undefined) {
    for (const [name, value] of Object.entries(params)) {
      text = text.replaceAll(`{${name}}`, value)
    }
  }
  return text
}
