# dsh-dag-view

Read-only Web GUI surface for [dsh-dag-orchestrator](../README.md) runs,
living in this repo's `ui/` directory. One package, two faces on the
official `dsh.client` web platform:

- **Host half** (`exports "."`, `lib/index.js`) — a plain cordis plugin
  that registers `POST /dag-view/*` JSON-envelope routes on the shared
  webserver. Every fact comes from the orchestrator's read-only
  `dagOrchestrator` service face (`ctx.get`, resolved lazily per
  request); the host half owns no state and never touches the database.
- **Browser half** (`exports "./client"`, `lib/client.js`) — a
  closure-factory bundle (`window.__ModuleLoader__.load({ id, factory })`)
  that registers the official conversation tab and renders the React
  views: this conversation's runs, all runs, layered DAG graph with state
  colors and per-kind shapes, task detail, event stream, per-attempt
  subagent logs (live history with a stored summary fallback), and
  validated outputs.

Control operations stay on the `dag_*` conversation tools — this surface
is view-only by design. Updates are polling only: the run lists refresh
every 10 s, an open run every 2 s, and both pause while the tab is hidden.

## Entry: the conversation tab

The tab is a first-class shell citizen, not an injected DOM island:

- The web shell exposes the `conversation.view` slot ring — one list
  entry per conversation-header tab (the shell's own Chat view registers
  there at `id: 'chat', order: 0`).
- The browser half registers
  `ctx.slots.register({ name: 'conversation.view', id: 'dag', order: 10, label: () => 'DAG', locale: NS, inject: () => ({ dag: context }) }, DagTabView)`.
  The label resolves through the shell's locale machinery
  (`resolveSlotLabel`); the dictionaries behind the namespace are
  registered with `ctx.locale.register(NS, { zh, en })`.
- `DagTabView` receives the framework session kit (`sessionId` — the
  slot is session-scoped) plus the injected mount context (a
  session-id getter over `ctx.sessions.list`, the envelope api, the
  locale, and the live log seam built from `ctx.connection` when
  present).
- Tab body layout: this conversation's runs first (compact cards), a
  divider, then all runs; selecting a run embeds the full `DagViewApp`
  tree (graph + detail + events/outputs/logs) inline. No runs at all —
  a centered guidance card.
- Service prerequisites ride package.json's `dsh.client.inject` —
  `@deepseek-ai/dsh-client-runtime` (slots/sessions services),
  `@deepseek-ai/dsh-client-locale` (the locale registry the dictionaries
  register into), and `@deepseek-ai/dsh-client-ui-conversation` (declares
  the `conversation.view` slot the tab registers into). The list is
  informational boot-graph metadata, never a load-order guarantee, so the
  registration additionally runs through `ctx.slots.inject(
  'conversation.view', ...)` — the declaration-wait guard that waits for
  (and re-follows) the slot's declaration instead of crashing on a
  boot-order race.

## File map

| Path | What lives there |
| --- | --- |
| `src/index.ts` | Host-half entry: `apply()` registers the routes (`inject: ['webServer']`). |
| `src/host/routes.ts` | The `POST /dag-view/*` layer: envelope, POST + JSON content-type enforcement, 1 MiB body cap, per-endpoint dispatch. |
| `src/host/face.ts` | Lazy `dagOrchestrator` accessor plus the structural TypeScript mirror of the orchestrator's `lib/dag-face.js`. |
| `src/core/types.ts` | Browser/host-safe type mirrors of the orchestrator's projections (type-only knowledge, no runtime import). |
| `src/core/graph.ts` | Pure layered layout: longest-path layering, deterministic ordering, orthogonal edges, cycle guard. |
| `src/core/format.ts` | Locale-aware display helpers: state tone classes, count summaries, relative time. |
| `src/client/index.ts` | Browser-half entry: locale dictionaries + the `conversation.view` tab registration. |
| `src/client/api.ts` | Typed fetch client for the five routes; never throws — every failure is an envelope arm. |
| `src/client/views/DagTabView.tsx` | The conversation-tab body: session runs, all runs, run-view embed, empty-state card. |
| `src/client/locales.ts` | zh/en dictionaries; zh is the key-set source of truth, en mirrors it completely. |
| `src/client/styles.module.css` | CSS Modules; colors ride `var(--dsw-*)` / `var(--dsh-*)` tokens only. |
| `src/client/views/` | React views: `DagViewApp` shell, `RunsListView`, `DagGraphView`, `TaskDetailView`, `EventsView`, `OutputsView`, `AttemptLogsView`. |
| `build/tsdown.client.ts`, `build/web-platform.ts` | Local copies of the dsh-web-ui client-bundle preset and the platform module table (the copy-in-package pattern; no cross-checkout imports). |
| `tests/host/`, `tests/client/` | vitest: route tests in node; api / format / graph / tabview / views suites in jsdom — 58 cases. |
| `cordis.patch.yml` | The bundle-patch insert row (`id: dag-view`). |

## HTTP endpoints

All POST with `application/json` — cross-site forms cannot send a JSON
content-type without a CORS preflight, which is the CSRF hardening for
this read-only surface. Bodies over 1 MiB are destroyed, never drained.
Every response carries the shared envelope:

```
{ "ok": true, "value": ... }  |  { "ok": false, "error": { "code": "...", "message": "..." } }
```

| Endpoint | Request body | Response `value` |
| --- | --- | --- |
| `POST /dag-view/runs` | `{}` | `{ runs }` — run rows: `run_id`, `name`, `state`, `counts` (per-task-state tallies), `created_at`, `updated_at`. |
| `POST /dag-view/session-runs` | `{ session_id }` | `{ runs }` — the same row shape, filtered to runs planned by that GUI conversation (`runs.planner_session`). An unknown session returns `{ runs: [] }`. |
| `POST /dag-view/run` | `{ run_id }` | `{ run, spec, tasks, attempts, outputs }` — one aggregate snapshot per round trip: the base run row, the parsed spec (graph edges = `spec.tasks[].dependsOn`), task rows, attempt rows, validated outputs. |
| `POST /dag-view/events` | `{ run_id, after_seq?, task_id?, limit? }` | `{ events }` — the event tail window (`seq`, `type`, `at`, optional `task_id`/`attempt_id`, `payload`); `after_seq` drives incremental polling, `task_id` filters to one task. |
| `POST /dag-view/attempt-logs` | `{ run_id }` | `{ items }` — attempt summaries: `attempt_id`, `task_id`, `ordinal`, `state`, `backend`, `started_at`, plus `child_session`, `stop_reason`, and `summary` (parsed `result_json`) when present. |

Error codes on the `ok:false` arm:

| Code | Meaning |
| --- | --- |
| `dag_view.unavailable` | The core plugin is absent or its face is not usable — the UI degrades, never throws. |
| `dag_view.bad_request` | Malformed JSON body, a missing/empty `run_id`, or a missing/empty `session_id`. |
| `dag_view.internal` | A face call threw; the raw host detail stays in the host log. |
| `dag.run_not_found` | Passed through from the orchestrator's stable error codes. |

Non-POST requests get 405, a wrong content-type 415, and unknown paths
404, all without a body. On the browser side, transport and decode
failures surface as `dag_view.transport` / `network`.

## Graph layout

`src/core/graph.ts` is a pure function from spec to `{ nodes, edges }` —
no DOM:

- Edges come from `spec.tasks[].dependsOn`; unknown targets are ignored
  and duplicate edges deduped.
- Layering is longest-path: a node sits at `max(predecessor layers) + 1`,
  with sources at layer 0.
- Ordering is deterministic — ids sort lexicographically, layers
  numerically — so the same spec always lays out identically.
- Nodes are 180x44 px (taller when the id is long), stacked vertically
  inside their layer; the layer index sets the x column.
- Edges are orthogonal four-point polylines: source bottom, down to the
  midpoint, across, into the target top.
- A DFS cycle guard throws before layout (specs are validated acyclic
  upstream; the guard is defense in depth).

## Client tab pattern

- The entry is the official `conversation.view` slot (see above): the
  shell owns the tab chrome, activation, and the mount point — the
  plugin owns only the component and its injected context.
- Theming rides CSS tokens only — every color goes through
  `var(--dsw-*, var(--dsh-*, fallback))` — so every shipped skin works;
  CSS Modules compile into the bundle as idempotent
  `<style data-plugin>` tags.
- Polling runs only while the tab is mounted (the shell mounts the
  active view) and the document visible: the run lists every 10 s, an
  open run every 2 s.
- Copy is zh-first with a complete English mirror, selected by the
  document language (`src/client/locales.ts`); the tab label resolves
  through the shell's locale registry.

## Development

```sh
cd ui
npm install
npm run build       # tsc -p tsconfig.build.json && tsdown → lib/index.js + lib/client.js
npm run typecheck   # tsc --noEmit
npm test            # vitest: host routes in node, client suites in jsdom — 58 cases
```

The browser bundle is a closure factory
(`window.__ModuleLoader__.load({ id, factory: (require) => ... })`).
Value imports are restricted to the platform module table — `react`,
`react-dom`, `@deepseek-ai/cordis`, and the `dsh-client-*` entries in
`build/web-platform.ts`; every other `@deepseek-ai/*` import must be
type-only. Cross-plugin collaboration goes through cordis services.

## Install into a profile

Requires `dsh-dag-orchestrator` active in the same profile — the panel
reads through its face; without it every route answers
`dag_view.unavailable` and the views show a not-loaded notice.

```sh
cd <repo>/ui && npm install && npm run build
ln -s <repo>/ui ~/.dsh/profiles/web/node_modules/dsh-dag-view
```

Then append the insert row from `cordis.patch.yml` to the profile's
`cordis.patch.yml`:

```yaml
- insert:
    - id: dag-view
      name: dsh-dag-view
```

Restart `dsh web`. One-command alternative, which applies the bundle
patch through the package's `dsh.bundle.patch` manifest field:
`dsh plugin --profile web add link:<repo>/ui`.

## Known limitations (v1)

- Read-only: tick, approve, pause, retry, and every other control
  operation stays on the `dag_*` tools.
- Polling, not push — no SSE or websocket.
- Live subagent logs render only when the DAG's parent session is
  resolvable through the current GUI session's subagent catalog
  (`subagents.history`); otherwise the stored attempt summary is shown.
- Session-linked runs appear only for runs created after this change:
  the planner_session column is new, legacy rows read as NULL and only
  surface under "All runs".

## 中文说明

`dsh-dag-view` 是 dsh-dag-orchestrator 的只读 Web 界面（本仓库 `ui/` 子包）。
双面插件：宿主半在共享 webserver 上提供 `POST /dag-view/*` JSON envelope 路由
（每次请求经核心插件的 `dagOrchestrator` 服务面惰性读取）；浏览器半经官方
`conversation.view` slot 注册对话头部的 DAG 页签（与"Chat"并列，选中时渲染在
对话主区）—— 页签先展示本会话的运行（`runs.planner_session` 关联），分隔后是
全部运行；点选运行在页签内渲染完整视图 —— 分层 DAG 图、任务详情、事件流、
子代理日志（实时 history，解析不到时回退存储摘要）、任务输出。空状态显示引导
卡片。只用轮询（列表 10 秒 / 打开的运行 2 秒，页面不可见时暂停）；控制操作留在
`dag_*` 工具上，界面只读是设计决定。开发：`cd ui && npm install && npm run
build && npm test`（vitest，58 个用例）。安装进 profile：构建后把 `ui/` 符号
链接进 profile 的 node_modules，追加 `cordis.patch.yml` 的 insert 行，重启
`dsh web`；核心插件缺席时路由返回 `dag_view.unavailable`，界面优雅降级。
