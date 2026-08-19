/**
 * tsdown wiring for dsh-dag-view: the node-half lib build (src/index.ts,
 * cordis external) plus the browser-half closure-factory bundle
 * (src/client/index.ts) through the LOCAL preset copy in build/ — this
 * package lives outside the dsh-web-ui workspace, so it never imports the
 * shared preset across checkouts (the copy-in-package pattern the
 * git-graph package documents for its own build/ copies).
 */
import { clientBundle } from './build/tsdown.client.ts'

export default clientBundle('dsh-dag-view', ['src/index.ts'])
