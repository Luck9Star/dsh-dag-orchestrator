/**
 * Local copy of the web platform module table (mirrors the dsh-web-ui
 * shared table). The module specifiers the shell seeds into the frozen
 * module table — seeding, bundling externals, and the browser require all
 * consume this one list. Kept as a local copy so this package never
 * imports across checkouts; resync from dsh-web-ui when the shell table
 * changes.
 */
export const PLATFORM_MODULES = [
  'react',
  'react/jsx-runtime',
  'react-dom',
  'react-dom/client',
  '@deepseek-ai/cordis',
  '@deepseek-ai/dsh-client-ui-slots',
  '@deepseek-ai/dsh-client-web-react',
  '@deepseek-ai/dsh-client-ui-primitives',
  '@deepseek-ai/dsh-client-schema-form',
] as const
