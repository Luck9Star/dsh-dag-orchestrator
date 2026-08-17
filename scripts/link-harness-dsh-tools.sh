#!/usr/bin/env bash
# link-harness-dsh-tools.sh — symlink the RUNNING harness's @deepseek-ai peer
# packages (@deepseek-ai/{cordis,dsh-tools,dsh-subagent}) into this repo's
# node_modules.
#
# Why: those three are peerDependencies of dsh-dag-orchestrator. npm ≥7
# auto-installs peers as REAL directories (a second physical copy), which
# breaks the harness's single-instance invariants:
#   - dsh-tools registers its tool-runtime scheduler under a module-level
#     Symbol (TOOL_RUNTIME_SCHEDULER), so a second physical copy is a SECOND
#     module instance with a SECOND Symbol — the agent loop then fails every
#     tool call: Cannot read properties of undefined (reading 'prepare').
#   - dsh-subagent / cordis carry the same class of module-identity state
#     (session registries, Cordis plugin contexts); this plugin binds the
#     execution layer through ctx.subagents, so it must see the SAME dsh-subagent
#     instance the harness runs.
# Linking every peer to the copy the harness itself runs keeps a single
# instance of each.
#
# Re-run after a dsh upgrade (new npx cache dir) or an `npm install` here
# (npm recreates the peer copies as real directories every time).
#
# Note: dsh-dag-orchestrator carries NO patches/ directory (zero host patches),
# so the resolve_live_root logic from the dsh-plugin-subagents family's
# patches/resolve-root.sh is INLINED below (red line 11 stays intact: no
# hardcoded cache-hash paths, no `ls ~/.npm/_npx/* | tail -1` pick-a-root
# heuristics). resolve_live_root walks `command -v dsh` -> realpath -> upward
# to the node_modules parent (or honors DSH_HARNESS_ROOT), and the peer
# packages are then read from that root. Use this script (or the explicit
# HARNESS_PEERS_ROOT / HARNESS_DSH_TOOLS overrides) to keep peers in sync
# after a harness move.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# --- inline resolve_live_root (this plugin has no patches/ dir) --------------
# Resolves the live dsh harness root: the parent of the node_modules directory
# that holds the `dsh` binary currently on PATH. Prints the root on stdout;
# returns 1 (loud) on any failure. Never picks a root heuristically — only the
# running `dsh` (or the explicit override) defines "live".
resolve_live_root() {
  # 1. An explicit DSH_HARNESS_ROOT beats PATH resolution.
  if [ -n "${DSH_HARNESS_ROOT:-}" ]; then
    _rr_root="${DSH_HARNESS_ROOT}"
  else
    # 2. `command -v dsh` -> realpath -> walk up to the node_modules parent.
    _rr_bin="$(command -v dsh 2>/dev/null || true)"
    if [ -z "${_rr_bin}" ]; then
      printf 'resolve-root: ERROR: dsh not found on PATH — is the harness installed?\n' >&2
      printf 'resolve-root: the live harness root must be resolvable one of these ways:\n' >&2
      printf 'resolve-root:   1. dsh launched from an npx cache install — `command -v dsh` then\n' >&2
      printf 'resolve-root:      realpath + upward walk to the enclosing node_modules parent;\n' >&2
      printf 'resolve-root:   2. dsh installed globally (npm install -g) — same resolution;\n' >&2
      printf 'resolve-root:   3. exotic launch forms — export DSH_HARNESS_ROOT=/path/to/root explicitly.\n' >&2
      return 1
    fi
    if ! _rr_real="$(rr_realpath "${_rr_bin}")"; then
      printf 'resolve-root: ERROR: realpath failed for %s\n' "${_rr_bin}" >&2
      return 1
    fi
    _rr_dir="$(dirname "${_rr_real}")"
    _rr_root=""
    while [ -n "${_rr_dir}" ] && [ "${_rr_dir}" != "/" ]; do
      if [ "$(basename "${_rr_dir}")" = "node_modules" ]; then
        _rr_root="$(dirname "${_rr_dir}")"
        break
      fi
      _rr_dir="$(dirname "${_rr_dir}")"
    done
    if [ -z "${_rr_root}" ]; then
      printf 'resolve-root: ERROR: %s is not under any node_modules — this is not a live dsh harness root\n' "${_rr_real}" >&2
      return 1
    fi
  fi
  # 3. Self-verification: the root must actually carry the harness's
  #    dsh-subagent package, or we resolved something that is not a live root.
  if [ ! -d "${_rr_root}/node_modules/@deepseek-ai/dsh-subagent" ]; then
    printf 'resolve-root: ERROR: %s/node_modules/@deepseek-ai/dsh-subagent does not exist — this is not a live dsh harness root\n' "${_rr_root}" >&2
    return 1
  fi
  printf '%s\n' "${_rr_root}"
}

# Canonicalize an existing file or directory. realpath first (macOS 12+/Linux),
# node fallback (node is a hard prerequisite of everything else here anyway).
rr_realpath() {
  if command -v realpath >/dev/null 2>&1; then
    realpath "$1" 2>/dev/null || return 1
  else
    node -e 'const fs=require("fs");process.stdout.write(fs.realpathSync(process.argv[1]))' "$1" 2>/dev/null || return 1
  fi
}

# Allow an explicit override: HARNESS_PEERS_ROOT=/path/to/harness-root ./scripts/link-harness-dsh-tools.sh
# (HARNESS_DSH_TOOLS=/path/to/dsh-tools still works and links ONLY dsh-tools.)
if [ -n "${HARNESS_DSH_TOOLS:-}" ]; then
  HARNESS_PEERS_ROOT=''
elif [ -n "${HARNESS_PEERS_ROOT:-}" ]; then
  :
else
  LIVE_ROOT="$(resolve_live_root)" || {
    echo "ERROR: cannot resolve the live dsh harness root (see resolve-root diagnostics above)" >&2
    echo "Hint: HARNESS_PEERS_ROOT=/path/to/harness-root $0" >&2
    echo "      or HARNESS_DSH_TOOLS=/path/to/dsh-tools $0   (legacy: links only dsh-tools)" >&2
    exit 1
  }
  HARNESS_PEERS_ROOT="${LIVE_ROOT}"
fi

mkdir -p "${REPO_DIR}/node_modules/@deepseek-ai"

CHANGED=0
link_peer() {
  _name="$1"
  _expected=''
  if [ "${_name}" = "dsh-tools" ] && [ -n "${HARNESS_DSH_TOOLS:-}" ]; then
    _expected="${HARNESS_DSH_TOOLS}"
  elif [ -n "${HARNESS_PEERS_ROOT:-}" ]; then
    _expected="${HARNESS_PEERS_ROOT}/node_modules/@deepseek-ai/${_name}"
  else
    # HARNESS_DSH_TOOLS-only mode: nothing to link for non-dsh-tools peers.
    echo "SKIP: @deepseek-ai/${_name} (HARNESS_DSH_TOOLS override set — links only dsh-tools)"
    return 0
  fi
  _target="${REPO_DIR}/node_modules/@deepseek-ai/${_name}"
  if [ ! -d "${_expected}" ]; then
    echo "ERROR: ${_expected} is not a directory" >&2
    echo "Hint: point HARNESS_PEERS_ROOT at the live harness root, e.g. HARNESS_PEERS_ROOT=\$(dirname \$(dirname \$(command -v dsh))) $0" >&2
    exit 1
  fi
  if [ -L "${_target}" ] && [ "$(readlink "${_target}")" = "${_expected}" ]; then
    echo "OK: ${_target} -> ${_expected} (already correct)"
    return 0
  fi
  rm -rf "${_target}"
  ln -s "${_expected}" "${_target}"
  CHANGED=$((CHANGED + 1))
  echo "LINKED: ${_target} -> ${_expected}"
}

link_peer dsh-tools
if [ -z "${HARNESS_DSH_TOOLS:-}" ]; then
  link_peer cordis
  link_peer dsh-subagent
  echo "Done. ${CHANGED} link(s) created/updated; peers now resolve to the live harness root."
else
  echo "Done. ${CHANGED} link(s) created/updated; dsh-tools now resolves to the live harness root (HARNESS_DSH_TOOLS legacy mode — cordis/dsh-subagent untouched)."
fi
