#!/usr/bin/env bash
# Parse Jepsen/Maelstrom result output and print a concise summary.
# Expects raw Jepsen output (captured via tee in the Makefile target).
# Usage: maelstrom-summary.sh <output-file>
#
# Canonical format of Jepsen result output (EDN):
#
#   INFO [...] jepsen.core {:perf {...},
#    :timeline {:valid? true},
#    :exceptions {:valid? true},
#    :stats {:valid? false,
#            :count 910,
#            :ok-count 1,
#            :fail-count 222,
#            :info-count 687,
#            :by-f {:cas {:valid? false,
#                         :count 459,
#                         :ok-count 0,
#                         :fail-count 0,
#                         :info-count 459},
#                   :read {:valid? false,
#                          :count 222,
#                          :ok-count 0,
#                          :fail-count 222,
#                          :info-count 0},
#                   :write {:valid? true,
#                           :count 229,
#                           :ok-count 1,
#                           :fail-count 0,
#                           :info-count 228}}},
#    :availability {:valid? true, :ok-fraction 0.0010989011},
#    ...
#    :valid? true}
#
#   Everything looks good! ヽ('ー`)ノ        (on success)
#   Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻       (on failure)

set -euo pipefail

file="${1:?Usage: maelstrom-summary.sh <output-file>}"
[[ -f "$file" ]] || { echo "File not found: $file" >&2; exit 0; }

echo "=== Maelstrom Summary ==="

# Jepsen prints one of these verdict lines after the EDN results block.
if grep -q "Everything looks good" "$file"; then
  echo "result:  PASS"
elif grep -q "Analysis invalid" "$file"; then
  echo "result:  FAIL"
else
  echo "result:  UNKNOWN"
fi

# Parse stats from Jepsen EDN output using an awk state machine.
#
# State variables:
#   in_stats  — 1 when inside the :stats {...} block
#   in_byf    — 1 when inside the :by-f {...} sub-block
#   section   — which counter group we're accumulating:
#               "top" (before :by-f), "cas", "read", or "write"
awk '
{
  # Normalize: remove commas/braces/parens so ":ok-count 1," becomes
  # ":ok-count 1" and awk field splitting ($i, $(i+1)) works cleanly.
  gsub(/[,{}()]/, " ")
}

# ── :stats {:valid? false, ──
# Opens the stats block. :valid? is on the same line as :stats.
# After gsub: " :stats  :valid? false "
/:stats / {
  in_stats = 1; in_byf = 0; section = "top"
  if ($0 ~ /:valid\?/) {
    for (i = 1; i <= NF; i++)
      if ($i == ":valid?") { stats_valid = $(i+1); break }
  }
}

# ── :by-f {:cas {:valid? false, ──
# Opens the per-operation-type sub-block. :by-f and the first type
# keyword (:cas) may appear on the same line. Awk fires rules
# top-to-bottom per line, so in_byf=1 is set before the :cas rule.
in_stats && /:by-f/ { in_byf = 1 }

# ── :cas {:valid? false, ──  (may be on same line as :by-f)
# ── :read {:valid? false, ──
# ── :write {:valid? true, ──
# Each type keyword switches section so subsequent counter lines
# are stored under the correct key.
in_stats && in_byf && /:cas/   { section = "cas" }
in_stats && in_byf && /:read/  { section = "read" }
in_stats && in_byf && /:write/ { section = "write" }

# ── :ok-count 1, ──
# Appears once per section (top, cas, read, write).
# ":ok-count" does NOT match /:stats/ or /:by-f/ patterns.
# ":count" (without prefix) is intentionally skipped — we compute
# total = ok + fail + info in the END block instead.
in_stats && section != "" && /:ok-count/ {
  for (i = 1; i <= NF; i++)
    if ($i == ":ok-count") { ok[section] = $(i+1) + 0; break }
}

# ── :fail-count 222, ──
in_stats && section != "" && /:fail-count/ {
  for (i = 1; i <= NF; i++)
    if ($i == ":fail-count") { fail[section] = $(i+1) + 0; break }
}

# ── :info-count 687, ──
in_stats && section != "" && /:info-count/ {
  for (i = 1; i <= NF; i++)
    if ($i == ":info-count") { info[section] = $(i+1) + 0; break }
}

# ── :availability {:valid? true, :ok-fraction 0.0010989011}, ──
# Always comes after the :stats block. Extracts the top-level
# ok-fraction and ends stats parsing (in_stats=0) so later
# :valid? occurrences in the :workload section are ignored.
/:ok-fraction/ {
  for (i = 1; i <= NF; i++)
    if ($i == ":ok-fraction") { ok_fraction = $(i+1) + 0; break }
  in_stats = 0
}

END {
  printf "stats:   valid=%s\n", stats_valid

  # Top-level summary (total = ok + fail + info, matches Jepsen :count)
  top_total = ok["top"] + fail["top"] + info["top"]
  printf "total: %d, ok: %d, fail: %d, info: %d, ok-fraction: %.1f%%\n", \
    top_total, ok["top"], fail["top"], info["top"], ok_fraction * 100

  # Per-operation-type breakdown with computed ok-fraction
  types_n = split("read cas write", types)
  for (i = 1; i <= types_n; i++) {
    t = types[i]
    total = ok[t] + fail[t] + info[t]
    frac = (total > 0) ? (ok[t] / total) * 100 : 0
    printf "  %-6s total: %d, ok: %d, fail: %d, info: %d, ok-fraction: %.1f%%\n", \
      t ":", total, ok[t], fail[t], info[t], frac
  }
}
' "$file"
