#!/usr/bin/env bash
# Static checks for non-determinism sources that clippy cannot catch.
#
# Currently checks:
# - thread_rng() usage in library code (binaries excluded)
#
# Not checked (handled elsewhere):
# - HashMap/HashSet iteration: clippy::iter_over_hash_type (Makefile)
# - select! without biased: not an issue with Builder::rng_seed()
set -euo pipefail

errors=0

# Check for thread_rng() in library source (excluding binaries, doc comments, and regular comments)
if grep -rn 'thread_rng()' src/ --include='*.rs' --exclude-dir='bin' 2>/dev/null \
    | grep -v '^\([^:]*:[^:]*:\s*///\|[^:]*:[^:]*:\s*//\)' ; then
    echo "ERROR: thread_rng() found in library code. Use seeded StdRng instead."
    errors=1
fi

if [ "$errors" -ne 0 ]; then
    exit 1
fi
