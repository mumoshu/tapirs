#!/bin/bash
set -e
if [ -n "$NETWORK_DELAY_MS" ] && [ "$NETWORK_DELAY_MS" != "0" ]; then
  tc qdisc add dev eth0 root netem delay ${NETWORK_DELAY_MS}ms ${NETWORK_JITTER_MS:-0}ms 2>/dev/null || true
fi
exec "$@"
