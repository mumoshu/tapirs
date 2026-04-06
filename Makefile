.PHONY: test lint check-no-cfg-test-helpers lock_server_stress_test coordinator_failure_stress_test_3 coordinator_failure_stress_test_7 bench bench/ro bench/rw bench/mix bench/compare fuzz fuzz100 maelstrom maelstrom-run maelstrom-sync-ro-txn-get maelstrom-skewed-rw-txn-get-commit maelstrom-skewed-ro-txn-get-fail maelstrom-sync-ro-fast-path maelstrom-sync-ro-fast-path-may-fail maelstrom-skew-ro-slow-path-truetime ci ci-full ci/operator-lint ci/operator-test ci/bench-solo ci/bench-compare ci/testbed-kube-operator ci/testbed-kube-operator-tls ci/testbed-kube ci/testbed-docker-compose ci/testbed-solo ci/testbed ci/fuzz-diagnose ci/fuzz-multi-seed ci/test-s3

lint: check-no-cfg-test-helpers
	cargo clippy --workspace --all-targets -- -D warnings -D clippy::iter_over_hash_type && ./scripts/check-determinism.sh

check-no-cfg-test-helpers:
	./scripts/check-no-cfg-test-helper-fns.py

test: lint
	cargo test --workspace --release --no-run && timeout -k 10s 120s cargo test --workspace --release

lock_server_stress_test:
	timeout -k 10s 600s cargo test --release -- lock_server_loop --nocapture --include-ignored

coordinator_failure_stress_test_3:
	timeout -k 10s 600s cargo test --release -- coordinator_recovery_3_loop --nocapture --include-ignored

coordinator_failure_stress_test_7:
	timeout -k 10s 600s cargo test --release -- coordinator_recovery_7_loop --nocapture --include-ignored

bench:
	timeout -k 10s 180s cargo test throughput_3_ser --release -- --nocapture --include-ignored

bench/ro:
	timeout -k 10s 300s cargo test bench_ro_ --release -- --nocapture --include-ignored

bench/rw:
	timeout -k 10s 300s cargo test bench_rw --release -- --nocapture --include-ignored

bench/mix:
	timeout -k 10s 300s cargo test bench_mixed --release -- --nocapture --include-ignored

bench/compare:
	scripts/bench-compare.sh

ci/bench-compare:
	BENCH_CPUS=2 BENCH_MEM=4 BENCH_CLIENTS=2 BENCH_DURATION=2 BENCH_KEY_SPACE=20 scripts/bench-compare.sh

fuzz:
	./scripts/fuzz-multi-seed.sh

fuzz100:
	FUZZ_ITERATIONS=100 FUZZ_PARALLEL=4 ./scripts/fuzz-multi-seed.sh

# Maelstrom (Jepsen linearizability checker)
MAELSTROM_VERSION := 0.2.4
MAELSTROM_DIR := .maelstrom
MAELSTROM_BIN := $(MAELSTROM_DIR)/maelstrom/maelstrom

$(MAELSTROM_BIN):
	@command -v java >/dev/null 2>&1 || { \
		echo "ERROR: Java JDK 11+ is required for maelstrom but 'java' was not found in PATH."; \
		echo "Install it with: sudo apt install openjdk-11-jdk (Debian/Ubuntu)"; \
		echo "              or: brew install openjdk@11 (macOS)"; \
		exit 1; \
	}
	@command -v gnuplot >/dev/null 2>&1 || { \
		echo "ERROR: gnuplot is required for maelstrom but 'gnuplot' was not found in PATH."; \
		echo "Install it with: sudo apt install gnuplot-nox (Debian/Ubuntu)"; \
		echo "              or: brew install gnuplot (macOS)"; \
		exit 1; \
	}
	@mkdir -p $(MAELSTROM_DIR)
	@echo "Downloading maelstrom v$(MAELSTROM_VERSION)..."
	@curl -fsSL https://github.com/jepsen-io/maelstrom/releases/download/v$(MAELSTROM_VERSION)/maelstrom.tar.bz2 | tar -xjf - -C $(MAELSTROM_DIR)
	@echo "Maelstrom v$(MAELSTROM_VERSION) installed to $(MAELSTROM_DIR)/"

# ── Maelstrom linearizability tests ──────────────────────────────────
#
# Each target exercises a different read path / clock configuration.
# Maelstrom's lin-kv workload verifies linearizability via Knossos.
#
# Key settings (env vars → Maelstrom binary):
#   TAPIR_CLOCK_SKEW_MAX          Max simulated clock offset (ns). 0 = synchronized.
#   TAPIR_LINEARIZABLE_READ_METHOD  ro_txn_get | rw_txn_get_commit
#     ro_txn_get:           RO transaction — quorum read (+ optional fast path).
#     rw_txn_get_commit:    RW transaction — OCC validates the read at commit.
#   TAPIR_RO_FAST_PATH_DELAY_MS   Sleep before trying single-replica read_validated.
#   TAPIR_READ_TIMEOUT_MS         Timeout for the read_validated RPC.
#   TAPIR_VIEW_CHANGE_INTERVAL_MS Override IR view change timer (default 2s).
#   TAPIR_INCONSISTENT_RESULT_DEADLINE_MS  IR client deadline for inconsistent ops.
#   TAPIR_RO_CLOCK_SKEW_UNCERTAINTY_BOUND  TrueTime uncertainty window (ns).
#
# Setting relationships:
#   - fast_path_delay is a performance knob, not a correctness one.
#     Too short → more misses (safe quorum fallback). Never causes stale reads.
#     Recommended: fast_path_delay > view_change_interval for high hit rate.
#   - view_change_interval controls how often replicas initiate view changes.
#     Too short under partition nemesis → replicas spend most time in
#     ViewChanging, rejecting operations → near-zero availability.
#     The default (2s) works well with Maelstrom's 20s nemesis-interval.
#   - All delays (fast_path_delay + read_timeout + quorum_read) must fit
#     within Maelstrom's ~1s per-operation timeout, otherwise reads show
#     as :net-timeout (fail) even though the system is correct.
#   - inconsistent_result_deadline must exceed view_change_interval so
#     the IR client can retry after a view change completes.
#
# Misconfiguration examples:
#   - view_change_interval=200ms + partition nemesis:
#     View numbers advance every 200ms, faster than view changes complete.
#     System stuck in ViewChanging → 0% availability → test fails on stats.
#   - fast_path_delay=2s with Maelstrom:
#     Delay alone exceeds the ~1s operation timeout → all reads timeout.
#   - clock_skew > 0 + ro_txn_get (without TrueTime uncertainty_bound):
#     Snapshot timestamp may be < commit_ts of a completed write → stale read.
#     This is a KNOWN limitation (Paper S6.1), tested by skewed-ro-txn-get-fail.

maelstrom: maelstrom-sync-ro-txn-get maelstrom-skewed-rw-txn-get-commit maelstrom-skewed-ro-txn-get-fail maelstrom-sync-ro-fast-path maelstrom-sync-ro-fast-path-partition maelstrom-skew-ro-slow-path-truetime

# Baseline: synchronized clocks, RO quorum read, with partitions.
# Proves the quorum read path (f+1 merge by highest write_ts) is linearizable.
maelstrom-sync-ro-txn-get:
	TAPIR_CLOCK_SKEW_MAX=0 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get MAELSTROM_RUN_NAME=sync-ro-txn-get $(MAKE) maelstrom-run

# Clock skew + RW transactions: OCC validates reads at commit time.
# Proves linearizability when clocks are skewed, using the RW path.
maelstrom-skewed-rw-txn-get-commit:
	TAPIR_CLOCK_SKEW_MAX=1000 TAPIR_LINEARIZABLE_READ_METHOD=rw_txn_get_commit MAELSTROM_RUN_NAME=skewed-rw-txn-get-commit $(MAKE) maelstrom-run

# Negative test: clock skew + RO reads (no TrueTime) → NOT linearizable.
# Expected to FAIL. Validates that Maelstrom catches the known limitation.
maelstrom-skewed-ro-txn-get-fail:
	@echo "Expecting linearizability FAILURE (RO reads under clock skew)..."
	@TAPIR_CLOCK_SKEW_MAX=1000 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get MAELSTROM_RUN_NAME=skewed-ro-txn-get-fail $(MAKE) maelstrom-run \
		&& { echo "ERROR: expected maelstrom to fail but it passed"; exit 1; } \
		|| echo "Good: maelstrom failed as expected (RO reads not linearizable under clock skew)"

# Fast path (no partitions): exercises single-replica validated reads
# (do_uncommitted_get_validated). Replica returns a cached value when
# last_read_ts >= snapshot_ts, skipping quorum. On miss, falls back to
# quorum read. No nemesis → high availability, high fast path hit rate.
maelstrom-sync-ro-fast-path:
	TAPIR_CLOCK_SKEW_MAX=0 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get TAPIR_RO_FAST_PATH_DELAY_MS=200 TAPIR_READ_TIMEOUT_MS=200 TAPIR_INCONSISTENT_RESULT_DEADLINE_MS=500 MAELSTROM_RUN_NAME=sync-ro-fast-path $(MAKE) maelstrom-run-no-nemesis

# Fast path (with partitions): same fast path logic under network faults.
# Uses default 2s view_change_interval for stability under partitions.
# fast_path_delay (200ms) < view_change_interval → mostly misses, proving
# that misses safely fall back to quorum reads without stale data.
maelstrom-sync-ro-fast-path-partition:
	TAPIR_CLOCK_SKEW_MAX=0 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get TAPIR_RO_FAST_PATH_DELAY_MS=200 TAPIR_READ_TIMEOUT_MS=200 TAPIR_INCONSISTENT_RESULT_DEADLINE_MS=500 MAELSTROM_RUN_NAME=sync-ro-fast-path-partition $(MAKE) maelstrom-run

# TrueTime-style: RO linearizability under clock skew (Paper S6.1 + Spanner).
# uncertainty_bound = 2 * max_skew covers worst-case bidirectional skew.
# snapshot_ts is shifted by the bound so it always >= commit_ts of any
# completed write. The delay ensures FinalizeInconsistent(Commit) propagates.
maelstrom-skew-ro-slow-path-truetime:
	TAPIR_CLOCK_SKEW_MAX=100 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get TAPIR_RO_CLOCK_SKEW_UNCERTAINTY_BOUND=200 MAELSTROM_RUN_NAME=skew-ro-slow-path-truetime $(MAKE) maelstrom-run

# ── Manual / exploratory targets (NOT in `make maelstrom`) ──────────

# Deliberately broken fast path: 1ms delay is far too short for any
# view change sync. read_validated may see stale last_read_ts and return
# an old value. Expected to fail with linearizability violations.
# Probabilistic — may occasionally pass when FinalizeInconsistent
# propagates fast enough over Maelstrom's zero-latency transport.
maelstrom-sync-ro-fast-path-may-fail:
	TAPIR_CLOCK_SKEW_MAX=0 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get TAPIR_RO_FAST_PATH_DELAY_MS=1 TAPIR_READ_TIMEOUT_MS=200 TAPIR_VIEW_CHANGE_INTERVAL_MS=200 MAELSTROM_RUN_NAME=sync-ro-fast-path-may-fail $(MAKE) maelstrom-run

MAELSTROM_RUN_NAME ?= unnamed
maelstrom-run-no-nemesis: $(MAELSTROM_BIN)
	cargo build --release -p tapi-maelstrom
	$(eval MAELSTROM_LOG := /tmp/maelstrom-output-$(MAELSTROM_RUN_NAME)-$(shell date +%Y%m%d-%H%M%S).txt)
	bash -c 'set -o pipefail; $(MAELSTROM_BIN) test -w lin-kv --bin target/release/maelstrom --latency 0 --rate 10 --time-limit 90 --concurrency 20 2>&1 | tee $(MAELSTROM_LOG)'; \
	EXIT=$$?; \
	cp $(MAELSTROM_LOG) /tmp/maelstrom-output.txt; \
	bash scripts/maelstrom-summary.sh $(MAELSTROM_LOG); \
	if [ $$EXIT -eq 0 ]; then \
		echo "PASSED"; \
	else \
		echo "FAILED (exit $$EXIT). Investigate with: less $(MAELSTROM_LOG)"; \
		exit 1; \
	fi

maelstrom-run: $(MAELSTROM_BIN)
	cargo build --release -p tapi-maelstrom
	$(eval MAELSTROM_LOG := /tmp/maelstrom-output-$(MAELSTROM_RUN_NAME)-$(shell date +%Y%m%d-%H%M%S).txt)
	bash -c 'set -o pipefail; $(MAELSTROM_BIN) test -w lin-kv --bin target/release/maelstrom --latency 0 --rate 10 --time-limit 90 --concurrency 20 --nemesis partition --nemesis-interval 20 2>&1 | tee $(MAELSTROM_LOG)'; \
	EXIT=$$?; \
	cp $(MAELSTROM_LOG) /tmp/maelstrom-output.txt; \
	bash scripts/maelstrom-summary.sh $(MAELSTROM_LOG); \
	if [ $$EXIT -eq 0 ]; then \
		echo "PASSED (exit $$EXIT). Log: $(MAELSTROM_LOG)"; \
	else \
		echo "FAILED (exit $$EXIT). Investigate with: less $(MAELSTROM_LOG)"; \
	fi; \
	exit $$EXIT

ci: test maelstrom ci/operator-lint ci/operator-test
	@echo "All CI checks passed."

ci-full: ci ci/testbed-kube-operator ci/testbed-kube
	@echo "All CI checks (including E2E) passed."

ci/operator-lint:
	$(MAKE) -C kubernetes/operator lint

ci/operator-test:
	$(MAKE) -C kubernetes/operator test

ci/testbed-kube-operator:
	TAPIR_KIND=1 scripts/testbed-kube-operator.sh up
	scripts/testbed-kube-operator.sh down

ci/testbed-kube-operator-tls:
	TAPIR_TLS=1 TAPIR_KIND=1 TAPIR_WAIT_TIMEOUT=300 scripts/testbed-kube-operator.sh up
	scripts/testbed-kube-operator.sh down

ci/testbed-kube:
	TAPIR_KIND=1 scripts/testbed-kube.sh up
	scripts/testbed-kube.sh demo
	scripts/testbed-kube.sh down

ci/bench-solo:
	scripts/testbed-solo.sh up
	BENCH_CLUSTER=127.0.0.1:6000,127.0.0.1:6001,127.0.0.1:6002 \
	  timeout -k 10s 120s cargo test bench_rw --release -- --nocapture --include-ignored \
	  || { scripts/testbed-solo.sh down; exit 1; }
	scripts/testbed-solo.sh down

ci/testbed-solo:
	scripts/testbed-solo.sh up && scripts/testbed-solo.sh down || { scripts/testbed-solo.sh down; exit 1; }

ci/testbed-docker-compose:
	scripts/testbed-docker-compose.sh up && scripts/testbed-docker-compose.sh down || { scripts/testbed-docker-compose.sh down; exit 1; }

ci/testbed-kube-operator-s3:
	TAPIR_KIND=1 scripts/testbed-kube-operator-s3.sh up
	scripts/testbed-kube-operator-s3.sh down

ci/testbed: ci/testbed-solo ci/testbed-docker-compose ci/testbed-kube ci/testbed-kube-operator ci/testbed-kube-operator-tls ci/testbed-kube-operator-s3
	@echo "All testbed checks passed."

ci/fuzz-diagnose:
	FUZZ_RUNS=10 ./scripts/fuzz-diagnose.sh

ci/fuzz-multi-seed:
	./scripts/fuzz-multi-seed.sh

ci/test-s3:
	docker run -d --name tapi-minio -p 9100:9000 \
	  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
	  minio/minio server /data
	sleep 2
	docker run --rm --network host \
	  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
	  amazon/aws-cli --endpoint-url http://localhost:9100 s3 mb s3://tapi-test \
	  || { docker rm -f tapi-minio; exit 1; }
	AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
	  cargo clippy --all-targets -- -D warnings \
	  && AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
	  TAPI_TEST_S3_ENDPOINT=http://localhost:9100 \
	  cargo test --release -- s3backup remote_store::tests::test_auto_flush remote_store::tests::test_read_replica remote_store::tests::test_e2e_s3 \
	  ; EXIT=$$?; docker rm -f tapi-minio; exit $$EXIT
