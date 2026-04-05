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

maelstrom: maelstrom-sync-ro-txn-get maelstrom-skewed-rw-txn-get-commit maelstrom-skewed-ro-txn-get-fail maelstrom-sync-ro-fast-path maelstrom-skew-ro-slow-path-truetime

# Synchronized clocks: RO quorum read is linearizable.
maelstrom-sync-ro-txn-get:
	TAPIR_CLOCK_SKEW_MAX=0 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get MAELSTROM_RUN_NAME=sync-ro-txn-get $(MAKE) maelstrom-run

maelstrom-skewed-rw-txn-get-commit:
	TAPIR_CLOCK_SKEW_MAX=1000 TAPIR_LINEARIZABLE_READ_METHOD=rw_txn_get_commit MAELSTROM_RUN_NAME=skewed-rw-txn-get-commit $(MAKE) maelstrom-run

# Under clock skew (max 1000ms), RO reads are not linearizable (Paper S6.1).
# Use RW transaction: OCC validates the read at commit time.
maelstrom-skewed-ro-txn-get-fail:
	@echo "Expecting linearizability FAILURE (RO reads under clock skew)..."
	@TAPIR_CLOCK_SKEW_MAX=1000 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get MAELSTROM_RUN_NAME=skewed-ro-txn-get-fail $(MAKE) maelstrom-run \
		&& { echo "ERROR: expected maelstrom to fail but it passed"; exit 1; } \
		|| echo "Good: maelstrom failed as expected (RO reads not linearizable under clock skew)"

maelstrom-sync-ro-fast-path:
	TAPIR_CLOCK_SKEW_MAX=0 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get TAPIR_RO_FAST_PATH_DELAY_MS=200 TAPIR_READ_TIMEOUT_MS=200 TAPIR_VIEW_CHANGE_INTERVAL_MS=200 TAPIR_INCONSISTENT_RESULT_DEADLINE_MS=500 MAELSTROM_RUN_NAME=sync-ro-fast-path $(MAKE) maelstrom-run

# Expect frequent failure: 1ms delay is too short for replicas to sync via view change
# (view change interval is 200ms), so read_validated may return stale data.
# When it did return stale data, it's linearizability violation that maelstrom catches.
#
# NOT included in `make maelstrom` — probabilistic, not deterministic.
# With zero-latency transport, async FinalizeInconsistent propagates
# nearly instantly, so read_validated often returns correct data even
# with 1ms delay. The stale-read scenario requires a specific partition
# timing where a prior commit_get set read_ts on a replica that then
# misses a subsequent write's FinalizeInconsistent. Run manually to
# check: it should fail more often than not, but may occasionally pass.
maelstrom-sync-ro-fast-path-may-fail:
	TAPIR_CLOCK_SKEW_MAX=0 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get TAPIR_RO_FAST_PATH_DELAY_MS=1 TAPIR_READ_TIMEOUT_MS=200 TAPIR_VIEW_CHANGE_INTERVAL_MS=200 MAELSTROM_RUN_NAME=sync-ro-fast-path-may-fail $(MAKE) maelstrom-run

# TrueTime-style RO linearizability under clock skew (Paper S6.1 + Spanner).
# Clock skew up to 100ms (one-directional: now + [0,100ms)).
# Uncertainty bound = 2 * max_skew = 200ms: covers worst case where writer's
# clock is +100ms ahead and reader's clock is +0ms (or bidirectional ±100ms).
# snapshot_ts is adjusted by the bound so that snapshot_ts >= commit_ts of
# any completed write. The delay before quorum_read ensures async
# FinalizeInconsistent(Commit) propagation. No fast path — always quorum read.
maelstrom-skew-ro-slow-path-truetime:
	TAPIR_CLOCK_SKEW_MAX=100 TAPIR_LINEARIZABLE_READ_METHOD=ro_txn_get TAPIR_RO_CLOCK_SKEW_UNCERTAINTY_BOUND=200 MAELSTROM_RUN_NAME=skew-ro-slow-path-truetime $(MAKE) maelstrom-run

MAELSTROM_RUN_NAME ?= unnamed
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

ci/testbed: ci/testbed-solo ci/testbed-docker-compose ci/testbed-kube ci/testbed-kube-operator ci/testbed-kube-operator-tls
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
	  cargo clippy --features s3 --all-targets -- -D warnings \
	  && AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
	  TAPI_TEST_S3_ENDPOINT=http://localhost:9100 \
	  cargo test --features s3 --release -- s3backup \
	  ; EXIT=$$?; docker rm -f tapi-minio; exit $$EXIT
