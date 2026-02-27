.PHONY: test lint lock_server_stress_test coordinator_failure_stress_test_3 coordinator_failure_stress_test_7 bench bench/ro bench/rw bench/mix bench/compare fuzz fuzz100 maelstrom maelstrom-run maelstrom-sync maelstrom-skewed maelstrom-sync-ro-fast-path maelstrom-sync-ro-fast-path-fail ci ci-full ci/operator-lint ci/operator-test ci/bench-solo ci/bench-compare ci/testbed-kube-operator ci/testbed-kube-operator-tls ci/testbed-kube ci/testbed-docker-compose ci/testbed-solo ci/testbed ci/fuzz-diagnose ci/fuzz-multi-seed ci/test-surrealkv ci/test-s3

lint:
	cargo clippy --workspace --all-targets -- -D warnings -D clippy::iter_over_hash_type && ./scripts/check-determinism.sh

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

maelstrom: maelstrom-sync maelstrom-skewed

maelstrom-sync:
	TAPIR_CLOCK=sync $(MAKE) maelstrom-run

maelstrom-skewed:
	TAPIR_CLOCK=skewed $(MAKE) maelstrom-run

maelstrom-sync-ro-fast-path:
	TAPIR_CLOCK=sync TAPIR_RO_FAST_PATH_DELAY_MS=200 TAPIR_VIEW_CHANGE_INTERVAL_MS=200 $(MAKE) maelstrom-run

# Expect failure: 1ms delay is too short for replicas to sync via view change
# (view change interval is 200ms), so read_validated returns stale data.
maelstrom-sync-ro-fast-path-fail:
	@echo "Expecting linearizability FAILURE (delay 1ms < view change interval 200ms)..."
	@TAPIR_CLOCK=sync TAPIR_RO_FAST_PATH_DELAY_MS=1 TAPIR_VIEW_CHANGE_INTERVAL_MS=200 $(MAKE) maelstrom-run \
		&& { echo "ERROR: expected maelstrom to fail but it passed"; exit 1; } \
		|| echo "Good: maelstrom failed as expected (fast path delay too short)"

maelstrom-run: $(MAELSTROM_BIN)
	cargo build --release -p tapi-maelstrom
	bash -c 'set -o pipefail; $(MAELSTROM_BIN) test -w lin-kv --bin target/release/maelstrom --latency 0 --rate 10 --time-limit 90 --concurrency 20 --nemesis partition --nemesis-interval 20 2>&1 | tee /tmp/maelstrom-output.txt'; \
	EXIT=$$?; \
	bash scripts/maelstrom-summary.sh /tmp/maelstrom-output.txt; \
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

ci/test-surrealkv:
	cargo clippy --features surrealkv --all-targets -- -D warnings
	cargo test --features surrealkv --release -- surrealkvstore

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
