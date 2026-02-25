.PHONY: test lint lock_server_stress_test coordinator_failure_stress_test_3 coordinator_failure_stress_test_7 bench bench/ro bench/rw bench/mix fuzz fuzz100 maelstrom ci ci-full ci/operator-lint ci/operator-test ci/testbed-kube-operator ci/testbed-kube-operator-tls ci/testbed-kube ci/testbed-docker-compose ci/testbed-solo ci/testbed ci/fuzz-diagnose ci/fuzz-multi-seed

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

maelstrom: $(MAELSTROM_BIN)
	cargo build --release -p tapi-maelstrom
	$(MAELSTROM_BIN) test -w lin-kv --bin target/release/maelstrom --latency 0 --rate 10 --time-limit 90 --concurrency 20 --nemesis partition --nemesis-interval 20

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
