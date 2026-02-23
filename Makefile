.PHONY: test lock_server_stress_test coordinator_failure_stress_test_3 coordinator_failure_stress_test_7 bench fuzz fuzz100 maelstrom ci ci-full ci/operator-lint ci/operator-test ci/testbed-kubernetes-operator

test:
	cargo clippy --workspace -- -D clippy::disallowed_methods && timeout -k 10s 120s cargo test --workspace --release

lock_server_stress_test:
	timeout -k 10s 600s cargo test --release -- lock_server_loop --nocapture --include-ignored

coordinator_failure_stress_test_3:
	timeout -k 10s 600s cargo test --release -- coordinator_recovery_3_loop --nocapture --include-ignored

coordinator_failure_stress_test_7:
	timeout -k 10s 600s cargo test --release -- coordinator_recovery_7_loop --nocapture --include-ignored

bench:
	timeout -k 10s 180s cargo test throughput_3_ser --release -- --nocapture --include-ignored

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

ci-full: ci ci/testbed-kubernetes-operator
	@echo "All CI checks (including E2E) passed."

ci/operator-lint:
	$(MAKE) -C kubernetes/operator lint

ci/operator-test:
	$(MAKE) -C kubernetes/operator test

ci/testbed-kubernetes-operator:
	TAPIR_KIND=1 scripts/testbed-kube-operator.sh up
	scripts/testbed-kube-operator.sh down
