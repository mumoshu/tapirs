.PHONY: test lock_server_stress_test coordinator_failure_stress_test_3 coordinator_failure_stress_test_7 bench fuzz fuzz100 maelstrom ci/testbed-kubernetes-operator

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

maelstrom:
	cargo build --release -p tapi-maelstrom
	maelstrom test -w lin-kv --bin target/release/maelstrom --latency 0 --rate 10 --time-limit 90 --concurrency 20 --nemesis partition --nemesis-interval 20

ci/testbed-kubernetes-operator:
	TAPIR_KIND=1 scripts/testbed-kube-operator.sh up
	scripts/testbed-kube-operator.sh down
