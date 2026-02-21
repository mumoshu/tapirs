.PHONY: test lock_server_stress_test coordinator_failure_stress_test_3 coordinator_failure_stress_test_7 bench fuzz fuzz100 maelstrom

test:
	cargo clippy -- -D clippy::disallowed_methods && timeout -k 10s 180s cargo test --release

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
	cargo build --release --features maelstrom --bin maelstrom
	maelstrom test -w lin-kv --bin target/release/maelstrom --latency 0 --rate 10 --time-limit 90 --concurrency 20 --nemesis partition --nemesis-interval 20
