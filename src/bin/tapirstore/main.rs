// Experimental direct store interpreter for UnifiedStore internals.
//
// Usage:
//   tapirstore "open /tmp/store; tapir-prepare 1:1 5 w:x=v1; tapir-commit 1:1 5; get x"
//
// Command families are intentionally split:
// - TAPIR path (`tapir-prepare`, `tapir-commit`) for MVCC/value durability.
// - IR path (`ir-prepare`, `ir-commit`) for inconsistent record persistence.
//
// Backward-compat aliases:
// - `prepare` == `tapir-prepare`
// - `commit`  == `tapir-commit`
fn main() {
    let code = tapirs::unified::cli::run(
        std::env::args(),
        std::io::stdin().lock(),
        std::io::stdout().lock(),
        std::io::stderr().lock(),
    );
    std::process::exit(code);
}

#[cfg(test)]
mod integration_test;
