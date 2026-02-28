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
