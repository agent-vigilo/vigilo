//! Process entry point; the CLI adapter owns parsing, logging and exit codes.

#[tokio::main]
async fn main() -> std::process::ExitCode {
    vigilo::cli::run().await
}
