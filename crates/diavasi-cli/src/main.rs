use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "diavasi", version, about = "Diavasi administration CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print library version (Stage 0 placeholder; serve/admin arrive in Stage 4).
    Version,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Version => {
            println!("diavasi {}", diavasi::VERSION);
        }
    }
}
