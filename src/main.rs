use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
struct Args {}

fn main() {
    let _args = Args::parse();

    println!("Hello, world!");
}
