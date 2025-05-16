# Agent Instructions

These instructions apply to all files in this repository.

## Setup

Run `scripts/setup.sh` to install the Rust nightly toolchain and required
components (`rustfmt` and `clippy`). The repository expects the nightly
channel.

## Development Workflow

1. Format the code with `cargo fmt --all`.
2. Run `cargo clippy --all-targets --all-features -- -D warnings`.
3. Execute tests with `cargo test`.

All checks should succeed before committing changes.
