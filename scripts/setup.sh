#!/usr/bin/env bash
set -euo pipefail

# Install latest nightly toolchain and required components
if ! command -v rustup >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
  source "$HOME/.cargo/env"
fi

rustup toolchain install nightly --allow-downgrade --profile minimal
rustup default nightly
rustup component add rustfmt clippy

# Install other lint tools if needed
# (placeholder for future tools)

echo "Rust nightly toolchain and components installed"
