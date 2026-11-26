#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$root"
cargo build --release --locked
mkdir -p target
out="target/uring-$(date +%Y%m%d-%H%M%S)-$$"
exec python3 -B tools/run_io_uring_durability.py --recorder target/release/recorder --out "$out" --compare
