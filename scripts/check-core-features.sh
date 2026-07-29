#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

cargo check --locked -p kokage-supervisor --no-default-features
cargo check --locked -p kokage --no-default-features

tree=$(cargo tree --locked -p kokage --no-default-features \
    --edges normal,build,features --invert tokio)
for feature in rt rt-multi-thread time net process signal; do
    if grep -Fq "tokio feature \"$feature\"" <<<"$tree"; then
        printf 'core dependency graph enables forbidden Tokio feature: %s\n' "$feature" >&2
        exit 1
    fi
done

for feature in sync macros; do
    if ! grep -Fq "tokio feature \"$feature\"" <<<"$tree"; then
        printf 'core dependency graph is missing required Tokio feature: %s\n' "$feature" >&2
        exit 1
    fi
done

printf 'core builds without defaults and Tokio is limited to sync + macros\n'
