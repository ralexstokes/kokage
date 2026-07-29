#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

toolchain=${KOKAGE_RUSTDOC_TOOLCHAIN-nightly}
cargo=(cargo)
if [[ -n "$toolchain" ]]; then
    cargo+=("+$toolchain")
fi

status=0
package=kokage
crate=kokage
RUSTDOCFLAGS="${RUSTDOCFLAGS:-} -Z unstable-options --output-format json" \
    "${cargo[@]}" rustdoc --locked -p "$package" --all-features --lib

leaks=$(jq -r -f scripts/public-api-paths.jq "target/doc/$crate.json")
if [[ -n "$leaks" ]]; then
    printf 'public API of %s exposes forbidden runtime paths:\n%s\n' "$package" "$leaks" >&2
    status=1
else
    printf 'public API of %s is free of tokio and tokio_util paths\n' "$package"
fi

exit "$status"
