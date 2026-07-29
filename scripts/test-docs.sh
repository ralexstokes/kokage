#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

cargo build --locked -p kokage --all-features

kokage_docs_rlib=$(pwd)/$(ls -t target/debug/deps/libkokage-*.rlib | sed -n '1p')
tokio_docs_rlib=$(pwd)/$(ls -t target/debug/deps/libtokio-*.rlib | sed -n '1p')
docs_deps_dir=$(pwd)/target/debug/deps
docs_wrapper_dir=$(mktemp -d "$docs_deps_dir/docs-test-bin.XXXXXX")
docs_real_rustdoc=$(command -v rustdoc)
docs_bash=$(command -v bash)
trap 'rm -rf "$docs_wrapper_dir"' EXIT

export KOKAGE_DOCS_RLIB="$kokage_docs_rlib"
export KOKAGE_DOCS_TOKIO_RLIB="$tokio_docs_rlib"
export KOKAGE_DOCS_DEPS="$docs_deps_dir"
export KOKAGE_DOCS_REAL_RUSTDOC="$docs_real_rustdoc"

printf '%s\n' \
  "#!$docs_bash" \
  'exec "$KOKAGE_DOCS_REAL_RUSTDOC" "$@" -L "dependency=$KOKAGE_DOCS_DEPS" --extern "kokage=$KOKAGE_DOCS_RLIB" --extern "tokio=$KOKAGE_DOCS_TOKIO_RLIB"' \
  > "$docs_wrapper_dir/rustdoc"
chmod +x "$docs_wrapper_dir/rustdoc"

# mdBook launches `rustdoc` through PATH. The generated wrapper uses the
# resolved Bash path because clean Nix sandboxes do not provide /usr/bin/env.
export PATH="$docs_wrapper_dir:$PATH"
if [[ $(command -v rustdoc) != "$docs_wrapper_dir/rustdoc" ]]; then
  echo "failed to select the documentation rustdoc wrapper" >&2
  exit 1
fi

mdbook test docs -L "$docs_deps_dir"
"$docs_wrapper_dir/rustdoc" --test --edition=2024 README.md
