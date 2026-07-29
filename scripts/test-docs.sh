#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docs_build_messages=$(mktemp)
docs_wrapper_dir=
cleanup() {
  rm -f "$docs_build_messages"
  if [[ -n "$docs_wrapper_dir" ]]; then
    rm -rf "$docs_wrapper_dir"
  fi
}
trap cleanup EXIT
if ! cargo build --locked -p kokage --all-features --message-format=json-render-diagnostics \
  > "$docs_build_messages"; then
  jq -r 'select(.reason == "compiler-message") | .message.rendered // empty' \
    "$docs_build_messages" >&2
  exit 1
fi

artifact_rlib() {
  jq -rs --arg crate "$1" '
    [
      .[]
      | select(
          .reason == "compiler-artifact"
          and .target.name == $crate
          and (.target.kind | index("lib"))
        )
      | .filenames[]
      | select(endswith(".rlib"))
    ]
    | last // empty
  ' "$docs_build_messages"
}

kokage_docs_rlib=$(artifact_rlib kokage)
tokio_docs_rlib=$(artifact_rlib tokio)
if [[ -z "$kokage_docs_rlib" || -z "$tokio_docs_rlib" ]]; then
  echo "cargo did not report the kokage and tokio rlibs required for docs" >&2
  exit 1
fi

docs_deps_dir=$(dirname "$tokio_docs_rlib")
docs_wrapper_dir=$(mktemp -d "$docs_deps_dir/docs-test-bin.XXXXXX")
docs_real_rustdoc=$(command -v rustdoc)
docs_bash=$(command -v bash)

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
