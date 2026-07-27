#!/usr/bin/env bash

set -euo pipefail

metadata=$(cargo metadata --locked --no-deps --format-version 1)
target_dir=$(jq -r '.target_directory' <<<"$metadata")
mapfile -t examples < <(
  jq -r '
    .packages[] as $package
    | $package.targets[]
    | select(.kind | index("example"))
    | [$package.name, .name]
    | @tsv
  ' <<<"$metadata"
)

console_found=false
for target in "${examples[@]}"; do
  IFS=$'\t' read -r package example <<<"$target"
  if [[ "$package" == "tokio-otp-console" && "$example" == "console" ]]; then
    console_found=true
    continue
  fi

  echo "Running $package/$example"
  timeout --kill-after=10s 60s \
    cargo run --locked -p "$package" --example "$example" --all-features
done

if [[ "$console_found" != true ]]; then
  echo "Console example was not discovered" >&2
  exit 1
fi

echo "Running tokio-otp-console/console"
cargo build --locked -p tokio-otp-console --example console --all-features
console_log=$(mktemp)
console_pid=
cleanup() {
  if [[ -n "$console_pid" ]] && kill -0 "$console_pid" 2>/dev/null; then
    kill -KILL "$console_pid" 2>/dev/null || true
    wait "$console_pid" 2>/dev/null || true
  fi
  rm -f "$console_log"
}
trap cleanup EXIT

"$target_dir/debug/examples/console" >"$console_log" 2>&1 &
console_pid=$!
console_started=false
for _ in {1..300}; do
  if grep -q "console available at" "$console_log"; then
    console_started=true
    break
  fi
  if ! kill -0 "$console_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

if [[ "$console_started" != true ]]; then
  cat "$console_log"
  echo "Console example did not bind within 30 seconds" >&2
  exit 1
fi

kill -INT "$console_pid"
if ! timeout 10s tail --pid="$console_pid" -f /dev/null; then
  cat "$console_log"
  echo "Console example did not stop after SIGINT" >&2
  exit 1
fi

if wait "$console_pid"; then
  console_status=0
else
  console_status=$?
fi
console_pid=
cat "$console_log"
if [[ "$console_status" -ne 0 ]]; then
  echo "Console example exited with status $console_status" >&2
  exit "$console_status"
fi
