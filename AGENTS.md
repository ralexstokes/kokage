# tokio-otp

## Running anything

All tooling (`cargo`, `just`, `nextest`, `mdbook`, `nixfmt`) comes from the Nix
devshell. It is **not** on the base PATH. Prefix commands with `./scripts/dev`:

```sh
./scripts/dev just ci          # the full local CI mirror
./scripts/dev just test
./scripts/dev cargo nextest run --workspace
```

`./scripts/dev` exec's straight through when the devshell for *this* checkout is already
active, so it is free in an interactive shell and correct everywhere else. Use
it rather than assuming direnv has loaded — see below for why.

`just ci` is the local mirror of CI; `just ci-nix` runs the authoritative clean
Nix lane. `nix/crane-checks.nix` and `flake.nix` define CI, and the `justfile`
recipes mirror them — keep the two in sync when changing either.

## Worktrees

Create them wherever suits the task, with one hard rule: **not in `/tmp`** — it
is cleared on reboot, which has already lost in-flight agent work here.

`./scripts/dev` gives any worktree the correct toolchain no matter how it was created:

- direnv never loads in non-interactive (agent) shells, and `direnv allow` is
  keyed on the absolute `.envrc` path, so a fresh worktree has no toolchain on
  the base PATH at all.
- A shell spawned from another checkout inherits *that* checkout's devshell, so
  a worktree can appear to work while silently using the wrong toolchain.
  `./scripts/dev` detects this via `TOKIO_OTP_DEVSHELL` and re-enters the right one.

Worktrees created by Claude Code are direnv-allowed automatically by the
`WorktreeCreate` hook in `.claude/settings.json`; for a manually created one,
run `direnv allow <dir>` once if you want the devshell in interactive shells.
