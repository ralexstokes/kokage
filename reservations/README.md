# Kokage crate markers

These standalone packages hold the crates.io names selected in
[issue #245](https://github.com/ralexstokes/tokio-otp/issues/245) while the
project rename is completed. Each `0.0.0` release is dependency-free and
intentionally exposes no API.

`kokage-console` is intentionally not published or reserved. Its local marker
is retained only as a draft and must not be published unless the project
separately decides to distribute the console as a crates.io package.

Commit and push this directory before publishing. Publish from a clean
worktree without `--allow-dirty`:

```sh
./scripts/dev cargo publish --locked --manifest-path reservations/kokage/Cargo.toml
./scripts/dev cargo publish --locked --manifest-path reservations/kokage-supervisor/Cargo.toml
./scripts/dev cargo publish --locked --manifest-path reservations/kokage-derive/Cargo.toml
./scripts/dev cargo publish --locked --manifest-path reservations/kokage-tokio/Cargo.toml
```

After crates.io has indexed all four releases, yank the empty marker versions
so new dependency resolution will not select them:

```sh
./scripts/dev cargo yank --version 0.0.0 kokage
./scripts/dev cargo yank --version 0.0.0 kokage-supervisor
./scripts/dev cargo yank --version 0.0.0 kokage-derive
./scripts/dev cargo yank --version 0.0.0 kokage-tokio
```

Do not delete the crates: deletion would release the names. The real packages
can begin at version `0.1.0` after the rename.
