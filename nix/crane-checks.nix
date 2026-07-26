# Authoritative CI check definitions (run via `nix flake check` / `just ci-nix`).
# The `ci` recipe in the top-level justfile mirrors these for fast local runs —
# keep the cargo flags in sync.
{
  pkgs,
  crane,
  rustToolchain,
  nightlyToolchain,
  src,
}:
let
  craneLib = crane.mkLib pkgs;
  craneLibStable = craneLib.overrideToolchain rustToolchain;
  craneLibNightly = craneLib.overrideToolchain nightlyToolchain;
  cargoSrc = pkgs.lib.cleanSourceWith {
    src = pkgs.lib.cleanSource src;
    filter =
      path: type:
      type == "directory"
      || craneLib.filterCargoSources path type
      || pkgs.lib.hasSuffix ".stderr" (toString path)
      || pkgs.lib.hasSuffix "/assets/index.html" (toString path);
  };
  commonArgs = {
    CARGO_PROFILE = "";
    pname = "tokio-otp";
    src = cargoSrc;
    strictDeps = true;
    version = "0.1.0";
  };
  dependencyArgs = commonArgs // {
    cargoCheckExtraArgs = "";
    cargoExtraArgs = "--locked --workspace --all-targets --all-features";
  };
  cargoArtifactsStable = craneLibStable.buildDepsOnly dependencyArgs;
  cargoArtifactsNightly = craneLibNightly.buildDepsOnly dependencyArgs;
in
{
  cargo-fmt = craneLibNightly.cargoFmt (
    commonArgs
    // {
      cargoExtraArgs = "--all";
    }
  );

  cargo-clippy = craneLibNightly.cargoClippy (
    commonArgs
    // {
      cargoArtifacts = cargoArtifactsNightly;
      cargoExtraArgs = "--locked";
      cargoClippyExtraArgs = "--workspace --all-targets --all-features -- -D warnings";
      doInstallCargoArtifacts = false;
    }
  );

  cargo-ci = craneLibStable.mkCargoDerivation (
    commonArgs
    // {
      cargoArtifacts = cargoArtifactsStable;
      nativeBuildInputs = [ pkgs.cargo-nextest ];
      buildPhaseCargoCommand = ''
        cargo build --locked --workspace --all-targets --all-features
        cargo nextest run --locked --workspace --all-features --lib --bins --tests --examples
        # Custom benchmark harnesses are not compatible with nextest's test listing.
        cargo test --locked --workspace --all-features --bench '*'
        cargo test --locked --workspace --doc --all-features
        cargo run --locked -p tokio-otp --example trading_engine --features metrics
        cargo run --locked -p tokio-otp --example agent_control --features metrics
        cargo run --locked -p tokio-otp --example supervised_actors
        cargo run --locked -p tokio-otp --example ref_rebind
        cargo run --locked -p tokio-otp --example drain_policy
        RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features
      '';
      doInstallCargoArtifacts = false;
    }
  );
}
