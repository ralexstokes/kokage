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
      || pkgs.lib.hasInfix "/docs/" (toString path)
      || pkgs.lib.hasSuffix "/README.md" (toString path)
      || pkgs.lib.hasSuffix ".stderr" (toString path)
      || pkgs.lib.hasSuffix "/assets/index.html" (toString path)
      || pkgs.lib.hasSuffix "/scripts/check-public-api.sh" (toString path)
      || pkgs.lib.hasSuffix "/scripts/test-docs.sh" (toString path)
      || pkgs.lib.hasSuffix "/scripts/public-api-paths.jq" (toString path);
  };
  commonArgs = {
    CARGO_PROFILE = "";
    pname = "kokage";
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
      nativeBuildInputs = [
        pkgs.cargo-nextest
        pkgs.jq
        pkgs.mdbook
      ];
      buildPhaseCargoCommand = ''
        cargo build --locked --workspace --all-targets --all-features
        cargo nextest run --locked --workspace --all-features --lib --bins --tests --examples
        # Custom benchmark harnesses are not compatible with nextest's test listing.
        cargo test --locked --workspace --all-features --bench '*'
        cargo test --locked --workspace --doc --all-features
        cargo run --locked -p kokage --example trading_engine --features metrics,derive
        cargo run --locked -p kokage --example agent_control --features metrics,derive
        RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features
        bash scripts/test-docs.sh
      '';
      doInstallCargoArtifacts = false;
    }
  );

  public-api = craneLibNightly.mkCargoDerivation (
    commonArgs
    // {
      cargoArtifacts = cargoArtifactsNightly;
      nativeBuildInputs = [ pkgs.jq ];
      KOKAGE_RUSTDOC_TOOLCHAIN = "";
      buildPhaseCargoCommand = "bash scripts/check-public-api.sh";
      doInstallCargoArtifacts = false;
    }
  );
}
