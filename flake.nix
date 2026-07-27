{
  description = "tokio-otp development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane = {
      url = "github:ipetkov/crane";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      crane,
      rust-overlay,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        toolchainConfig = builtins.fromTOML (builtins.readFile ./rust-toolchain.toml);
        stableChannel = toolchainConfig.toolchain.channel;
        rustHost = pkgs.stdenv.hostPlatform.rust.rustcTarget;
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        nightlyToolchain = pkgs.rust-bin.selectLatestNightlyWith (
          toolchain:
          toolchain.default.override {
            extensions = [
              "clippy"
              "rustfmt"
            ];
          }
        );
        rustupHome = pkgs.runCommandLocal "tokio-otp-rustup-home" { } ''
          mkdir -p $out/toolchains $out/update-hashes
          ln -s ${rustToolchain} $out/toolchains/${stableChannel}-${rustHost}
          ln -s ${nightlyToolchain} $out/toolchains/nightly-${rustHost}
          printf 'version = "12"\n\n[overrides]\n' > $out/settings.toml
        '';
        cargoChecks = import ./nix/crane-checks.nix {
          inherit
            pkgs
            crane
            rustToolchain
            nightlyToolchain
            ;
          src = ./.;
        };
      in
      {
        formatter = pkgs.nixfmt;

        checks = {
          nixfmt = pkgs.runCommandLocal "nixfmt-check" { nativeBuildInputs = [ pkgs.nixfmt ]; } ''
            nixfmt --check ${./flake.nix}
            nixfmt --check ${./nix/crane-checks.nix}
            touch $out
          '';
          book = pkgs.runCommandLocal "mdbook-build" { nativeBuildInputs = [ pkgs.mdbook ]; } ''
            mdbook build ${./docs} --dest-dir $out
          '';
        }
        // cargoChecks;

        devShells.default = pkgs.mkShell {
          RUSTUP_HOME = rustupHome;
          RUSTUP_TOOLCHAIN = stableChannel;
          packages = with pkgs; [
            rustup
            cargo-nextest
            git
            just
            jq
            mdbook
            nixfmt
            ripgrep
          ];
        };
      }
    );
}
