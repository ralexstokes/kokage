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
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        nightlyToolchain = pkgs.rust-bin.nightly.latest.default.override {
          extensions = [
            "clippy"
            "rustfmt"
          ];
        };
        nightlyCargo = pkgs.writeShellScriptBin "cargo-nightly" ''
          export PATH=${nightlyToolchain}/bin:$PATH
          exec ${nightlyToolchain}/bin/cargo "$@"
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
          NIGHTLY_CARGO = "${nightlyCargo}/bin/cargo-nightly";
          packages = with pkgs; [
            git
            just
            mdbook
            nixfmt
            ripgrep
            rustToolchain
            nightlyToolchain
            rustup
          ];
        };
      }
    );
}
