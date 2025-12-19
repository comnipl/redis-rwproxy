{
  nixConfig = {
    extra-substituters = [
      "https://nix-community.cachix.org"
    ];
    extra-trusted-public-keys = [
      "nix-community.cachix.org-1:mB9FSh9qf2dCimDSUo8Zy7bkq5CX+/rkCWyvRCYg3Fs="
    ];
  };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    fenix.url = "github:nix-community/fenix";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
      crane,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        rustToolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-SDu4snEWjuZU475PERvu+iO50Mi39KVjqCeJeNvpguU=";
        };

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = craneLib.cleanCargoSource ./.;
        commonArgs = {
          inherit src;
          strictDeps = true;
          buildInputs = [ ];
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        crate = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
          }
        );
      in
      {

        formatter = pkgs.writeShellApplication {
          name = "redis-rwproxy-formatter";
          runtimeInputs = with pkgs; [
            nixfmt-rfc-style
            rustToolchain
          ];
          text = ''
            fd "$@" -t f -e nix -x nixfmt '{}'
            cargo fmt
          '';
        };

        checks = {
          inherit crate;

          nix-fmt =
            pkgs.runCommand "check-nix-formatting"
              {
                buildInputs = with pkgs; [
                  nixfmt-rfc-style
                  fd
                  gitMinimal
                ];
                src = self;
              }
              ''
                cp -r --no-preserve=mode $src src
                cd src
                git init --quiet && git add .
                fd "$@" -t f -e nix -x nixfmt '{}'
                if ! git diff --exit-code; then
                  exit 1
                fi
                touch $out
              '';

          crate-clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets -- --deny warnings";
            }
          );

          crate-test = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
            }
          );

          crate-fmt = craneLib.cargoFmt {
            inherit src;
          };
        };

        devShells.default = pkgs.mkShell {
          name = "redis-rwproxy-dev";
          buildInputs = [
            rustToolchain
          ];
        };

        packages.default = crate;

        apps.default = flake-utils.lib.mkApp {
          drv = crate;
        };

        packages.dockerImage = pkgs.dockerTools.buildLayeredImage {
          name = "redis-rwproxy";
          tag = "latest";
          contents = [ crate ];
          config = {
            Entrypoint = [ "/bin/redis-rwproxy" ];
          };
        };
      }
    );
}
