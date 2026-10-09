{
  description = "Dev environment with supporting tools";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

      in {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            # Keep in sync with version in CI (build.yaml)
            pkgs.foundry
            # NOTE: Version must match one in tests/contracts/foundry.toml
            # And tests/contracts/.env forces foundry to use pre-installed binary instead of downloading
            pkgs.solc
          ];
        };
      });
}
