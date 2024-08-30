{
  description = "A Nix Flake for a poetry environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable"; # follow the unstable branch
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
      };
    in {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [
          pkgs.python311
          (pkgs.poetry.override { python3 = pkgs.python311; })
        ];
      };
    };
}
