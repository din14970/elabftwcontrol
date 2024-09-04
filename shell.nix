{ pkgs ? import <nixpkgs> {} }:

let
  poetry2nix = pkgs.callPackage (pkgs.fetchgit {
    url = "https://github.com/nix-community/poetry2nix.git";
    rev = "7619e43c2b48c29e24b88a415256f09df96ec276"; # Replace this with the correct sha256 hash
    sha256 = "sha256-gD0N0pnKxWJcKtbetlkKOIumS0Zovgxx/nMfOIJIzoI=";
  }) {};

  mkPoetryEnv = poetry2nix.mkPoetryEnv;
in
pkgs.mkShell {
  buildInputs = [
    (mkPoetryEnv {
      projectDir = ./.;
      checkGroups = [ "dev" "test" "lsp" ];
      editablePackageSources = {
        elabftwcontrol = ./src;
      };
      preferWheels = true;
    })
    pkgs.poetry
  ];
}
