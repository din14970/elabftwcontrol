{
  description = "Application packaged using poetry2nix";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable-small";
    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, poetry2nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        # see https://github.com/nix-community/poetry2nix/tree/master#api for more functions and examples.
        pkgs = nixpkgs.legacyPackages.${system};
        inherit (poetry2nix.lib.mkPoetry2Nix { inherit pkgs; }) mkPoetryApplication;
        inherit (poetry2nix.lib.mkPoetry2Nix { inherit pkgs; }) mkPoetryEnv;
      in
      {
        packages = {
          elabftwcontrol = mkPoetryApplication {
            projectDir = ./.;
            checkGroups = [ "dev" "test" "lsp" ];
            preferWheels = true;
            # editablePackageSources = {
            #   myapp = ./src;
            # };
          };
          default = self.packages.${system}.elabftwcontrol;
        };

        # Shell for app dependencies.
        #
        #     nix develop
        #
        # Use this shell for developing your app.

        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.devShells.${system}.pythonEnv.env ];
          #inputsFrom = [ self.packages.${system}.default ];
        };

        devShells.pythonEnv = mkPoetryEnv {
          projectDir = ./.;
          checkGroups = [ "dev" "test" "lsp" ];
          editablePackageSources = {
            elabftwcontrol = "${builtins.getEnv "PWD"}/src";
          };
          preferWheels = true;
          # overrides = poetry2nix.overrides.withDefaults (final: prev: {
          #   pyarrow = prev.pyarrow.override {
          #     preferWheel = true;
          #   };
          # });
        };


        # Shell for poetry.
        #
        #     nix develop .#poetry
        #
        # Use this shell for changes to pyproject.toml and poetry.lock.
        devShells.poetry = pkgs.mkShell {
          packages = [ pkgs.poetry ];
        };
      });
}
