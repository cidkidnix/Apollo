let
  lib = import "${import ./dep/nixpkgs/thunk.nix}/lib/default.nix";
  packageImport = args:
    import ./dep/nixpkgs/default.nix args;

  makePackage = { pkgs, static ? false }: pkgs.buildGoModule {
    name = "Apollo";
    src = builtins.fetchGit ./.;


    CGO_ENABLED = if static then 0 else 1;
    vendorHash = "sha256-4DNIYRYsO7u0oGDKAONS2/z6JUzFHZiH7H0Fx99lS8M=";

    meta = with lib; {
      description = "A Canton ODS implementation in Go";
      platforms = platforms.all;
      #license = license.mit;
    };
  };


  makeDockerImage = { pkgs }: pkgs.dockerTools.buildImage {
    name = "Apollo";
    copyToRoot = [
      (makePackage { inherit pkgs; })
      pkgs.cacert
    ];
    config = {
      Entrypoint = [ "/bin/Apollo" ];
    };
  };

in {
  linux = {
    docker = {
      amd64 = makeDockerImage {
        pkgs = packageImport { system = "x86_64-linux"; };
      };
      aarch64 = makeDockerImage {
        pkgs = packageImport { system = "aarch64-linux"; };
      };
    };
    static = {
      amd64 = makePackage {
        static = true;
        pkgs = packageImport {
          system = "x86_64-linux";
        };
      };
      aarch64 = makePackage {
        static = true;
        pkgs = packageImport {
          system = "aarch64-linux";
        };
      };
    };
    amd64 = makePackage {
      pkgs = packageImport {
        system = "x86_64-linux";
      };
    };
    aarch64 = makePackage {
      pkgs = packageImport {
        system = "aarch64-linux";
      };
    };
  };

  macos = {
    amd64 = makePackage {
      pkgs = packageImport {
        system = "x86_64-darwin";
      };
    };
    aarch64 = makePackage {
      pkgs = packageImport {
        system = "aarch64-darwin";
      };
    };
  };

  windows = {
    amd64 = makePackage {
      pkgs = packageImport {
        crossSystem = lib.systems.examples.mingwW64;
      };
    };
  };
}
