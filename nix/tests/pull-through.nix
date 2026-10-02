{ pkgs, self }:
let
  # Only the upstream has these; the harmonia node pulls them on demand.
  dep = pkgs.writeText "pull-through-dep" "dependency";
  target = pkgs.writeText "pull-through-target" "pulled through ${dep}";
  gcTarget = pkgs.writeText "pull-through-gc" "survives a gc between narinfo and nar";
  hashPart = pkg: builtins.substring (builtins.stringLength builtins.storeDir + 1) 32 pkg.outPath;
  publicKey = pkgs.lib.fileContents ../../tests/cache.pk;
  unknownHash = "00000000000000000000000000000000";
in
pkgs.testers.nixosTest {
  name = "pull-through";

  nodes = {
    upstream =
      { ... }:
      {
        services.nginx = {
          enable = true;
          virtualHosts.upstream = {
            default = true;
            root = "/srv/cache";
            extraConfig = "access_log /var/log/nginx/upstream.log;";
          };
        };
        networking.firewall.allowedTCPPorts = [ 80 ];
        nix.extraOptions = "experimental-features = nix-command";
        system.extraDependencies = [
          target
          gcTarget
        ];
      };

    harmonia =
      { lib, ... }:
      {
        imports = [ self.nixosModules.harmonia ];

        services.harmonia-dev.cache.enable = true;
        services.harmonia-dev.cache.settings.pull_through = {
          enable = true;
          upstreams = [ "http://upstream" ];
          negative_ttl = "1h";
        };

        # The daemon does the substituting, from its own configuration.
        nix.settings.substituters = lib.mkForce [ "http://upstream" ];
        nix.settings.trusted-public-keys = [ publicKey ];
        nix.extraOptions = "experimental-features = nix-command";

        networking.firewall.allowedTCPPorts = [ 5000 ];
      };

    client01 =
      { lib, ... }:
      {
        nix.settings.substituters = lib.mkForce [ "http://harmonia:5000" ];
        # harmonia has no key of its own here: this checks that the
        # upstream's signature comes through.
        nix.settings.trusted-public-keys = [ publicKey ];
        nix.extraOptions = "experimental-features = nix-command";
      };
  };

  testScript = ''
    start_all()

    upstream.wait_for_unit("nginx.service")
    upstream.succeed(
        "nix copy --to 'file:///srv/cache?secret-key=${../../tests/cache.sk}' ${target} ${gcTarget}"
    )
    client01.wait_until_succeeds("curl -f http://upstream/nix-cache-info")

    harmonia.wait_for_unit("harmonia-dev.socket")
    harmonia.fail("nix-store --check-validity ${target}")
    harmonia.fail("nix-store --check-validity ${dep}")
    client01.wait_until_succeeds("timeout 1 curl -f http://harmonia:5000/nix-cache-info")

    with subtest("a closure harmonia lacks is pulled through its daemon"):
        client01.succeed("nix copy --from http://harmonia:5000 ${target}")
        client01.succeed("grep 'pulled through' ${target}")
        harmonia.succeed("nix-store --check-validity ${target} ${dep}")

    with subtest("the upstream signature is preserved"):
        narinfo = client01.succeed("curl -f http://harmonia:5000/${hashPart target}.narinfo")
        print(narinfo)
        assert "Sig: cache.example.com-1:" in narinfo, "upstream signature missing"

    with subtest("unknown hashes 404 with one upstream lookup per negative_ttl"):
        for _ in range(3):
            client01.fail("curl -f http://harmonia:5000/${unknownHash}.narinfo")
        count = upstream.succeed("grep -c '/${unknownHash}.narinfo' /var/log/nginx/upstream.log").strip()
        assert count == "1", f"expected 1 upstream lookup, got {count}"

    with subtest("a gc between the narinfo and nar requests doesn't lose the path"):
        narinfo = client01.succeed("curl -f http://harmonia:5000/${hashPart gcTarget}.narinfo")
        harmonia.succeed("nix-collect-garbage")
        harmonia.succeed("nix-store --check-validity ${gcTarget}")
        url = next(l[len("URL: "):] for l in narinfo.splitlines() if l.startswith("URL: "))
        client01.succeed(f"curl -f -o /dev/null http://harmonia:5000/{url}")

    metrics = client01.succeed("curl -f http://harmonia:5000/metrics")
    print(metrics)
    assert 'harmonia_pull_through_requests_total{result="pulled"}' in metrics
  '';
}
