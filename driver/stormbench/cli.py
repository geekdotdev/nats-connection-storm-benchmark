# SPDX-License-Identifier: Apache-2.0
"""Command line for the driver (`uv run bench ...`). The `images` and `keys` commands work;
the rest are placeholders. `cluster` works for --target local."""
from __future__ import annotations

import argparse
import subprocess
import sys

from pathlib import Path

from . import config, eks, images, k8s, kind, registry, report, runcmd, secrets_cmd, state
from .ab import StormFailed
from .runner import Runner

PLANNED = {
    "ci": "dispatch the GitHub workflow and fetch its result artifacts",
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bench", description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="print the commands without running them")
    p.add_argument("--target", choices=k8s.TARGETS, default="local",
                   help="which cluster `cluster`, `secrets` and `run` act on: local (kind) or eks. Each has its own "
                        "kubeconfig file. May also follow the subcommand: `bench secrets create --target eks`")
    # The same option after the subcommand. SUPPRESS: when it is not given there, the value given before it stands.
    on_target = argparse.ArgumentParser(add_help=False)
    on_target.add_argument("--target", choices=k8s.TARGETS, default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("keys", help="show which API keys are set (names only, never values)")

    im = sub.add_parser("images", help="build, load into kind, or push images")
    isub = im.add_subparsers(dest="action", required=True)
    b = isub.add_parser("build", help="build the broker image for a commit, and the other images")
    b.add_argument("ref", nargs="?", help="full 40 character commit sha of nats-server (not needed with --only callout,loadgen,provision,cpuprobe)")
    b.add_argument("--only", help="build only these images, comma separated: nats-server, callout, loadgen, provision, cpuprobe")
    b.add_argument("--repo", default=images.DEFAULT_REPO, help="https URL of the nats-server repository")
    b.add_argument("--ref-name", help="branch or tag name to record on the broker image: a label and a second tag (the commit stays the identity)")
    b.add_argument("--platform", help="linux/amd64, linux/arm64, or both comma separated (both needs podman); default is this machine's")
    k = isub.add_parser("load-kind", help="load images into the kind cluster")
    k.add_argument("images", nargs="+")
    pu = isub.add_parser("push", help="push images to REGISTRY (needs REGISTRY, REGISTRY_USER, REGISTRY_TOKEN)")
    pu.add_argument("images", nargs="+")
    pu.add_argument("--insecure", action="store_true", help="skip TLS verification; only for a test registry on localhost")

    cl = sub.add_parser("cluster", parents=[on_target], help="create or delete the cluster (local = kind, eks = eksctl), or print the EKS config")
    cl.add_argument("action", choices=["up", "down", "config", "addons", "infra"], help="config prints the eksctl ClusterConfig (eks only) and touches nothing; "
                    "addons installs the EBS CSI driver and its IAM role on an existing eks cluster (needs --yes); "
                    "infra adds the infra node group (for Prometheus) to an existing eks cluster (needs --yes)")
    cl.add_argument("--yes", action="store_true", help="eks up: confirm that you accept the AWS charges")
    cl.add_argument("--region", help="eks: AWS region (default: AWS_REGION)")
    cl.add_argument("--zone", help="eks: availability zone for the nodes (default: <region>a)")
    cl.add_argument("--instance-type", help="eks: node instance type (default c6i.xlarge; the overlays' sizes assume it)")
    cl.add_argument("--broker-nodes", type=int, help="eks: nodes in the brokers group (default 3; one broker per node)")
    cl.add_argument("--client-nodes", type=int, help="eks: nodes in the clients group (default 1)")
    cl.add_argument("--infra-nodes", type=int, help="eks: nodes in the infra group, for Prometheus and its volume (default 1; 0 for none)")
    cl.add_argument("--infra-instance-type", help="eks: instance type of the infra nodes (default c6i.large)")
    cl.add_argument("--k8s-version", help="eks: Kubernetes version, e.g. 1.36 (default: pinned in the driver, see eks.DEFAULT_K8S_VERSION)")

    se = sub.add_parser("secrets", parents=[on_target], help="create the TLS Secret (never written to disk) or delete it and what the provision init container made, in the target's cluster")
    se.add_argument("action", choices=["create", "delete"])

    ru = sub.add_parser("run", parents=[on_target], help="storm two or more variants on the target's cluster: blocks in rotating order, one fresh broker per variant per block")
    ru.add_argument("--a", help="full commit sha of variant A (its image must be built and loaded, or pushed)")
    ru.add_argument("--b", help="full commit sha of variant B")
    ru.add_argument("--a-label", default="a", help="variant A's name in file names and the report")
    ru.add_argument("--b-label", default="b")
    ru.add_argument("--variants", help="TOML file naming two or more variants (docs/variants.example.toml), instead of --a and --b")
    ru.add_argument("--blocks", type=int, help="blocks (default 8)")
    ru.add_argument("--sizes", help="storm sizes, comma separated (default 800,1600)")
    ru.add_argument("--threads", type=int, help="generator worker threads (default 4)")
    ru.add_argument("--settle-s", type=float, help="wait after a broker restart before its first storm (default 8)")
    ru.add_argument("--image-prefix", help="prefix of the broker image names in the cluster (default: localhost/ for local, "
                                           "REGISTRY/ for eks)")
    ru.add_argument("--stack-overlay", help="kustomize overlay to apply (default: deploy/stack/overlays/<target>; "
                                            "for a cluster, e.g. deploy/stack/overlays/eks-cluster)")
    ru.add_argument("--loadgen-overlay", help="kustomize overlay of the storm Job (default: deploy/loadgen/overlays/<target>)")
    ru.add_argument("--no-cpu", action="store_true", help="do not read the broker's and callout's CPU use around each storm (about 2 kubectl calls "
                    "before and after each; the generator reports its own regardless)")
    ru.add_argument("--no-monitoring", action="store_true", help="ignore the CPU probes and Prometheus even if they are installed (`bench monitoring up`)")
    ru.add_argument("--no-preflight", action="store_true", help="skip the checks made before anything is applied (see `bench preflight`)")
    pf = sub.add_parser("preflight", parents=[on_target], help="check the cluster, Secrets, node placement and (eks) the registry images a run needs; changes nothing")
    pf.add_argument("--a", help="full commit sha of variant A")
    pf.add_argument("--b", help="full commit sha of variant B")
    pf.add_argument("--a-label", default="a")
    pf.add_argument("--b-label", default="b")
    pf.add_argument("--variants", help="TOML file naming two or more variants, as for `run`")
    pf.add_argument("--image-prefix", help="as for `run`")
    pf.add_argument("--stack-overlay", help="as for `run`")
    pf.add_argument("--loadgen-overlay", help="as for `run`")
    ru.add_argument("--name", help="results directory is results/<name>-<date> (default ab)")
    ru.add_argument("--out", help="results directory (overrides --name)")
    ru.add_argument("--resume", action="store_true", help="continue the run recorded in the cluster (see `bench status`): its plan comes from "
                    "the record, so no plan option may be given; finished units are skipped and an unfinished one is redone from a fresh broker")
    ru.add_argument("--discard-state", action="store_true", help="forget an unfinished recorded run and start this one (its measurements stay)")
    ru.add_argument("--takeover", action="store_true", help="go on although another driver wrote to the run in the last 5 minutes "
                    "(it was killed; a driver that was stopped normally has handed the run back)")
    ru.add_argument("--accept-design-change", action="store_true", help="with --resume: go on although this driver's phases or pauses differ from the ones the run started with")
    mo = sub.add_parser("monitoring", parents=[on_target], help="the CPU probes and Prometheus (the series on an EBS volume on eks): install, remove, show, "
                        "or take a run's series out of Prometheus")
    mo.add_argument("action", choices=["up", "down", "status", "export"])
    mo.add_argument("dir", nargs="?", help="export: the results directory to write the series into (prometheus/ inside it)")
    mo.add_argument("--no-preflight", action="store_true", help="up: do not check the nodes, the StorageClass and the images first")
    mo.add_argument("--purge", action="store_true", help="down: also delete the claim, and so the volume and the data on it")
    ad = sub.add_parser("adopt", parents=[on_target], help="record a results directory that has no run state, so `run --resume` can continue it "
                        "(optionally running chosen units again)")
    ad.add_argument("dir", help="a results directory made by `bench run`, whose run has ended")
    ad.add_argument("--redo", action="append", metavar="UNIT", help="run this unit again (<variant>-b<block>, e.g. main-b4); repeatable. Its files "
                    "are moved to DIR/discarded/, not deleted")
    ad.add_argument("--redo-failed", action="store_true", help="run again every unit with a burst in which not every connection was admitted")
    ad.add_argument("--reason", help="why, recorded in environment.json next to the units run again")
    ad.add_argument("--stack-overlay", help="the overlay the run used, if not the target's default")
    ad.add_argument("--loadgen-overlay", help="the storm Job's overlay, if not the target's default")
    ad.add_argument("--variants-file", help="the variants file that made the run: saved in DIR as variants.toml, with its SHA-256 recorded")
    ad.add_argument("--force", action="store_true", help="adopt although raw/DONE is missing (the run's driver was killed)")
    ad.add_argument("--discard-state", action="store_true", help="forget an unfinished run recorded in the cluster")
    ad.add_argument("--takeover", action="store_true", help="as for `run`")
    sub.add_parser("status", parents=[on_target], help="show the progress of the run recorded in the target's cluster; changes nothing")
    rp = sub.add_parser("report", help="summarise a results directory (exact permutation tests on block medians)")
    rp.add_argument("dir")

    for name, text in PLANNED.items():
        sub.add_parser(name, help=f"(not implemented) {text}")
    return p


def main(argv: list[str] | None = None) -> int:
    config.load_dotenv()
    args = build_parser().parse_args(argv)
    run = Runner(dry_run=args.dry_run)
    try:
        if args.cmd == "keys":
            rows = config.describe()
            width = max(len(n) for n, _, _ in rows)
            for name, is_set, needed_by in rows:
                print(f"{name:<{width}}  {'set    ' if is_set else 'not set'}  {needed_by}")
            return 0
        if args.cmd == "images":
            if args.action == "build":
                only = tuple(n.strip() for n in args.only.split(",")) if args.only else ()
                if args.ref is None and (not only or "nats-server" in only):
                    print("bench: images build needs the commit sha unless --only leaves nats-server out", file=sys.stderr)
                    return 2
                images.build(run, args.ref, args.repo, images.parse_platforms(args.platform), args.ref_name, only)
            elif args.action == "load-kind":
                kind.load(run, args.images)
            else:
                for ref in registry.push(run, args.images, args.insecure):
                    print(ref)  # deploy by digest, so a tag can never change under a run
            return 0
        if args.cmd == "cluster":
            if args.target == "eks":
                params = eks.params_from_env(region=args.region, zone=args.zone, instance_type=args.instance_type,
                                             broker_nodes=args.broker_nodes, client_nodes=args.client_nodes,
                                             infra_nodes=args.infra_nodes, infra_instance_type=args.infra_instance_type,
                                             k8s_version=args.k8s_version)
                if args.action == "config":
                    print(eks.render(params), end="")
                elif args.action == "up":
                    kc = eks.up(run, params, args.yes)
                    if not args.dry_run:
                        print(f"kubeconfig: {kc}\nsecrets and run use it with --target eks")
                elif args.action == "addons":
                    eks.add_addons(run, params, args.yes)
                elif args.action == "infra":
                    eks.add_infra(run, params, args.yes)
                else:
                    eks.down(run, params)
                return 0
            if args.action in ("config", "addons", "infra"):
                print(f"bench cluster {args.action}: only for --target eks", file=sys.stderr)
                return 2
            (kind.up if args.action == "up" else kind.down)(run)
            return 0
        if args.cmd == "secrets":
            if args.action == "create":
                names = secrets_cmd.create(run, args.target)
                print("created Secret " + ", ".join(names) + f" in namespace {config.NAMESPACE} (values not shown)")
            else:
                secrets_cmd.delete(run, args.target)
            return 0
        if args.cmd == "run":
            return runcmd.execute(run, args)
        if args.cmd == "preflight":
            return runcmd.execute_preflight(run, args)
        if args.cmd == "status":
            return runcmd.execute_status(run, args)
        if args.cmd == "adopt":
            return runcmd.execute_adopt(run, args)
        if args.cmd == "monitoring":
            return runcmd.execute_monitoring(run, args)
        if args.cmd == "report":
            text = report.write(Path(args.dir))
            print(text)
            return 0
    except eks.NotConfirmed as e:
        print(f"bench: {e}", file=sys.stderr)
        return 2
    except StormFailed as e:
        print(f"bench: {e}", file=sys.stderr)
        return 1
    except k8s.NoCluster as e:
        print(f"bench: {e}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as e:
        print(f"bench: {e.cmd[0]} failed (exit {e.returncode})", file=sys.stderr)
        if args.cmd == "secrets" and args.action == "create":
            print("bench: if the Secret already exists, run `bench secrets delete` first and restart the pods afterwards", file=sys.stderr)
        return 1
    except (state.StateError, state.LeaseHeld) as e:
        print(f"bench: {e}", file=sys.stderr)
        return 1
    except config.MissingSecret as e:
        print(f"bench: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"bench: {e}", file=sys.stderr)
        return 2
    print(f"bench {args.cmd}: not implemented yet", file=sys.stderr)
    return 2
