#!/usr/bin/env python3
import sys
import argparse

# Make sure local SDK is importable
sys.path.insert(0, "/data/users/her2/trino-shiny-demo/dmap-data-sdk/src/")

from dmap_data_sdk.data_utils import PlatformFactory


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read a DMAP dataset from the local catalog via Spark."
    )

    # Required positional args
    parser.add_argument(
        "-r",
        "--rid",
        default="ri.foundry.main.dataset.ffe70086-f96c-4c24-bfc9-c0e86c504068",
        help="RID to read from the DMAP catalog (e.g. HER2_PROT_001)",
    )
    parser.add_argument(
        "-b",
        "--branch",
        default="master",
        help="Branch name in the DMAP catalog (e.g. master, dev, snapshot_2024_11_01)",
    )

    # Optional arg with default
    parser.add_argument(
        "-c",
        "--config",
        default="/data/foundry_mirror_items/resource_yellowpage/dmap_util_config.json",
        help="Path to DMAP config.json (default: %(default)s)",
    )

    args = parser.parse_args()

    # Build DMAP platform (Spark session auto-created when spark=None)
    platform = PlatformFactory.dmap_prod(
        spark=None,
        config_path=args.config
    )

    # Read and show data for the given RID + branch
    df = platform.read_table(args.rid, branch=args.branch)
    df.show()


if __name__ == "__main__":
    main()
