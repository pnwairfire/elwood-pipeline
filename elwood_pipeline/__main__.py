"""CLI entrypoint: run a single Elwood pipeline end-to-end and log to stdout.

    python -m elwood_pipeline <stream>
    elwood-pipeline <stream>
"""
import argparse
import logging
import sys

from elwood_pipeline import (
    maintain_data_feed,
    elwood_outliers,
    state_management,
)

REGISTRY = {
    "maintain-data-feed": maintain_data_feed,
    "outliers": elwood_outliers,
    "state-management": state_management,
}


def main(argv=None):
    parser = argparse.ArgumentParser(prog="elwood-pipeline", description=__doc__)
    parser.add_argument(
        "stream", choices=sorted(REGISTRY), help="which pipeline to run"
    )
    parser.add_argument(
        "--log-level", default="INFO",
        help="stdlib logging level (default: INFO)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("elwood_pipeline")

    module = REGISTRY[args.stream]
    log.info(f"Starting pipeline: {args.stream}")
    summary = module.run()
    log.info(f"Finished pipeline: {args.stream} — {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
