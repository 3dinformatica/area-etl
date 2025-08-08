import argparse
import logging
from datetime import datetime

from auac import migrate_auac
from core import migrate_core
from cronos import migrate_cronos
from poa import migrate_poa
from ppf import migrate_ppf
from utils import (
    format_elapsed_time,
    setup_connections,
    setup_logging,
)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the ETL process.

    Returns
    -------
    argparse.Namespace
        The parsed command-line arguments
    """
    parser = argparse.ArgumentParser(description="A.Re.A. ETL process")
    parser.add_argument(
        "--modules",
        type=str,
        default="all",
        help="Modules to migrate: 'all', 'core', 'poa', 'cronos', 'auac', 'ppf', or a comma-separated list (default: 'all')",
    )
    return parser.parse_args()


def main() -> None:
    """Execute the A.Re.A. ETL process."""
    args = parse_args()
    setup_logging()
    start_time = datetime.now()

    try:
        ctx = setup_connections()

        logging.info("Starting A.Re.A. ETL process...")
        modules_to_migrate = args.modules.lower().split(",")

        if "all" in modules_to_migrate:
            migrate_core(ctx)
            migrate_poa(ctx)
            migrate_cronos(ctx)
            migrate_auac(ctx)
            migrate_ppf(ctx)
        else:
            if "core" in modules_to_migrate:
                migrate_core(ctx)
            if "poa" in modules_to_migrate:
                migrate_poa(ctx)
            if "cronos" in modules_to_migrate:
                migrate_cronos(ctx)
            if "auac" in modules_to_migrate:
                migrate_auac(ctx)
            if "ppf" in modules_to_migrate:
                migrate_ppf(ctx)

        elapsed_time = format_elapsed_time(start_time)
        logging.info(f"Total migration time: {elapsed_time}")
        logging.info("ETL process completed successfully")
    except Exception as e:
        elapsed_time = format_elapsed_time(start_time)
        logging.error(
            f"Error during execution after {elapsed_time}: {e!s}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    main()
