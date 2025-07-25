import argparse
import logging
from datetime import datetime

from company import (
    migrate_buildings,
    migrate_companies,
    migrate_company_types,
    migrate_operational_offices,
    migrate_physical_structures,
)
from location import (
    migrate_districts,
    migrate_municipalities,
    migrate_provinces,
    migrate_regions,
    migrate_toponyms,
    migrate_ulss,
)
from resolution import migrate_resolution_types, migrate_resolutions
from specialty import (
    migrate_grouping_specialties,
    migrate_specialties,
)
from udo import (
    migrate_operational_units,
    migrate_production_factor_types,
    migrate_production_factors,
    migrate_udo_production_factors,
    migrate_udo_resolutions,
    migrate_udo_specialties_from_branches,
    migrate_udo_specialties_from_disciplines,
    migrate_udo_type_classifications,
    migrate_udo_type_production_factor_types,
    migrate_udo_types,
    migrate_udos,
    migrate_udos_history,
)
from user import migrate_user_companies, migrate_users
from utils import (
    export_tables_to_csv,
    format_elapsed_time,
    setup_connections,
    setup_logging,
    truncate_postgresql_tables,
)


def parse_args():
    """
    Parse command-line arguments for the ETL process.

    Sets up an argument parser with options for exporting to CSV and specifying
    the export directory.

    Returns
    -------
    argparse.Namespace
        The parsed command-line arguments
    """
    parser = argparse.ArgumentParser(description="A.Re.A. ETL process")
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export all PostgreSQL tables to CSV files",
    )
    parser.add_argument(
        "--export-dir",
        type=str,
        default="export",
        help="Directory where CSV files will be saved (default: 'export')",
    )
    return parser.parse_args()


def main() -> None:
    """
    Execute the A.Re.A. ETL process.

    This is the main entry point for the ETL process. It parses command-line arguments,
    sets up logging, and either exports tables to CSV or runs the full ETL process,
    which includes migrating data from various sources to PostgreSQL tables.

    The function measures and logs the total execution time and handles any exceptions
    that occur during the process.
    """
    args = parse_args()
    setup_logging()
    start_time = datetime.now()

    try:
        ctx = setup_connections()

        if args.export_csv:
            logging.info("Starting CSV export process...")
            export_tables_to_csv(ctx, args.export_dir)
            elapsed_time = format_elapsed_time(start_time)
            logging.info(f"Total export time: {elapsed_time}")
            logging.info("CSV export completed successfully")
            return

        # Regular ETL process
        logging.info("Starting A.Re.A. ETL process...")
        truncate_postgresql_tables(ctx)
        migrate_regions(ctx)
        migrate_provinces(ctx)
        migrate_municipalities(ctx)
        migrate_toponyms(ctx)
        migrate_districts(ctx)
        migrate_ulss(ctx)
        migrate_company_types(ctx)
        migrate_companies(ctx)
        migrate_physical_structures(ctx)
        migrate_operational_offices(ctx)
        migrate_buildings(ctx)
        migrate_operational_units(ctx)
        migrate_grouping_specialties(ctx)
        migrate_specialties(ctx)
        migrate_production_factor_types(ctx)
        migrate_production_factors(ctx)
        migrate_udo_type_classifications(ctx)
        migrate_udo_types(ctx)
        migrate_udos(ctx)
        migrate_udo_production_factors(ctx)
        migrate_udo_type_production_factor_types(ctx)
        migrate_udo_specialties_from_branches(ctx)
        migrate_udo_specialties_from_disciplines(ctx)
        migrate_udo_resolutions(ctx)
        migrate_udos_history(ctx)
        migrate_resolution_types(ctx)
        migrate_resolutions(ctx)
        migrate_users(ctx)
        migrate_user_companies(ctx)

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
