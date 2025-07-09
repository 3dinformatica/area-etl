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
from core import (
    export_tables_to_csv,
    setup_connections,
    setup_logging,
    truncate_postgresql_tables,
)
from location import (
    migrate_municipalities,
    migrate_provinces,
    migrate_regions,
    migrate_toponyms,
)
from resolution import migrate_resolution_types, migrate_resolutions
from specialty import (
    migrate_grouping_specialties,
    migrate_specialties,
)
from udo import (
    migrate_production_factor_types,
    migrate_production_factors,
    migrate_udo_production_factors,
    migrate_udo_resolutions,
    migrate_udo_specialties_from_branches,
    migrate_udo_specialties_from_disciplines,
    migrate_udo_status_history,
    migrate_udo_type_production_factor_types,
    migrate_udo_types,
    migrate_udos,
    migrate_operational_units,
)


def parse_args():
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
    args = parse_args()
    setup_logging()
    start_time = datetime.now()

    try:
        ctx = setup_connections()

        if args.export_csv:
            logging.info("Starting CSV export process...")
            export_tables_to_csv(ctx, args.export_dir)
            end_time = datetime.now()
            elapsed_time = end_time - start_time
            hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            logging.info(f"Total export time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")
            logging.info("CSV export completed successfully")
            return

        # Regular ETL process
        logging.info("Starting A.Re.A. ETL process...")
        truncate_postgresql_tables(ctx)
        migrate_regions(ctx)
        migrate_provinces(ctx)
        migrate_municipalities(ctx)
        migrate_toponyms(ctx)
        migrate_resolution_types(ctx)
        migrate_resolutions(ctx)
        migrate_company_types(ctx)
        migrate_companies(ctx)
        migrate_physical_structures(ctx)
        migrate_operational_offices(ctx)
        migrate_buildings(ctx)
        migrate_grouping_specialties(ctx)
        migrate_specialties(ctx)
        migrate_production_factor_types(ctx)
        migrate_production_factors(ctx)
        migrate_udo_types(ctx)
        migrate_operational_units(ctx)
        migrate_udos(ctx)
        migrate_udo_production_factors(ctx)
        migrate_udo_type_production_factor_types(ctx)
        migrate_udo_specialties_from_branches(ctx)
        migrate_udo_specialties_from_disciplines(ctx)
        migrate_udo_resolutions(ctx)
        migrate_udo_status_history(ctx)

        end_time = datetime.now()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        logging.info(f"Total migration time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")
        logging.info("ETL process completed successfully")
    except Exception as e:
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        logging.error(
            f"Error during execution after {int(hours)}h {int(minutes)}m {seconds:.2f}s: {e!s}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    main()
