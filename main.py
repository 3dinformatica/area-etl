import logging

from company import (
    migrate_company_types,
    migrate_companies,
    migrate_physical_structures,
    migrate_operational_offices,
    migrate_buildings,
)
from core import setup_logging, setup_connections, truncate_postgresql_tables
from location import migrate_regions, migrate_provinces, migrate_municipalities, migrate_toponyms
from resolution import migrate_resolution_types, migrate_resolutions
from specialty import (
    migrate_grouping_specialties,
    migrate_specialties,
)
from udo import (
    migrate_production_factor_types,
    migrate_production_factors,
    migrate_udo_types,
    migrate_udos,
    migrate_udo_production_factors,
    migrate_udo_type_production_factor_types,
    migrate_udo_specialties_from_branches,
    migrate_udo_specialties_from_disciplines,
    migrate_udo_resolutions,
    migrate_udo_status_history,
)


def main() -> None:
    setup_logging()
    logging.info("Starting A.Re.A. ETL process...")

    try:
        ctx = setup_connections()
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
        migrate_udos(ctx)
        migrate_udo_production_factors(ctx)
        migrate_udo_type_production_factor_types(ctx)
        migrate_udo_specialties_from_branches(ctx)
        migrate_udo_specialties_from_disciplines(ctx)
        migrate_udo_resolutions(ctx)
        migrate_udo_status_history(ctx)
        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error(f"Error during ETL execution: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
