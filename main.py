import logging

from company import (
    migrate_company_types,
    migrate_companies,
    migrate_physical_structures,
    migrate_operational_office,
    migrate_buildings,
)
from core import setup_logging, setup_connections, truncate_postgresql_tables
from location import migrate_regions_provinces_municipalities, migrate_toponyms
from specialty import (
    migrate_grouping_specialties,
    migrate_specialties,
)
from udo import (
    migrate_production_factor_types,
    migrate_production_factors,
    migrate_udo_types,
    migrate_udo_production_factors,
    migrate_udo_type_production_factor_types,
    migrate_udo_branches,
)


def main() -> None:
    setup_logging()
    logging.info("===>>> A.Re.A. ETL <<<===")
    ctx = setup_connections()
    truncate_postgresql_tables(ctx)
    migrate_regions_provinces_municipalities(ctx)
    migrate_toponyms(ctx)
    migrate_company_types(ctx)
    migrate_companies(ctx)
    migrate_physical_structures(ctx)
    migrate_operational_office(ctx)
    migrate_buildings(ctx)
    migrate_grouping_specialties(ctx)
    migrate_specialties(ctx)
    migrate_production_factor_types(ctx)
    migrate_production_factors(ctx)
    migrate_udo_types(ctx)
    migrate_udo_production_factors(ctx)
    migrate_udo_type_production_factor_types(ctx)
    migrate_udo_branches(ctx)
    logging.info("===>>> ETL completato con successo <<<===")


if __name__ == "__main__":
    main()
