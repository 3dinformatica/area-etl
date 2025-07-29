import logging

from utils import ETLContext, truncate_pg_table

CRONOS_TABLES = [
    "cronos_companies",
    "cronos_physical_structures",
    "cronos_plan_specialties",
    "cronos_plan_specialty_aliases",
    "cronos_plan_grouping_specialties",
    "cronos_plans",
    "cronos_taxonomies",
    "dm70_taxonomies",
    "healthcare_companies",
]


def truncate_cronos_tables(ctx: ETLContext) -> None:
    """
    Truncate all the tables in the PostgreSQL database of A.Re.A. Cronos service.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    logging.info(f"Truncating all target tables in PostgreSQL {ctx.pg_engine_cronos}...")

    for table in CRONOS_TABLES:
        truncate_pg_table(ctx.pg_engine_cronos, table)


def migrate_cronos(ctx: ETLContext) -> None:
    """
    Migrate data from source databases to the Cronos service database.

    This function orchestrates the ETL process for the Cronos service,
    currently only truncating all target tables.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    truncate_cronos_tables(ctx)
