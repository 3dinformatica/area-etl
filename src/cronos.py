import logging

import polars as pl

from utils import ETLContext, extract_data, handle_text, load_data, truncate_pg_table

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


def migrate_cronos_taxonomies(ctx: ETLContext) -> None:
    """Migrate "cronos_taxonomies" data.

    Migrate data from ORACLE table "AUAC_USR.CLASSIFICAZIONE_PROGRAMMAZIONE" to Cronos service PostgreSQL table
    "cronos_taxonomies".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_classificazione_programmazione = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.CLASSIFICAZIONE_PROGRAMMAZIONE"
    )

    ### TRANSFORM ###
    df_result = df_classificazione_programmazione.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        handle_text(source_col="NOME", target_col="name"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_cronos, df_result, "cronos_taxonomies")


def migrate_dm70_taxonomies(ctx: ETLContext) -> None:
    """Migrate "dm70_taxonomies" data.

    Migrate data from ORACLE table "AUAC_USR.CLASSIFICAZIONE_DM_70" to Cronos service PostgreSQL table
    "dm70_taxonomies".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_classificazione_programmazione = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.CLASSIFICAZIONE_DM_70"
    )

    ### TRANSFORM ###
    df_result = df_classificazione_programmazione.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        handle_text(source_col="NOME", target_col="name"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_cronos, df_result, "dm70_taxonomies")


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
    migrate_cronos_taxonomies(ctx)
    migrate_dm70_taxonomies(ctx)
