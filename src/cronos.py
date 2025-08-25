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


def migrate_healthcare_companies(ctx: ETLContext) -> None:
    """Migrate "healthcare_companies" data.

    Migrate data from ORACLE table "AUAC_USR.AZIENDA_SANITARIA" to Cronos service PostgreSQL table
    "healthcare_companies".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_azienda_sanitaria = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.AZIENDA_SANITARIA")
    df_ulss = extract_data(ctx.pg_engine_core, "SELECT * FROM ulss")

    ### TRANSFORM ###
    df_ulss_tr = df_ulss.select(pl.col("id").alias("ulss_id"), pl.col("code"))

    df_result = df_azienda_sanitaria.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        handle_text(source_col="CODICE", target_col="code"),
        handle_text(source_col="DESCRIZIONE", target_col="name"),
    ).join(
        df_ulss_tr,
        left_on="code",
        right_on="code",
        how="left",
    )

    ### LOAD ###
    load_data(ctx.pg_engine_cronos, df_result, "healthcare_companies")


def migrate_cronos_plans(ctx: ETLContext) -> None:
    """Migrate "cronos_plans" data.

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    pass


def migrate_cronos_plan_grouping_specialties(ctx: ETLContext) -> None:
    """Migrate "cronos_plan_grouping_specialties" data.

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    pass


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
    migrate_healthcare_companies(ctx)
    migrate_cronos_plans(ctx)
    migrate_cronos_plan_grouping_specialties(ctx)
