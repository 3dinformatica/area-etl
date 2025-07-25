from datetime import datetime

import polars as pl
import pytz

from utils import ETLContext, extract_data, extract_data_from_csv, handle_timestamps, load_data


def migrate_regions(ctx: ETLContext) -> None:
    """
    Migrate regions from a local seed CSV file to PostgreSQL table "regions".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_regions = extract_data_from_csv("seed/regions.csv")

    ### LOAD ###
    load_data(ctx, df_regions, "regions")


def migrate_provinces(ctx: ETLContext) -> None:
    """
    Migrate regions from a local seed CSV file to PostgreSQL table "provinces".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_provinces = extract_data_from_csv("seed/provinces.csv")

    ### LOAD ###
    load_data(ctx, df_provinces, "provinces")


def migrate_municipalities(ctx: ETLContext) -> None:
    """
    Migrate regions from a local seed CSV file to PostgreSQL table "municipalities".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    schema_overrides = {"istat_code": pl.String}
    df_municipalities = extract_data_from_csv(
        "seed/municipalities.csv", schema_overrides=schema_overrides
    )

    ### LOAD ###
    load_data(ctx, df_municipalities, "municipalities")


def migrate_toponyms(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_USR.TOPONIMO_TEMPL" to PostgreSQL table "toponyms".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_toponimo_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TOPONIMO_TEMPL")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_toponimo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )

    ### LOAD ###
    load_data(ctx, df_result, "toponyms")


def migrate_ulss(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_USR.ULSS_TERRITORIALE" to PostgreSQL table "ulss".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_ulss_territoriale = extract_data(ctx, "SELECT * FROM AUAC_USR.ULSS_TERRITORIALE")

    ### TRANSFORM ###
    rome_tz = pytz.timezone("Europe/Rome")
    now_rome = datetime.now(rome_tz)
    df_result = df_ulss_territoriale.select(
        pl.col("DESCRIZIONE").str.strip_chars().alias("name"),
        pl.col("CODICE").alias("code"),
    ).with_columns(
        disabled_at=pl.lit(None),
        created_at=pl.lit(now_rome),
        updated_at=pl.lit(now_rome),
    )

    ### LOAD ###
    load_data(ctx, df_result, "ulss")


def migrate_districts(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_USR.DISTRETTO_TEMPL" to PostgreSQL table "districts".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_toponimo_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.DISTRETTO_TEMPL")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_toponimo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("TITOLARE")
        .str.strip_chars()
        .str.strip_suffix("-")
        .str.replace("-", " - ")
        .alias("name"),
        pl.col("DISTRETTO").alias("code"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )

    ### LOAD ###
    load_data(ctx, df_result, "districts")
