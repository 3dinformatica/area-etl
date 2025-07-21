from datetime import datetime

import polars as pl
import pytz

from core import ETLContext, extract_data, extract_data_from_csv, load_data


def migrate_regions(ctx: ETLContext) -> None:
    """
    Migrate regions from seed CSV file to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_regions = extract_data_from_csv("seed/regions.csv")

    ### TRANSFORM ###
    rome_tz = pytz.timezone("Europe/Rome")
    now_rome = datetime.now(rome_tz)
    df_result = df_regions.with_columns(
        disabled_at=pl.lit(None),
        created_at=pl.lit(now_rome).dt.replace_time_zone("Europe/Rome"),
        updated_at=pl.lit(now_rome).dt.replace_time_zone("Europe/Rome"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "regions")


def migrate_provinces(ctx: ETLContext) -> None:
    """
    Migrate provinces from seed CSV file to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_provinces = extract_data_from_csv("seed/provinces.csv")

    ### TRANSFORM ###
    rome_tz = pytz.timezone("Europe/Rome")
    now_rome = datetime.now(rome_tz)
    df_result = df_provinces.with_columns(
        disabled_at=pl.lit(None),
        created_at=pl.lit(now_rome).dt.replace_time_zone("Europe/Rome"),
        updated_at=pl.lit(now_rome).dt.replace_time_zone("Europe/Rome"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "provinces")


def migrate_municipalities(ctx: ETLContext) -> None:
    """
    Migrate municipalities from seed CSV file to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    schema_overrides = {"istat_code": pl.String}
    df_municipalities = extract_data_from_csv(
        "seed/municipalities.csv", schema_overrides=schema_overrides
    )

    ### TRANSFORM ###
    rome_tz = pytz.timezone("Europe/Rome")
    now_rome = datetime.now(rome_tz)
    df_result = df_municipalities.with_columns(
        disabled_at=pl.lit(None),
        created_at=pl.lit(now_rome),
        updated_at=pl.lit(now_rome),
    )

    ### LOAD ###
    load_data(ctx, df_result, "municipalities")


def migrate_toponyms(ctx: ETLContext) -> None:
    """
    Migrate toponyms from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_toponimo_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TOPONIMO_TEMPL")

    ### TRANSFORM ###
    rome_tz = pytz.timezone("Europe/Rome")
    now_rome = datetime.now(rome_tz)

    # Convert now_rome to timezone-naive for consistent handling
    now_rome_naive = now_rome.replace(tzinfo=None)

    df_result = df_toponimo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("CREATION").fill_null(now_rome_naive).alias("created_at"),
        pl.col("LAST_MOD").fill_null(pl.col("CREATION")).alias("updated_at"),
        pl.when(pl.col("DISABLED") == "S")
        .then(pl.col("LAST_MOD").fill_null(pl.col("CREATION")))
        .otherwise(None)
        .alias("disabled_at"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "toponyms")


def migrate_ulss(ctx: ETLContext) -> None:
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
    ### EXTRACT ###
    df_toponimo_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.DISTRETTO_TEMPL")

    ### TRANSFORM ###
    rome_tz = pytz.timezone("Europe/Rome")
    now_rome = datetime.now(rome_tz)

    # Convert now_rome to timezone-naive for consistent handling
    now_rome_naive = now_rome.replace(tzinfo=None)

    df_result = df_toponimo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("TITOLARE")
        .str.strip_chars()
        .str.strip_suffix("-")
        .str.replace("-", " - ")
        .alias("name"),
        pl.col("DISTRETTO").alias("code"),
        pl.col("CREATION").fill_null(now_rome_naive).alias("created_at"),
        pl.col("LAST_MOD").fill_null(pl.col("CREATION")).alias("updated_at"),
        pl.when(pl.col("DISABLED") == "S")
        .then(pl.col("LAST_MOD").fill_null(pl.col("CREATION")))
        .otherwise(None)
        .alias("disabled_at"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "districts")
