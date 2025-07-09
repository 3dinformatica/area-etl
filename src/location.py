from datetime import datetime, timezone

import polars as pl

from core import ETLContext, extract_data, extract_data_from_csv, load_data


def migrate_regions(ctx: ETLContext) -> None:
    """
    Migrate regions from seed CSV file to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    df = extract_data_from_csv("seed/regions.csv")
    load_data(ctx, df, "regions")


def migrate_provinces(ctx: ETLContext) -> None:
    """
    Migrate provinces from seed CSV file to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    df = extract_data_from_csv("seed/provinces.csv")
    load_data(ctx, df, "provinces")


def migrate_municipalities(ctx: ETLContext) -> None:
    """
    Migrate municipalities from seed CSV file to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    schema_overrides = {"istat_code": pl.String}
    df = extract_data_from_csv("seed/municipalities.csv", schema_overrides=schema_overrides)
    load_data(ctx, df, "municipalities")


def migrate_toponyms(ctx: ETLContext) -> None:
    """
    Migrate toponyms from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_toponimo_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TOPONIMO_TEMPL")

    ### TRANSFORM ###
    df_result = df_toponimo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("CREATION")
        .fill_null(datetime.now(timezone.utc).replace(tzinfo=None))
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
        pl.when(pl.col("DISABLED") == "S")
        .then(
            pl.col("LAST_MOD")
            .fill_null(pl.col("CREATION"))
            .dt.replace_time_zone("Europe/Rome")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "toponyms")


def migrate_districts(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_toponimo_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.DISTRETTO_TEMPL")

    ### TRANSFORM ###
    df_result = df_toponimo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("TITOLARE")
        .str.strip_chars()
        .str.strip_suffix("-")
        .str.replace("-", " - ")
        .alias("name"),
        pl.col("DISTRETTO").alias("code"),
        pl.col("CREATION")
        .fill_null(datetime.now(timezone.utc).replace(tzinfo=None))
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
        pl.when(pl.col("DISABLED") == "S")
        .then(
            pl.col("LAST_MOD")
            .fill_null(pl.col("CREATION"))
            .dt.replace_time_zone("Europe/Rome")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "districts")
