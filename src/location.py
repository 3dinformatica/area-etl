import logging
from datetime import datetime, timezone

import polars as pl

from core import ETLContext


def migrate_regions_provinces_municipalities(ctx: ETLContext) -> None:
    for file_name, table in [
        ("regions.csv", "regions"),
        ("provinces.csv", "provinces"),
        ("municipalities.csv", "municipalities"),
    ]:
        schema_overrides = (
            {"istat_code": pl.String} if "municipalities" in file_name else None
        )
        df = pl.read_csv(f"seed/{file_name}", schema_overrides=schema_overrides)
        df.write_database(
            table_name=table, connection=ctx.pg_engine, if_table_exists="append"
        )
        logging.info(f"Loaded seed data into {table}")


def migrate_toponyms(ctx: ETLContext) -> None:
    df = pl.read_database(
        "SELECT * FROM TOPONIMO_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    result = df.select(
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
    result.write_database(
        table_name="toponyms", connection=ctx.pg_engine, if_table_exists="append"
    )
    logging.info("Migrated toponyms")
