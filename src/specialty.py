import logging
from datetime import datetime, timezone

import polars as pl

from core import ETLContext


def map_macroarea(value: str | None) -> str | None:
    if not value:
        return None

    value = value.lower().strip()

    if value == "acuti":
        return "ACUTI"
    elif value == "riabilitazione":
        return "RIABILITAZIONE"
    elif value == "intermedie":
        return "INTERMEDIE"
    else:
        return value


def migrate_grouping_disciplines(ctx: ETLContext) -> None:
    df_ragg_discpl = pl.read_database(
        "SELECT * FROM RAGG_DISCPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_macroarea_programmazione = pl.read_database(
        "SELECT * FROM MACROAREA_PROGRAMMAZIONE",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    ).select(
        pl.col("CLIENTID").str.strip_chars().alias("ID_MACROAREA_FK"),
        pl.col("NOME").str.strip_chars().alias("macroarea"),
    )
    df_result = df_ragg_discpl.join(
        df_macroarea_programmazione,
        left_on="ID_MACROAREA_FK",
        right_on="ID_MACROAREA_FK",
        how="left",
    )

    df_result = df_result.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("macroarea")
        .str.strip_chars()
        .map_elements(map_macroarea, return_dtype=pl.String),
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

    df_result.write_database(
        table_name="grouping_disciplines",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info("Migrated grouping_disciplines")


def migrate_disciplines(ctx: ETLContext) -> None:
    df_disciplina_templ = pl.read_database(
        "SELECT * FROM DISCIPLINA_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )

    df_result = df_disciplina_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("DESCR").str.strip_chars().alias("description"),
        pl.col("TIPO").str.strip_chars().alias("type"),
        pl.col("CODICE").str.strip_chars().alias("code"),
        pl.when(pl.col("PROGRAMMAZIONE") == 1)
        .then(True)
        .otherwise(False)
        .alias("is_cronos"),
        pl.when(pl.col("POA") == 1).then(True).otherwise(False).alias("is_poa"),
        pl.col("ID_RAGG_DISCIPL_TEMPL_FK")
        .str.strip_chars()
        .alias("grouping_discipline_id"),
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

    df_result.write_database(
        table_name="disciplines",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info("Migrated disciplines")


def migrate_branches(ctx: ETLContext) -> None:
    df_branca_templ = pl.read_database(
        "SELECT * FROM BRANCA_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_artic_branca_altro_templ = pl.read_database(
        "SELECT * FROM ARTIC_BRANCA_ALTRO_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )

    # Prepara il dataframe principale
    df_branca_templ = df_branca_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().fill_null("-").alias("name"),
        pl.col("DESCR").str.strip_chars().alias("description"),
        pl.col("TIPO").str.strip_chars().alias("type"),
        pl.col("CODICE").str.strip_chars().fill_null(pl.col("NOME")).alias("code"),
        pl.col("IS_ALTRO").str.strip_chars(),
        pl.when(pl.col("PROGRAMMAZIONE") == 1)
        .then(True)
        .otherwise(False)
        .alias("is_cronos"),
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
        pl.when(pl.col("ATTIVA") == "N")
        .then(
            pl.col("LAST_MOD")
            .fill_null(pl.col("CREATION"))
            .dt.replace_time_zone("Europe/Rome")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
        pl.lit(None).alias("parent_branch_id"),
    )

    # Prepara il dataframe aggiuntivo con JOIN per parent_branch_id
    df_artic_branca_altro_templ = df_artic_branca_altro_templ.join(
        df_branca_templ.filter(pl.col("IS_ALTRO") == "S").select(
            pl.col("id").alias("parent_branch_id")
        ),
        how="cross",
    ).select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().fill_null("-").alias("name"),
        pl.col("SETTING_BRANCA").str.strip_chars().alias("description"),
        pl.lit(None).alias("type"),
        pl.col("DESCR").str.strip_chars().fill_null("-").alias("code"),
        pl.lit(False).alias("is_cronos"),
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
        pl.col("parent_branch_id"),
        pl.lit("{}").alias("extra"),
    )

    # Unisci i dataframe
    df_result = pl.concat([df_branca_templ, df_artic_branca_altro_templ])

    # Scrivi nel database
    df_result.write_database(
        table_name="branches",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info("Migrated branches")
