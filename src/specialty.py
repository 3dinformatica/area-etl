import logging
from datetime import datetime, timezone

import polars as pl

from core import ETLContext, extract_data, load_data


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
    elif value == "territoriale":
        return "TERRITORIALE"
    else:
        return value


def map_specialty_type(value: str) -> str | None:
    match value.lower().strip():
        case "alt":
            return "ALTRO"
        case "terr" | "ter":
            return "TERRITORIALE"
        case "nonosp":
            return "NON_OSPEDALIERO"
        case "osp":
            return "OSPEDALIERO"
        case _:
            return None


def migrate_grouping_specialties(ctx: ETLContext) -> None:
    """
    Migrate grouping specialties from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_ragg_discpl = extract_data(
        ctx, 
        "SELECT * FROM AUAC_USR.RAGG_DISCPL"
    )

    df_macroarea_programmazione = extract_data(
        ctx, 
        "SELECT * FROM AUAC_USR.MACROAREA_PROGRAMMAZIONE"
    ).select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("ID_MACROAREA_FK"),
        pl.col("NOME").str.strip_chars().alias("macroarea"),
    )

    ### TRANSFORM ###
    df_result = df_ragg_discpl.join(
        df_macroarea_programmazione,
        left_on="ID_MACROAREA_FK",
        right_on="ID_MACROAREA_FK",
        how="left",
    )
    df_result = df_result.select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("id"),
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

    ### LOAD ###
    load_data(ctx, df_result, "grouping_specialties")


def migrate_specialties(ctx: ETLContext) -> None:
    """
    Migrate specialties from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_disciplina_templ = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.DISCIPLINA_TEMPL"
    )

    df_branca_templ = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.BRANCA_TEMPL"
    )

    df_artic_branca_altro_templ = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.ARTIC_BRANCA_ALTRO_TEMPL"
    )

    ### TRANSFORM ###
    df_disciplines = df_disciplina_templ.select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("DESCR").str.strip_chars().alias("description"),
        pl.col("TIPO")
        .str.to_lowercase()
        .str.strip_chars()
        .map_elements(map_specialty_type, return_dtype=pl.String)
        .alias("type"),
        pl.col("CODICE").str.strip_chars().alias("code"),
        pl.when(pl.col("PROGRAMMAZIONE") == 1)
        .then(True)
        .otherwise(False)
        .alias("is_used_in_cronos"),
        pl.when(pl.col("POA") == 1).then(True).otherwise(False).alias("is_used_in_poa"),
        pl.col("ID_RAGG_DISCIPL_TEMPL_FK")
        .cast(pl.String)
        .str.strip_chars()
        .alias("grouping_specialty_id"),
        pl.col("ID_DISCIPLINA").cast(pl.String).str.strip_chars().alias("old_id"),
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
        pl.lit(None).alias("parent_specialty_id"),
    ).with_columns(record_type=pl.lit("DISCIPLINE"))

    # Process branches
    df_branches = df_branca_templ.select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().fill_null("-").alias("name"),
        pl.col("DESCR").str.strip_chars().alias("description"),
        pl.lit(None).alias("type"),
        pl.col("CODICE").str.strip_chars().fill_null(pl.col("NOME")).alias("code"),
        pl.when(pl.col("PROGRAMMAZIONE") == 1)
        .then(True)
        .otherwise(False)
        .alias("is_used_in_cronos"),
        pl.lit(True).alias("is_used_in_poa"),
        pl.lit(None).alias("grouping_specialty_id"),
        pl.col("ID_BRANCA").cast(pl.String).str.strip_chars().alias("old_id"),
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
        pl.lit(None).alias("parent_specialty_id"),
    ).with_columns(record_type=pl.lit("BRANCH"))

    # Find parent branch ID for additional branches
    parent_branches = df_branca_templ.filter(pl.col("IS_ALTRO") == "S")
    parent_branch_id = None
    if parent_branches.height > 0:
        parent_branch_id = parent_branches.select(
            pl.col("CLIENTID").cast(pl.String).str.strip_chars()
        ).row(0)[0]

    # Process additional branches
    df_additional_branches = df_artic_branca_altro_templ.select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().fill_null("-").alias("name"),
        pl.col("SETTING_BRANCA").str.strip_chars().alias("description"),
        pl.lit(None).alias("type"),
        pl.col("DESCR").str.strip_chars().fill_null("-").alias("code"),
        pl.lit(True).alias("is_used_in_cronos"),
        pl.lit(True).alias("is_used_in_poa"),
        pl.lit(None).alias("grouping_specialty_id"),
        pl.lit(None).alias("old_id"),
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
        pl.lit(parent_branch_id).alias("parent_specialty_id"),
    ).with_columns(record_type=pl.lit("BRANCH"))

    # Combine all dataframes
    df_result = pl.concat(
        [df_disciplines, df_branches, df_additional_branches],
        how="vertical_relaxed"
    )

    # Remove duplicates based on name and code to avoid unique constraint violation
    df_result = df_result.unique(subset=["name", "code"], keep="first")

    ### LOAD ###
    load_data(ctx, df_result, "specialties")
