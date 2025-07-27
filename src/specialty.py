import polars as pl

from utils import ETLContext, extract_data, handle_timestamps, load_data


def map_macroarea(value: str | None) -> str | None:
    """Map a macroarea string to a standardized format.

    This function takes a string representing a macroarea, converts it to lowercase,
    removes leading and trailing whitespace, and maps it to a standardized value
    using a predefined dictionary. If the value is not found in the mapping,
    the original value is returned.

    Parameters
    ----------
    value: str | None
        The macroarea string to be mapped or None

    Returns
    -------
    str | None
        The standardized macroarea value or None if the input is None
    """
    if not value:
        return None

    value = value.lower().strip()

    macroarea_mapping = {
        "acuti": "ACUTI",
        "riabilitazione": "RIABILITAZIONE",
        "intermedie": "INTERMEDIE",
        "territoriale": "TERRITORIALE",
    }

    return macroarea_mapping.get(value, value)


def map_specialty_type(value: str | None) -> str | None:
    """Map a specialty type string to a standardized format.

    This function takes a string representing a specialty type and maps it to a standardized value
    using a predefined dictionary. If the value is not found in the mapping,
    None is returned.

    Parameters
    ----------
    value: str | None
        The specialty type string to be mapped or None

    Returns
    -------
    str | None
        The standardized specialty type value or None if the input is not found in the mapping
    """
    specialty_type_mapping = {
        "alt": "ALTRO",
        "ter": "TERRITORIALE",
        "terr": "TERRITORIALE",
        "nonosp": "NON_OSPEDALIERO",
        "osp": "OSPEDALIERO",
    }

    return specialty_type_mapping.get(value)


def migrate_grouping_specialties(ctx: ETLContext) -> None:
    """Migrate grouping specialties from Oracle to PostgreSQL.

    Transfers data from the Oracle table "AUAC_USR.RAGG_DISCPL" to the PostgreSQL table
    "grouping_specialties".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_ragg_discpl = extract_data(ctx, "SELECT * FROM AUAC_USR.RAGG_DISCPL")

    df_macroarea_programmazione = extract_data(
        ctx, "SELECT * FROM AUAC_USR.MACROAREA_PROGRAMMAZIONE"
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
    # Get timestamp expressions
    timestamp_exprs = handle_timestamps()

    df_result = df_result.select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("id"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("macroarea").str.strip_chars().map_elements(map_macroarea, return_dtype=pl.String),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )

    ### LOAD ###
    load_data(ctx, df_result, "grouping_specialties")


def migrate_specialties(ctx: ETLContext) -> None:
    """Migrate specialties from Oracle to PostgreSQL.

    Transfers data from the Oracle tables "AUAC_USR.DISCIPLINA_TEMPL", "AUAC_USR.BRANCA_TEMPL",
    "ARTIC_BRANCA_ALTRO_TEMPL" to the PostgreSQL table "specialties".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_disciplina_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.DISCIPLINA_TEMPL")
    df_branca_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.BRANCA_TEMPL")
    df_artic_branca_altro_templ = extract_data(
        ctx, "SELECT * FROM AUAC_USR.ARTIC_BRANCA_ALTRO_TEMPL"
    )
    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_branca_templ_not_altro_tr = df_branca_templ.select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("DESCR").str.strip_chars().fill_null("-").alias("description"),
        pl.lit("BRANCH").alias("record_type"),
        pl.lit(None).alias("type"),
        pl.col("CODICE").str.strip_chars().alias("code"),
        pl.when(pl.col("PROGRAMMAZIONE") == 1)
        .then(True)
        .otherwise(False)
        .alias("is_used_in_cronos"),
        pl.lit(True).alias("is_used_in_poa"),
        pl.lit(None).alias("grouping_specialty_id"),
        pl.col("ID_BRANCA").cast(pl.String).str.strip_chars().alias("old_id"),
        pl.lit(None).alias("parent_specialty_id"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    df_branca_templ_altro_tr = df_branca_templ.filter(
        pl.col("IS_ALTRO").str.strip_chars().str.to_lowercase() == "s"
    )

    if df_branca_templ_altro_tr.height != 1:
        raise Exception(
            f'There are {df_branca_templ_altro_tr.height} branches marked as "ALTRO". There should be only 1 branch marked as "ALTRO".'
        )

    parent_specialty_id = df_branca_templ_altro_tr.item(row=0, column="CLIENTID")

    df_artic_branca_altro_templ_tr = df_artic_branca_altro_templ.select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().fill_null("-").alias("name"),
        pl.col("SETTING_BRANCA").str.strip_chars().alias("description"),
        pl.lit("BRANCH").alias("record_type"),
        pl.lit(None).alias("type"),
        pl.col("DESCR").str.strip_chars().fill_null("-").alias("code"),
        pl.lit(True).alias("is_used_in_cronos"),
        pl.lit(True).alias("is_used_in_poa"),
        pl.lit(None).alias("grouping_specialty_id"),
        pl.lit(None).alias("old_id"),
        pl.lit(parent_specialty_id).alias("parent_specialty_id"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    df_disciplines = df_disciplina_templ.select(
        pl.col("CLIENTID").cast(pl.String).str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("DESCR").str.strip_chars().alias("description"),
        pl.lit("DISCIPLINE").alias("record_type"),
        pl.col("TIPO")
        .str.strip_chars()
        .str.to_lowercase()
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
        pl.lit(None).alias("parent_specialty_id"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    df_result = pl.concat(
        [df_branca_templ_not_altro_tr, df_artic_branca_altro_templ_tr, df_disciplines],
        how="vertical_relaxed",
    )

    ### LOAD ###
    load_data(ctx, df_result, "specialties")
