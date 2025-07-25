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


def map_specialty_type(value: str) -> str | None:
    """Map a specialty type string to a standardized format.

    This function takes a string representing a specialty type, converts it to lowercase,
    removes leading and trailing whitespace, and maps it to a standardized value
    using a match-case statement. If the value doesn't match any of the defined cases,
    None is returned.

    Parameters
    ----------
    value: str
        The specialty type string to be mapped

    Returns
    -------
    str | None
        The standardized specialty type value or None if no match is found
    """
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
    # Get timestamp expressions for disciplines
    timestamp_exprs = handle_timestamps(df_disciplina_templ)

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
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
        pl.lit(None).alias("parent_specialty_id"),
    ).with_columns(record_type=pl.lit("DISCIPLINE"))

    # Process branches
    # Get timestamp expressions for branches with custom disabled column and value
    timestamp_exprs = handle_timestamps(disabled_col="ATTIVA", disabled_value="N")

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
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
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
    # Get timestamp expressions for additional branches
    timestamp_exprs = handle_timestamps()

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
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
        pl.lit(parent_branch_id).alias("parent_specialty_id"),
    ).with_columns(record_type=pl.lit("BRANCH"))

    # Combine all dataframes
    df_result = pl.concat(
        [df_disciplines, df_branches, df_additional_branches], how="vertical_relaxed"
    )

    # Remove duplicates based on name and code to avoid unique constraint violation
    df_result = df_result.unique(subset=["name", "code"], keep="first")

    ### LOAD ###
    load_data(ctx, df_result, "specialties")
