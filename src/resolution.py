from datetime import datetime, timezone

import polars as pl

from core import ETLContext, extract_data, load_data


def migrate_resolution_types(ctx: ETLContext) -> None:
    """
    Migrate resolution types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipo_delibera = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_DELIBERA")

    df_tipo_atto = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_ATTO")

    ### TRANSFORM ###
    df_tipo_delibera = df_tipo_delibera.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().str.to_uppercase().alias("name"),
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
    df_tipo_atto = df_tipo_atto.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().str.to_uppercase().alias("name"),
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
    df_result = pl.concat([df_tipo_delibera, df_tipo_atto], how="vertical")
    df_result = df_result.unique("name")

    ### LOAD ###
    load_data(ctx, df_result, "resolution_types")


def migrate_resolutions(ctx: ETLContext) -> None:
    """
    Migrate resolutions from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_delibera_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.DELIBERA_TEMPL")
    df_atto_model = extract_data(ctx, "SELECT * FROM AUAC_USR.ATTO_MODEL")
    df_tipo_proc_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_PROC_TEMPL")
    df_tipo_delibera = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_DELIBERA")
    df_tipo_atto = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_ATTO")
    df_resolution_types = extract_data(ctx, "SELECT * FROM resolution_types", source="pg")

    ### TRANSFORM ###
    # Column "id" is read as an object an not as a string
    df_resolution_types = df_resolution_types.to_pandas()
    df_resolution_types["id"] = df_resolution_types["id"].astype("string")
    df_resolution_types = pl.from_pandas(df_resolution_types)

    df_delibera_templ = df_delibera_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().alias("name"),
        pl.col("TIPO_DELIBERA").str.strip_chars().fill_null("ALTRO").alias("category"),
        pl.col("NUMERO").str.strip_chars().alias("number"),
        pl.col("ANNO").str.strip_chars().alias("year"),
        pl.col("INIZIO_VALIDITA")
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("valid_from"),
        pl.col("FINE_VALIDITA")
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("valid_to"),
        pl.col("ID_ALLEGATO_FK").alias("file_id"),  # TODO: Missing actual file save to storage
        pl.col("N_BUR").cast(pl.String).str.strip_chars().alias("bur_number"),
        pl.col("DATA_BUR")
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("bur_date"),
        pl.col("LINK_DGR").str.strip_chars().alias("dgr_link"),
        pl.col("DIREZIONE").str.strip_chars().alias("direction"),
        pl.col("ID_TIPO_FK").str.strip_chars().alias("resolution_type_id"),
        pl.lit(None).alias("company_id"),
        pl.lit(None).alias("procedure_type"),
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
    df_atto_model = df_atto_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("ID_TIPO_FK").str.strip_chars(),
        pl.col("ID_TITOLARE_FK").str.strip_chars().alias("company_id"),
        pl.col("ID_TIPO_PROC_FK"),
        pl.col("ANNO").str.strip_chars().alias("year"),
        pl.col("NUMERO").str.strip_chars().alias("number"),
        pl.col("INIZIO_VALIDITA")
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("valid_from"),
        pl.col("FINE_VALIDITA")
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("valid_to"),
        pl.col("ID_ALLEGATO_FK").alias("file_id"),  # TODO: Missing actual file save to storage
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
    df_tipo_proc_templ = df_tipo_proc_templ.select(
        pl.col("CLIENTID"),
        pl.col("DESCR")
        .str.strip_chars()
        .str.replace(r" ", "_")
        .str.replace(r"\.", "")
        .str.to_uppercase()
        .alias("procedure_type"),
    )
    df_tipo_delibera = df_tipo_delibera.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().str.to_uppercase().alias("resolution_type_name"),
    )
    df_tipo_atto = df_tipo_atto.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().str.to_uppercase().alias("resolution_type_name"),
    )
    df_tipo_delibera_concat_tipo_atto = pl.concat([df_tipo_delibera, df_tipo_atto], how="vertical")
    df_resolution_types = df_resolution_types.select(
        pl.col("id").alias("resolution_type_id"),
        pl.col("name"),
    )

    df_result = df_atto_model.join(
        df_tipo_proc_templ,
        left_on="ID_TIPO_PROC_FK",
        right_on="CLIENTID",
        how="left",
    )
    df_result = df_result.join(
        df_tipo_delibera_concat_tipo_atto,
        left_on="ID_TIPO_FK",
        right_on="id",
        how="left",
    )
    df_result = df_result.join(
        df_resolution_types,
        left_on="resolution_type_name",
        right_on="name",
        how="left",
    )

    df_result = df_result.with_columns(
        category=pl.lit("UDO"),
        name=pl.lit(None),
        bur_number=pl.lit(None),
        bur_date=pl.lit(None),
        dgr_link=pl.lit(None),
        direction=pl.lit(None),
    ).drop(["ID_TIPO_PROC_FK", "ID_TIPO_FK", "resolution_type_name"])

    sorted_cols = [
        "id",
        "name",
        "category",
        "number",
        "year",
        "valid_from",
        "valid_to",
        "file_id",
        "bur_number",
        "bur_date",
        "dgr_link",
        "direction",
        "resolution_type_id",
        "company_id",
        "procedure_type",
        "created_at",
        "updated_at",
        "disabled_at",
    ]

    df_delibera_templ = df_delibera_templ.select(sorted_cols)
    df_result = df_result.select(sorted_cols)
    df_result = pl.concat([df_delibera_templ, df_result], how="vertical_relaxed")

    ### LOAD ###
    load_data(ctx, df_result, "resolutions")
