import uuid
from datetime import datetime, timezone

import polars as pl

from utils import ETLContext, extract_data, handle_timestamps, load_data


def migrate_requirement_taxonomies(ctx: ETLContext) -> None:
    """
    Migrate requirement taxonomies from source database to target database.

    Extracts data from TIPO_REQUISITO and TIPO_SPECIFICO_REQUISITO tables,
    transforms it according to the target schema, and loads it into the
    requirement_taxonomies table.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipo_requisito = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_REQUISITO")
    df_tipo_specifico_requisito = extract_data(
        ctx, "SELECT * FROM AUAC_USR.TIPO_SPECIFICO_REQUISITO"
    )

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_tipo_requisito_tr = df_tipo_requisito.filter(
        pl.col("NOME").str.strip_chars().str.to_lowercase() == "generale",
    ).select(
        pl.col("CLIENTID").str.strip_chars().str.to_lowercase().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.lit(True).alias("is_readonly"),
        pl.lit(None).alias("disabled_at"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )
    df_tipo_specifico_requisito_tr = df_tipo_specifico_requisito.select(
        pl.col("CLIENTID").str.strip_chars().str.to_lowercase().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.lit(False).alias("is_readonly"),
        pl.lit(None).alias("disabled_at"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    # TODO: Questa cosa del fallback è giusta?
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    df_fallback = pl.DataFrame(
        [
            {
                "id": str(uuid.uuid4()),
                "name": "-",
                "is_readonly": False,
                "disabled_at": None,
                "created_at": now,
                "updated_at": now,
            }
        ]
    )

    df_result = pl.concat(
        [df_tipo_requisito_tr, df_tipo_specifico_requisito_tr, df_fallback], how="vertical_relaxed"
    )

    ### LOAD ###
    load_data(ctx, df_result, "requirement_taxonomies")


def migrate_requirement_lists(ctx: ETLContext) -> None:
    """
    Migrate requirement lists from source database to target database.

    Extracts data from LISTA_REQUISITI_TEMPL table, transforms it according to
    the target schema, and loads it into the requirement_lists table.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_lista_requisiti_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.LISTA_REQUISITI_TEMPL")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_lista_requisiti_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("ID_DELIBERA_TEMPL").str.strip_chars().alias("resolution_id"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    ### LOAD ###
    load_data(ctx, df_result, "requirement_lists")


def migrate_requirements(ctx: ETLContext) -> None:
    """
    Migrate requirements from source database to target database.

    Extracts data from REQUISITO_TEMPL and TIPO_RISPOSTA tables, as well as
    the previously migrated requirement_taxonomies table. Transforms the data
    according to the target schema and loads it into the requirements table.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_requisito_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.REQUISITO_TEMPL")
    df_tipo_risposta = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_RISPOSTA")
    df_requirement_taxonomies = extract_data(
        ctx, "SELECT * FROM requirement_taxonomies", source="pg_auac"
    )

    ### TRANSFORM ###
    requirement_taxonomy_fallback_df = df_requirement_taxonomies.filter(pl.col("name") == "-")

    if requirement_taxonomy_fallback_df.height != 1:
        raise Exception(
            f"Sono state trovate {requirement_taxonomy_fallback_df.height} righe per il valore di fallback di requirement_taxonomy"
        )

    requirement_taxonomy_fallback_id = requirement_taxonomy_fallback_df.item(row=0, column="id")

    df_tipo_risposta_tr = df_tipo_risposta.select(
        pl.col("CLIENTID").str.strip_chars(),
        pl.col("NOME")
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(" ", "_")
        .str.replace_all("/", "_")
        .alias("response_type"),
    )

    timestamp_exprs = handle_timestamps()

    df_requisito_templ_tr = df_requisito_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("TESTO").str.strip_chars().alias("text"),
        pl.col("ANNOTATIONS").str.strip_chars().alias("annotations"),
        pl.when(pl.col("VALIDATO").str.strip_chars().str.to_lowercase() == "s")
        .then(pl.lit("VALIDATO"))
        .when(pl.col("ANNULLATO").str.strip_chars().str.to_lowercase() == "s")
        .then(pl.lit("ANNULLATO"))
        .otherwise(pl.lit("BOZZA"))
        .alias("state"),
        pl.when(pl.col("IRRINUNCIABILE").str.strip_chars().str.to_lowercase() == "s")
        .then(True)
        .otherwise(False)
        .alias("is_required"),
        pl.when(pl.col("TIPO").str.strip_chars().str.to_lowercase() == "generale")
        .then(pl.col("ID_TIPO_REQUISITO_FK"))
        .otherwise(pl.col("ID_TIPO_SPECIFICO_REQUISITO_FK"))
        .fill_null(str(requirement_taxonomy_fallback_id))
        .alias("requirement_taxonomy_id"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        pl.col("ID_TIPO_RISPOSTA_FK"),
    )

    df_result = df_requisito_templ_tr.join(
        df_tipo_risposta_tr, left_on="ID_TIPO_RISPOSTA_FK", right_on="CLIENTID", how="left"
    )

    df_result = df_result.drop("ID_TIPO_RISPOSTA_FK")

    ### LOAD ###
    load_data(ctx, df_result, "requirements")


def migrate_procedures(ctx: ETLContext) -> None:
    """
    Migrate procedures from source database to target database.

    Extracts data from DOMANDA_INST and TIPO_PROC_TEMPL tables, transforms it
    according to the target schema, and loads it into the procedures table.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_domanda_inst = extract_data(ctx, "SELECT * FROM AUAC_USR.DOMANDA_INST")
    df_tipo_proc_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_PROC_TEMPL")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps(disabled_col="STATO", disabled_value="CESTINATA")

    df_domanda_inst_tr = df_domanda_inst.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("ID_DOMANDA").fill_null(pl.col("CODICE_UNIVOCO_NRECORD")).alias("progressive_code"),
        pl.col("ID_TITOLARE_FK").alias("company_id"),
        pl.col("ID_TIPO_PROC_FK"),
        pl.when(pl.col("STATO").str.strip_chars().str.replace_all(" ", "_") == "CESTINATA")
        .then("BOZZA")
        .otherwise(pl.col("STATO").str.strip_chars().str.replace_all(" ", "_"))
        .alias("status"),
        pl.col("DATA_CONCLUSIONE").alias("completion_date"),
        pl.col("DATA_INVIO_DOMANDA").alias("sent_date"),
        pl.col("DATA_SCADENZA").alias("expiration_date"),
        pl.col("DURATA_PROCEDIMENTO").alias("procedure_duration"),
        pl.col("MASSIMA_DURATA_PROCEDIMENTO").alias("max_procedure_duration"),
        pl.col("NUMERO_PROCEDIMENTO").alias("procedure_number"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    df_tipo_proc_templ_tr = df_tipo_proc_templ.select(
        pl.col("CLIENTID").str.strip_chars(),
        pl.col("DESCR")
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(" ", "_")
        .str.replace_all(".", "")
        .alias("procedure_type"),
    )

    df_result = df_domanda_inst_tr.join(
        df_tipo_proc_templ_tr, left_on="ID_TIPO_PROC_FK", right_on="CLIENTID", how="left"
    )

    ### LOAD ###
    load_data(ctx, df_result, "procedures")
