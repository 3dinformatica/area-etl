import json
import uuid
from datetime import datetime, timezone
from typing import Callable

import polars as pl

from core import ETLContext, extract_data, load_data

_NOW = datetime.now(timezone.utc).replace(tzinfo=None)

def _bool(col: str, true_values: tuple[str | int | bool, ...] = ("S", "s", 1, True)) -> pl.Expr:
    """Normalize *truthy* DB flags to Python booleans."""
    return pl.when(pl.col(col).cast(str).is_in(list(map(str, true_values)))).then(True).otherwise(False)

def migrate_requirement_taxonomies(ctx: ETLContext) -> None:
    """
    Migrate requirement taxonomies from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    df_type = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_REQUISITO")
    df_spec = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_SPECIFICO_REQUISITO")

    df_type = df_type.filter(pl.col("NOME") != "Specifico")

    df = pl.concat([
        df_type.select("CLIENTID", "NOME", "CREATION", "LAST_MOD"),
        df_spec.select("CLIENTID", "NOME", "CREATION", "LAST_MOD")
    ])

    default_ts = datetime(2024, 1, 1)

    df = (
        df.with_columns([
            pl.col("CREATION").fill_null(default_ts).alias("created_at"),
            pl.col("LAST_MOD").fill_null(pl.col("CREATION")).fill_null(default_ts).alias("updated_at"),
            pl.col("NOME").str.strip_chars().alias("name"),
        ])
        .with_columns([
            (pl.col("name") == "Generale").alias("is_readonly")
        ])
        .select([
            pl.col("CLIENTID").str.strip_chars().str.to_lowercase().alias("id"),
            "name", "is_readonly", "created_at", "updated_at"
        ])
    )

    # Add fallback/default taxonomy "-"
    df_default = pl.DataFrame([{
        "id": str(uuid.uuid4()),
        "name": "-",
        "is_readonly": False,
        "created_at": default_ts,
        "updated_at": default_ts
    }])

    df = pl.concat([df, df_default])

    load_data(ctx, df, "requirement_taxonomies")


def migrate_requirement_lists(ctx: ETLContext) -> None:
    """
    Migrate requirement lists from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    df = extract_data(ctx, "SELECT * FROM AUAC_USR.LISTA_REQUISITI_TEMPL")

    df = df.select(
        pl.col("CLIENTID").str.to_lowercase().str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("ID_DELIBERA_TEMPL").str.strip_chars().alias("resolution_id"),
        pl.col("CREATION")
        .fill_null(_NOW)
        .dt.replace_time_zone("Europe/Rome").dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome").dt.replace_time_zone(None)
        .alias("updated_at"),
        _bool("DISABLED").alias("disabled"),
    ).with_columns(
        pl.when(pl.col("disabled")).then(pl.col("updated_at")).otherwise(None).alias("disabled_at")
    ).drop("disabled")

    ### LOAD ###
    load_data(ctx, df, "requirement_lists")


def migrate_requirementlist_requirements(ctx: ETLContext) -> None:
    """
    Migrate requirement list → requirement bindings.
    Args:
        ctx: The ETL context containing database connections
    """
    df = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_LISTA_REQUISITO")

    df = (
        df.select(
            pl.col("ID_LISTA_FK").alias("requirement_list_id"),
            pl.col("ID_REQUISITO_FK").alias("requirement_id"),
            _bool("IRRINUNCIABILE").alias("is_required"),
        )
    )

    load_data(ctx, df, "requirementlist_requirements")


def migrate_requirements(ctx: ETLContext) -> None:
    """
    Migrate requirements from Oracle to PostgreSQL.
    """
    _DEFAULT_TIMESTAMP = datetime(2024, 1, 1)

    df = extract_data(ctx, "SELECT * FROM AUAC_USR.REQUISITO_TEMPL").select([
        "CLIENTID", "NOME", "TESTO", "ANNOTATIONS", "VALIDATO", "ANNULLATO", "IRRINUNCIABILE",
        "LAST_MOD", "CREATION", "DISABLED", "ID_TIPO_RISPOSTA_FK", "ID_TIPO_REQUISITO_FK",
        "ID_TIPO_SPECIFICO_REQUISITO_FK", "TIPO"
    ])

    # Strip whitespace manually (was strip_dataframe_fields)
    df = df.with_columns([
        pl.col("NOME").str.strip_chars().alias("NOME"),
        pl.col("TESTO").str.strip_chars().alias("TESTO"),
    ])


    # Inline date filling (was set_date)
    df = df.with_columns([
        pl.col("CREATION").fill_null(_DEFAULT_TIMESTAMP).alias("CREATION"),
        pl.col("LAST_MOD").fill_null(pl.col("CREATION")).alias("LAST_MOD"),
        (_bool("DISABLED")).alias("DISABLED_BOOL"),
    ])
    df = df.with_columns([
        pl.when(pl.col("DISABLED_BOOL")).then(_DEFAULT_TIMESTAMP).otherwise(None).alias("disabled_at")
    ])

    # Booleans
    df = df.with_columns([
        _bool("VALIDATO").alias("VALIDATO"),
        _bool("ANNULLATO").alias("ANNULLATO"),
        _bool("IRRINUNCIABILE").alias("IRRINUNCIABILE"),
    ])

    # Taxonomy fallback
    df_def = extract_data(ctx,"SELECT * FROM requirement_taxonomies", source="pg");
    fallback_id = df_def.filter(pl.col("name") == "-")["id"].item()

    # Taxonomy selection logic
    df = df.with_columns([
        pl.when(pl.col("TIPO").str.to_lowercase() == "generale")
        .then(pl.col("ID_TIPO_REQUISITO_FK"))
        .otherwise(pl.col("ID_TIPO_SPECIFICO_REQUISITO_FK"))
        .alias("requirement_taxonomy_id")
    ])
    df = df.with_columns([
        pl.col("requirement_taxonomy_id").fill_null(str(fallback_id))
    ])

    # State assignment (was get_requirements_state)
    df = df.with_columns(
        pl.when(pl.col("VALIDATO"))
        .then(pl.lit("VALIDATO"))
        .when(pl.col("ANNULLATO"))
        .then(pl.lit("ANNULLATO"))
        .otherwise(pl.lit("BOZZA"))
        .alias("state")
    )

    # Load response types
    df_res = extract_data(ctx, "SELECT CLIENTID, NOME FROM AUAC_USR.TIPO_RISPOSTA")
    df_res = df_res.with_columns([
        pl.col("NOME").str.strip_chars()
            .str.to_uppercase()
            .str.replace_all(" ", "_")
            .str.replace_all("/", "_")
            .alias("RESPONSE_TYPE")
    ])

    df = df.join(df_res, left_on="ID_TIPO_RISPOSTA_FK", right_on="CLIENTID", how="left")

    # Final selection
    df = df.select([
        pl.col("CLIENTID").str.strip_chars().str.to_lowercase().alias("id"),
        pl.col("NOME").alias("name"),
        pl.col("TESTO").alias("text"),
        pl.col("ANNOTATIONS").alias("annotations"),
        pl.col("IRRINUNCIABILE").alias("is_required"),
        "requirement_taxonomy_id",
        pl.col("RESPONSE_TYPE").alias("response_type"),
        pl.col("CREATION").alias("created_at"),
        pl.col("LAST_MOD").alias("updated_at"),
        "disabled_at",
        "state"
    ])

    load_data(ctx, df, "requirements")


def migrate_procedures(ctx: ETLContext) -> None:
    """
    Migrate procedures from Oracle to PostgreSQL.
    """
    _NOW = datetime.now()
    _DEFAULT_TIMESTAMP = datetime(2024, 1, 1)

    df = extract_data(ctx, "SELECT * FROM AUAC_USR.DOMANDA_INST").select([
        "CLIENTID", "ID_TIPO_PROC_FK", "NUMERO_PROCEDIMENTO", "DATA_CONCLUSIONE", "DATA_INVIO_DOMANDA",
        "ID_TITOLARE_FK", "STATO", "ID_DOMANDA", "CREATION", "LAST_MOD", "CODICE_UNIVOCO_NRECORD",
        "DATA_SCADENZA", "DURATA_PROCEDIMENTO", "MASSIMA_DURATA_PROCEDIMENTO", "CESTINATA_IL"
    ])

    df = df.with_columns([
        pl.col("CREATION").fill_null(_DEFAULT_TIMESTAMP).alias("CREATION"),
        pl.col("LAST_MOD").fill_null(pl.col("CREATION")).alias("LAST_MOD"),
        pl.col("STATO").str.strip_chars().str.replace_all(" ", "_").alias("STATO"),
    ])

    df = df.with_columns([
        pl.when(pl.col("STATO") == "CESTINATA").then(_NOW).otherwise(None).alias("disabled_at"),
        pl.when(pl.col("STATO") == "CESTINATA").then("BOZZA").otherwise(pl.col("STATO")).alias("status"),
    ])

    df_type = extract_data(ctx, "SELECT CLIENTID, DESCR FROM AUAC_USR.TIPO_PROC_TEMPL")
    df_type = df_type.with_columns([
        pl.col("DESCR").str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(" ", "_")
        .str.replace_all(".", "")
        .alias("procedure_type")
    ])

    df = df.join(df_type, left_on="ID_TIPO_PROC_FK", right_on="CLIENTID", how="left")

    df = df.select([
        pl.col("CLIENTID").str.strip_chars().str.to_lowercase().alias("id"),
        pl.col("ID_DOMANDA").fill_null(pl.col("CODICE_UNIVOCO_NRECORD")).alias("progressive_code"),
        pl.col("ID_TITOLARE_FK").alias("company_id"),
        "procedure_type",
        "status",
        pl.col("NUMERO_PROCEDIMENTO").alias("procedure_number"),
        pl.col("DATA_CONCLUSIONE").alias("completion_date"),
        pl.col("DATA_INVIO_DOMANDA").alias("sent_date"),
        pl.col("DATA_SCADENZA").alias("expiration_date"),
        pl.col("DURATA_PROCEDIMENTO").alias("procedure_duration"),
        pl.col("MASSIMA_DURATA_PROCEDIMENTO").alias("max_procedure_duration"),
        pl.col("CREATION").alias("created_at"),
        pl.col("LAST_MOD").alias("updated_at"),
        "disabled_at"
    ])

    load_data(ctx, df, "procedures")


def _load_proc_list_binding(ctx: ETLContext, table: str, columns: dict, target: str) -> None:
    df = extract_data(ctx, f"SELECT * FROM AUAC_USR.{table}").select(list(columns.keys()))

    df_type = extract_data(ctx, "SELECT CLIENTID, DESCR FROM AUAC_USR.TIPO_PROC_TEMPL").with_columns([
        pl.col("DESCR").str.strip_chars().str.to_uppercase().str.replace_all(" ", "_").str.replace_all(".", "").alias("procedure_type")
    ])

    df = df.join(df_type, left_on="ID_TIPO_PROC_FK", right_on="CLIENTID", how="left")

    df = df.select([
        pl.col(k).alias(v) if v != "procedure_type" else pl.col("procedure_type").alias(v)
        for k, v in columns.items()
    ])

    load_data(ctx, df, target)


def migrate_procedure_type_requirement_list_classification_mental(ctx: ETLContext) -> None:
    _load_proc_list_binding(ctx,
        table="BIND_UO_PROC_LISTA",
        columns={
            "ID_TIPO_PROC_FK": "procedure_type",
            "ID_LISTA_FK": "requirement_list_id",
            "ID_CLASS_UDO_TEMPL_FK": "udo_type_classification_id",
            "SALUTE_MENTALE": "is_mental_health"
        },
        target="procedure_type_requirement_list_classification_mental"
    )


def migrate_procedure_type_requirement_list_comp_type_comp_class(ctx: ETLContext) -> None:
    _load_proc_list_binding(ctx,
        table="BIND_LISTA_EDIF_TIPTIT_CLTIT",
        columns={
            "CLIENTID": "id",
            "ID_TIPO_PROC_FK": "procedure_type",
            "ID_LISTA_FK": "requirement_list_id",
            "ID_TIPO_TITOLARE_TEMPL_FK": "company_type_id",
            "ID_CLASSIFICAZIONE_TEMPL_FK": "company_classification_id",
        },
        target="procedure_type_requirement_list_comp_type_comp_class"
    )


def migrate_procedure_type_requirement_list_udo_type(ctx: ETLContext) -> None:
    _load_proc_list_binding(ctx,
        table="BIND_TIPO_22_LISTA",
        columns={
            "ID_TIPO_PROC_FK": "procedure_type",
            "ID_LISTA_FK": "requirement_list_id",
            "ID_TIPO_UDO_22_FK": "udo_type_id"
        },
        target="procedure_type_requirement_list_udo_type"
    )


def migrate_procedure_type_requirement_list_for_physical_structures(ctx: ETLContext) -> None:
    _load_proc_list_binding(ctx,
        table="BIND_LISTA_REQU_STRUTT_FIS",
        columns={
            "ID_TIPO_PROC_FK": "procedure_type",
            "ID_LISTA_FK": "requirement_list_id"
        },
        target="procedure_type_requirement_list_for_physical_structures"
    )


def migrate_procedure_entity_requirements(ctx: ETLContext) -> None:
    query = """
        SELECT * FROM AUAC_USR.REQUISITO_INST WHERE ID_DOMANDA_FK IS NULL
    """
    df = extract_data(ctx, query)

    default_cols = [
        "CLIENTID", "ROOT_ID_DOMANDA_FK", "ID_REQ_TEMPL_FK", "ID_ASSEGNATARIO_FK",
        "VALUTAZIONE", "EVIDENZE", "ANNOTATIONS", "LAST_MOD", "CREATION",
        "ID_UDO_FK", "ID_UO_FK", "ID_STRUTTURA_FK", "ID_EDIFICIO_FK", "ID_COMPRENSORIO_FK"
    ]
    for col in default_cols:
        if col not in df.columns:
            df = df.with_columns([pl.lit(None).alias(col)])

    df_req = extract_data(ctx, "SELECT * FROM requirements")
    df_tax = extract_data(ctx, "SELECT * FROM requirement_taxonomies")

    df_req = df_req.join(df_tax, left_on="requirement_taxonomy_id", right_on="id", how="left")

    df = df.with_columns([
        pl.col("ID_REQ_TEMPL_FK").apply(lambda x: uuid.UUID(x) if x else None)
    ])

    df = df.join(df_req, left_on="ID_REQ_TEMPL_FK", right_on="id", how="left")

    df = df.with_columns([
        pl.col("LAST_MOD").fill_null(pl.col("CREATION")).alias("updated_at"),
        pl.col("CREATION").fill_null(datetime(2024, 1, 1)).alias("created_at"),
        pl.when(pl.col("ID_UDO_FK").is_not_null()).then(pl.col("ID_UDO_FK"))
        .when(pl.col("ID_UO_FK").is_not_null()).then(pl.col("ID_UO_FK"))
        .when(pl.col("ID_STRUTTURA_FK").is_not_null()).then(pl.col("ID_STRUTTURA_FK"))
        .when(pl.col("ID_EDIFICIO_FK").is_not_null()).then(pl.col("ID_EDIFICIO_FK"))
        .when(pl.col("ID_COMPRENSORIO_FK").is_not_null()).then(pl.col("ID_COMPRENSORIO_FK"))
        .otherwise(None)
        .alias("procedure_entity_id")
    ])

    df = df.select([
        pl.col("CLIENTID").alias("id"),
        pl.col("ROOT_ID_DOMANDA_FK").alias("root_procedure_id"),
        pl.col("ID_ASSEGNATARIO_FK").alias("response_assignee_user_id"),
        pl.col("VALUTAZIONE").alias("response"),
        pl.col("EVIDENZE").alias("evidence"),
        pl.col("ANNOTATIONS").alias("notes"),
        "updated_at", "created_at",
        pl.col("id").alias("requirement_id"),
        pl.col("name").alias("requirement_name"),
        pl.col("text").alias("requirement_text"),
        pl.col("annotations_right").alias("requirement_annotation"),
        pl.col("response_type"),
        pl.col("requirement_taxonomy_id"),
        pl.col("name_right").alias("requirement_taxonomy_name"),
        "procedure_entity_id"
    ])

    load_data(ctx, df, "procedure_entity_requirements")



def migrate_generic_procedure_entity(
    ctx: ETLContext,
    source_table: str,
    object_type: str,
    object_reference_col: str,
    object_data_expr: Callable[[pl.DataFrame], pl.Series],
    rename_map: dict,
    extra_columns: list[str] = None,
) -> None:
    """
    Migrate a procedure entity from Oracle to PostgreSQL.

    Parameters:
    - source_table: The Oracle source table name
    - object_type: Type of the entity (e.g., UDO, OPERATIONAL_UNIT, etc.)
    - object_reference_col: The FK column to the model/template ID
    - object_data_expr: A function to build the JSON object_data column
    - rename_map: Dictionary for column renaming
    - extra_columns: Additional fields to select from source (optional)
    """
    cols = ["CLIENTID", "ID_DOMANDA_FK", object_reference_col] + (extra_columns or [])
    df = extract_data(ctx, f"SELECT * FROM AUAC_USR.{source_table}").select(cols)

    df = df.with_columns([
        pl.lit(object_type).alias("object_type"),
        pl.lit(object_type).alias("object_reference_type"),
        object_data_expr(df).alias("object_data")
    ])

    # Apply default timestamps
    df = df.with_columns([
        pl.col("CREATION").fill_null(datetime(2024, 1, 1)).alias("created_at"),
        pl.col("LAST_MOD").fill_null(pl.col("CREATION")).alias("updated_at"),
    ])

    # Rename fields
    df = df.rename(rename_map)

    load_data(ctx, df, "procedure_entities")


def migrate_udo_inst_in_procedure_entities(ctx: ETLContext) -> None:
    migrate_generic_procedure_entity(
        ctx=ctx,
        source_table="UDO_INST",
        object_type="UDO",
        object_reference_col="ID_UDO_MODEL_FK",
        object_data_expr=lambda df: (
            pl.struct([
                pl.col("ID_UDO_MODEL_FK").alias("id"),
                pl.col("ID_UNIVOCO").alias("code"),
                pl.col("DESCR").alias("name"),
            ])
            .map_elements(lambda d: json.dumps(d))
        ),
        rename_map={
            "CLIENTID": "id",
            "ID_DOMANDA_FK": "procedure_id",
            "ID_UDO_MODEL_FK": "object_reference_id",
            "COMPRENSORIO_CLIENTID": "district_id",
            "CREATION": "created_at",
            "LAST_MOD": "updated_at"
        },
        extra_columns=[
            "ID_UNIVOCO", "COMPRENSORIO_CLIENTID", "CREATION", "LAST_MOD", "DESCR"
        ]
    )


def migrate_uo_inst_in_procedure_entities(ctx: ETLContext) -> None:
    migrate_generic_procedure_entity(
        ctx=ctx,
        source_table="UO_INST",
        object_type="OPERATIONAL_UNIT",
        object_reference_col="ID_UO",
        object_data_expr=lambda df: (
            pl.struct([
                pl.col("ID_UO").alias("code"),
                pl.col("PROVENIENZA_UO").alias("provenience")
            ])
            .map_elements(lambda d: json.dumps(d))
        ),
        rename_map={
            "CLIENTID": "id",
            "ID_DOMANDA_FK": "procedure_id",
            "ID_UO": "object_reference_id",
            "COMPRENSORIO_CLIENTID": "district_id",
            "CREATION": "created_at",
            "LAST_MOD": "updated_at"
        },
        extra_columns=[
            "ID_UO", "PROVENIENZA_UO", "COMPRENSORIO_CLIENTID", "CREATION", "LAST_MOD"
        ]
    )


def migrate_edificio_inst_in_procedure_entities(ctx: ETLContext) -> None:
    migrate_generic_procedure_entity(
        ctx=ctx,
        source_table="EDIFICIO_INST",
        object_type="BUILDING",
        object_reference_col="ID_EDIFICIO_STR_TEMPL_FK",
        object_data_expr=lambda df: (
            pl.col("CODICE").str.strip_chars().map_elements(lambda code: json.dumps({"code": code}))
        ),
        rename_map={
            "CLIENTID": "id",
            "ID_DOMANDA_FK": "procedure_id",
            "ID_EDIFICIO_STR_TEMPL_FK": "object_reference_id",
            "SEDE_OPER_MODEL_CLIENTID": "operational_office_id",
        },
        extra_columns=[
            "CODICE", "SEDE_OPER_MODEL_CLIENTID"
        ]
    )


def migrate_struttura_inst_in_procedure_entities(ctx: ETLContext) -> None:
    migrate_generic_procedure_entity(
        ctx=ctx,
        source_table="STRUTTURA_INST",
        object_type="PHYSICAL_STRUCTURE",
        object_reference_col="ID_STRUTTURA_MODEL_FK",
        object_data_expr=lambda df: (
            pl.col("CODICE_PF").map_elements(lambda code: json.dumps({"code": code}))
        ),
        rename_map={
            "CLIENTID": "id",
            "ID_DOMANDA_FK": "procedure_id",
            "ID_STRUTTURA_MODEL_FK": "object_reference_id"
        },
        extra_columns=[
            "CODICE_PF"
        ]
    )


def migrate_comprensorio_inst_in_procedure_entities(ctx: ETLContext) -> None:
    migrate_generic_procedure_entity(
        ctx=ctx,
        source_table="COMPRENSORIO_INST",
        object_type="DISTRICT",
        object_reference_col="ID_COMPRENSORIO_TEMPL_FK",
        object_data_expr=lambda df: (
            pl.col("DENOMINAZIONE").map_elements(lambda name: json.dumps({"name": name}))
        ),
        rename_map={
            "CLIENTID": "id",
            "ID_DOMANDA_FK": "procedure_id",
            "ID_COMPRENSORIO_TEMPL_FK": "object_reference_id"
        },
        extra_columns=[
            "DENOMINAZIONE"
        ]
    )
