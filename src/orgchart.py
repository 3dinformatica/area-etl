import uuid

import polars as pl

from core import ETLContext, extract_data, load_data, set_boolean_from_values_0_1, save_organigram_with_attachments, \
    save_regulation_with_attachments


def migrate_areas_sub_areas(ctx: ETLContext) -> None:
    """
    Migrate areas and sub areas from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_area = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.AREA")
    df_sub_area = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.SUB_AREA")

    ### TRANSFORM ###
    # Process areas
    df_area_result = df_area.select(
        pl.col("ID").alias("old_id"),
        pl.lit(None).cast(pl.String).alias("id"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description")
    )

    df_area_result = df_area_result.with_columns(
        pl.col("id").map_elements(lambda _: str(uuid.uuid4()), return_dtype=pl.String)
    )

    area_id_mapping = dict(zip(
        df_area_result.get_column("old_id").to_list(),
        df_area_result.get_column("id").to_list()
    ))

    # Process sub-areas
    df_sub_area_result = df_sub_area.select(
        pl.col("ID").alias("old_id"),
        pl.lit(None).cast(pl.String).alias("id"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description"),
        pl.col("ID_AREA").alias("area_id_old")
    )

    df_sub_area_result = df_sub_area_result.with_columns([
        pl.col("id").map_elements(lambda _: str(uuid.uuid4()), return_dtype=pl.String),
        pl.col("area_id_old").map_elements(
            lambda x: area_id_mapping.get(x),
            return_dtype=pl.String
        ).alias("area_id")
    ]).drop("area_id_old")

    ### LOAD ###
    load_data(ctx, df_area_result, "areas")
    load_data(ctx, df_sub_area_result, "sub_areas")


def migrate_legal_inquiries_types_legal_inquiries(ctx: ETLContext) -> None:
    """
    Migrate legal inquiries types and legal inquiries from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipi_inq_giur = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.TIPI_INQ_GIUR")
    df_inq_giur = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.INQ_GIUR")

    ### TRANSFORM ###
    # Process legal inquiries types
    df_tipi_inq_giur_result = df_tipi_inq_giur.select(
        pl.col("ID").alias("old_id"),
        pl.lit(None).cast(pl.String).alias("id"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description"),
        pl.col("SHOW_ANAGRAFICA_AUAC").alias("show_anagraphic_auac")
    )

    df_tipi_inq_giur_result = set_boolean_from_values_0_1(
        df_tipi_inq_giur_result,
        ["show_anagraphic_auac"]
    )

    df_tipi_inq_giur_result = df_tipi_inq_giur_result.with_columns(
        pl.col("id").map_elements(lambda _: str(uuid.uuid4()), return_dtype=pl.String)
    )

    legal_inquiries_type_mapping = dict(zip(
        df_tipi_inq_giur_result.get_column("old_id").to_list(),
        df_tipi_inq_giur_result.get_column("id").to_list()
    ))

    # Process legal inquiries
    df_inq_giur_result = df_inq_giur.select(
        pl.col("ID").alias("old_id"),
        pl.lit(None).cast(pl.String).alias("id"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description"),
        pl.col("TIPO_INQ_GIUR_ID").alias("legal_inquiries_type_id_old")
    )

    df_inq_giur_result = df_inq_giur_result.with_columns([
        pl.col("id").map_elements(lambda _: str(uuid.uuid4()), return_dtype=pl.String),
        pl.col("legal_inquiries_type_id_old").map_elements(
            lambda x: legal_inquiries_type_mapping.get(x),
            return_dtype=pl.String
        ).alias("legal_inquiries_type_id")
    ]).drop("legal_inquiries_type_id_old")

    ### LOAD ###
    load_data(ctx, df_tipi_inq_giur_result, "legal_inquiries_types")
    load_data(ctx, df_inq_giur_result, "legal_inquiries")


def migrate_organigrams(ctx: ETLContext) -> None:
    """
    Migrate organigrams from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_organigrams = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.ORGANIGRAMMA")
    df_stati_organigramma = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.STATI_ORGANIGRAMMA")
    df_titolare = extract_data(ctx, "SELECT * FROM AUAC_USR.TITOLARE_MODEL")

    ### TRANSFORM ###
    df_organigrams_result = df_organigrams.select(
        pl.col("ID").alias("id"),
        pl.col("VERSIONE").alias("version"),
        pl.col("STATO_CODICE").alias("stato_codice"),
        pl.col("DATA_VALIDAZIONE").alias("validated_at"),
        pl.col("MODELLO").alias("is_from_model"),
        pl.col("ID_SOGG_GIUR").alias("id_sogg_giur"),
        pl.col("DATA_CREAZIONE").alias("disabled_at"),
        pl.col("DATA_AGGIORNAMENTO").alias("updated_at")
    )

    df_organigrams_result = set_boolean_from_values_0_1(
        df_organigrams_result,
        ["is_from_model"]
    )

    df_stati_mapping = df_stati_organigramma.select(
        pl.col("CODICE").alias("codice"),
        pl.col("DESCRIZIONE").alias("descrizione")
    )

    df_titolare_mapping = df_titolare.select(
        pl.col("ID_TITOLARE").alias("id_titolare"),
        pl.col("CLIENTID").alias("clientid")
    )

    # Map status codes
    df_organigrams_result = df_organigrams_result.join(
        df_stati_mapping,
        left_on="stato_codice",
        right_on="codice",
        how="left"
    ).with_columns(
        pl.col("descrizione")
        .str.to_uppercase()
        .str.replace(" ", "_")
        .alias("status_code")
    ).drop(["stato_codice", "codice", "descrizione"])

    df_organigrams_result = df_organigrams_result.join(
        df_titolare_mapping,
        left_on="id_sogg_giur",
        right_on="id_titolare",
        how="left"
    ).select(
        pl.col("id"),
        pl.col("version"),
        pl.col("status_code"),
        pl.col("validated_at"),
        pl.col("is_from_model"),
        pl.col("clientid").alias("legal_entity_id"),
        pl.col("disabled_at"),
        pl.col("updated_at")
    )

    ### LOAD ###
    load_data(ctx, df_organigrams_result, "organigrams")


def migrate_models(ctx: ETLContext) -> None:
    """
    Migrate models from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_models = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.MODELLI")
    df_titolare = extract_data(ctx, "SELECT * FROM AUAC_USR.TITOLARE_MODEL")

    ### TRANSFORM ###
    df_models_result = df_models.select(
        pl.col("ID").alias("id"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description"),
        pl.col("ID_SOGGETTO_GIURIDICO").alias("id_soggetto_giuridico"),
        pl.col("ID_ORGANIGRAMMA").alias("organigram_id"),
        pl.col("DATA_CREAZIONE").alias("created_at"),
        pl.col("VERSIONE").alias("version")
    )

    df_titolare_mapping = df_titolare.select(
        pl.col("ID_TITOLARE").alias("id_titolare"),
        pl.col("CLIENTID").alias("clientid")
    )

    df_models_result = df_models_result.join(
        df_titolare_mapping,
        left_on="id_soggetto_giuridico",
        right_on="id_titolare",
        how="left"
    ).select(
        pl.col("id"),
        pl.col("description"),
        pl.col("clientid").alias("legal_entity_id"),
        pl.col("organigram_id"),
        pl.col("created_at"),
        pl.col("version")
    )

    ### LOAD ###
    load_data(ctx, df_models_result, "models")


def migrate_organigram_emails(ctx: ETLContext) -> None:
    """
    Migrate organigram emails from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_emails = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.EMAIL_POA")

    ### TRANSFORM ###
    df_emails_result = df_emails.select(
        pl.col("ID").alias("id"),
        pl.col("ADDRESS").str.strip_chars().alias("address"),
        pl.col("ORGANIGRAMMA_ID").alias("organigram_id")
    )

    ### LOAD ###
    load_data(ctx, df_emails_result, "organigram_emails")


def migrate_notifications(ctx: ETLContext) -> None:
    """
    Migrate notifications from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_notifications = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.NOTIFICHE")
    df_stati_organigramma = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.STATI_ORGANIGRAMMA")
    df_titolare = extract_data(ctx, "SELECT * FROM AUAC_USR.TITOLARE_MODEL")

    ### TRANSFORM ###
    df_notifications_result = df_notifications.select(
        pl.col("ID").alias("id"),
        pl.col("MESSAGE").str.strip_chars().alias("message"),
        pl.col("VISTO").alias("is_viewed"),
        pl.col("STATO_CODICE").alias("stato_codice"),
        pl.col("DATA_VISTO").alias("date_viewed"),
        pl.col("ID_MITTENTE").alias("id_mittente"),
        pl.col("ID_DESTINATARIO").alias("recipient_id"),
        pl.col("ID_ORGANIGRAMMA").alias("organigram_id")
    )

    df_notifications_result = set_boolean_from_values_0_1(
        df_notifications_result,
        ["is_viewed"]
    )

    df_stati_mapping = df_stati_organigramma.select(
        pl.col("CODICE").alias("codice"),
        pl.col("DESCRIZIONE").alias("descrizione")
    )

    df_titolare_mapping = df_titolare.select(
        pl.col("ID_TITOLARE").alias("id_titolare"),
        pl.col("CLIENTID").alias("clientid")
    )

    df_notifications_result = df_notifications_result.join(
        df_stati_mapping,
        left_on="stato_codice",
        right_on="codice",
        how="left"
    ).with_columns(
        pl.col("descrizione")
        .str.to_uppercase()
        .str.replace(" ", "_")
        .alias("organigram_status")
    ).drop(["stato_codice", "codice", "descrizione"])

    df_notifications_result = df_notifications_result.join(
        df_titolare_mapping,
        left_on="id_mittente",
        right_on="id_titolare",
        how="left"
    ).with_columns(
        pl.col("clientid").alias("sender_id")
    ).drop(["id_mittente", "id_titolare", "clientid"])

    ### LOAD ###
    load_data(ctx, df_notifications_result, "notifications")


def migrate_node_types(ctx: ETLContext) -> None:
    """
    Migrate node types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_node_types = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.TIPI_NODO")
    df_areas = extract_data(ctx, "SELECT * FROM areas", source="pg")
    df_legal_inquiries = extract_data(ctx, "SELECT * FROM legal_inquiries", source="pg")

    ### TRANSFORM ###
    df_node_types_result = df_node_types.select(
        pl.col("ID").alias("id"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description"),
        pl.col("ID_AREA").alias("id_area"),
        pl.col("INQUADRAMENTO_GIURIDICO_ID").alias("inquadramento_giuridico_id")
    )

    df_areas_mapping = df_areas.select(
        pl.col("old_id"),
        pl.col("id").alias("area_id")
    )

    df_legal_inquiries_mapping = df_legal_inquiries.select(
        pl.col("old_id"),
        pl.col("id").alias("legal_inquiries_id")
    )

    df_node_types_result = df_node_types_result.join(
        df_areas_mapping,
        left_on="id_area",
        right_on="old_id",
        how="left"
    ).drop(["id_area", "old_id"])

    df_node_types_result = df_node_types_result.join(
        df_legal_inquiries_mapping,
        left_on="inquadramento_giuridico_id",
        right_on="old_id",
        how="left"
    ).drop(["inquadramento_giuridico_id", "old_id"])

    ### LOAD ###
    load_data(ctx, df_node_types_result, "node_types")


def migrate_node(ctx: ETLContext) -> None:
    """
    Migrate nodes from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_nodes = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.NODI")
    df_sub_areas = extract_data(ctx, "SELECT * FROM sub_areas", source="pg")
    df_specialties = extract_data(ctx, "SELECT * FROM specialties", source="pg")

    ### TRANSFORM ###
    df_nodes_result = df_nodes.select(
        pl.col("ID").alias("id"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("CODICE_UNIVOCO").str.strip_chars().alias("code"),
        pl.col("DATA_INIZIO_VAL").alias("valid_from"),
        pl.col("DATA_FINE_VAL").alias("valid_to"),
        pl.col("DATA_DISMISSIONE").alias("dismission_date"),
        pl.col("NOTE").str.strip_chars().alias("annotations"),
        pl.col("VERSIONE").alias("version"),
        pl.col("PROGRESSIVO").alias("progressive"),
        pl.col("PRESIDIO").str.strip_chars().alias("presidium"),
        pl.col("SIGLA_COD_UNIVOCO_REG").str.strip_chars().alias("abbreviation_univocal_uo_code"),
        pl.col("FL_COORDINAMENTO").alias("is_coordination"),
        pl.col("FACENTE_FUNZIONE").alias("is_acting_functionary"),
        pl.col("ORGANIGRAMMA_VALIDATO").alias("is_valid_organigram"),
        pl.col("RUOLO_RESP").alias("role_of_responsible"),
        pl.col("ATTIVITA").alias("activity"),
        pl.col("STATO").alias("state"),
        pl.col("PROFILO").alias("profile"),
        pl.col("EMAIL").alias("email"),
        pl.col("RESPONSABILE").alias("responsible_id"),
        pl.col("NODO_PADRE_ID").alias("parent_node_id"),
        pl.col("ORGANIGRAMMA_ID").alias("organigram_id"),
        pl.col("TIPO_NODO_ID").alias("node_type_id"),
        pl.col("ID_SPECIALITA").alias("id_specialita"),
        pl.col("TIPO_SPECIALITA").alias("tipo_specialita"),
        pl.col("ID_SUB_AREA").alias("id_sub_area"),
        pl.col("DATA_INSERIMENTO").alias("created_at"),
        pl.col("DATA_MODIFICA").alias("updated_at")
    )

    df_nodes_result = set_boolean_from_values_0_1(
        df_nodes_result,
        ["is_coordination", "is_acting_functionary", "is_valid_organigram"]
    )

    specialty_type_mapping = {1: 'BRANCH', 2: 'DISCIPLIN'}

    # Convert specialty ID and type to string and handle nulls
    df_nodes_result = df_nodes_result.with_columns([
        pl.col("id_specialita")
        .cast(pl.String)
        .str.split(".")
        .list.first()
        .fill_null("0")
        .alias("id_specialita_str"),

        pl.col("tipo_specialita")
        .cast(pl.Int64)
        .map_elements(lambda x: specialty_type_mapping.get(x) if x is not None else None, return_dtype=pl.String)
        .alias("specialty_type")
    ])

    df_specialties_mapping = df_specialties.select(
        pl.col("old_id").cast(pl.String),
        pl.col("specialty_type"),
        pl.col("id").alias("specialty_id")
    )

    df_nodes_result = df_nodes_result.join(
        df_specialties_mapping,
        left_on=["id_specialita_str", "specialty_type"],
        right_on=["old_id", "specialty_type"],
        how="left"
    ).drop(["id_specialita", "tipo_specialita", "id_specialita_str", "old_id", "specialty_type"])

    df_sub_areas_mapping = df_sub_areas.select(
        pl.col("old_id"),
        pl.col("id").alias("sub_area_id")
    )

    df_nodes_result = df_nodes_result.join(
        df_sub_areas_mapping,
        left_on="id_sub_area",
        right_on="old_id",
        how="left"
    ).drop(["id_sub_area", "old_id"])

    df_nodes_result = df_nodes_result.with_columns([
        pl.col("activity")
        .str.to_uppercase()
        .str.replace(" ", "_")
        .fill_null(None),

        pl.col("state")
        .str.to_uppercase()
        .str.replace(" ", "_")
        .fill_null(None)
        .map_elements(lambda x: "ASSENTE" if x == "-" else x, return_dtype=pl.String)
    ])

    # TODO uuid non valido ne ho trovato solo 1 da sentire adrian
    df_nodes_result = df_nodes_result.with_columns(
        pl.when(pl.col("id") == "03b168d2-91e1-4953-99bf-ad2705f70cvv")
        .then(pl.lit("1e9e6b0c-4914-488f-9f7a-29a949c22471"))
        .otherwise(pl.col("id"))
        .alias("id")
    )

    ### LOAD ###
    load_data(ctx, df_nodes_result, "nodes")


def migrate_rule_types(ctx: ETLContext) -> None:
    """
    Migrate rule types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_rule_types = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.TIPI_REGOLA")

    ### TRANSFORM ###
    df_rule_types_result = df_rule_types.select(
        pl.col("TIPO").alias("type"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description")
    )

    df_rule_types_result = df_rule_types_result.with_columns(
        pl.lit(None).cast(pl.String).map_elements(lambda _: str(uuid.uuid4()), return_dtype=pl.String).alias("id")
    )

    ### LOAD ###
    load_data(ctx, df_rule_types_result, "rule_types")


def migrate_rules(ctx: ETLContext) -> None:
    """
    Migrate rules from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_rules = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.REGOLE")
    df_titolare = extract_data(ctx, "SELECT * FROM AUAC_USR.TITOLARE_MODEL")
    df_rule_types_old = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.TIPI_REGOLA")
    df_legal_inquiries_types = extract_data(ctx, "SELECT * FROM legal_inquiries_types", source="pg")
    df_rule_types = extract_data(ctx, "SELECT * FROM rule_types", source="pg")
    df_specialties = extract_data(ctx, "SELECT * FROM specialties", source="pg")

    ### TRANSFORM ###
    df_rules_result = df_rules.select(
        pl.col("ID").alias("id"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description"),
        pl.col("N_MAX").alias("max_num"),
        pl.col("N_MIN").alias("min_num"),
        pl.col("MAX_RELAZIONE").alias("max_relation"),
        pl.col("FIGLIO_VIETATO").alias("forbidden_child"),
        pl.col("PADRE_VIETATO").alias("forbidden_parent"),
        pl.col("PRESIDIO").str.strip_chars().alias("presidium"),
        pl.col("ID_TIPO_INQUADRAMENTO").alias("legal_inquiries_type_id_old"),
        pl.col("TIPO_SPECIALITA").alias("tipo_specialita"),
        pl.col("ID_SPECIALITA").alias("id_specialita"),
        pl.col("ID_TIPO_NODO").alias("node_type_id"),
        pl.col("TIPO_REGOLA").alias("tipo_regola"),
        pl.col("ID_NODO_FIGLIO").alias("child_node_type_id"),
        pl.col("ID_NODO_PADRE").alias("parent_node_type_id"),
        pl.col("ID_SOGGETTO_GIURIDICO").alias("id_soggetto_giuridico")
    )

    df_rules_result = set_boolean_from_values_0_1(
        df_rules_result,
        ["forbidden_child", "forbidden_parent"]
    )

    specialty_type_mapping = {1: 'BRANCH', 2: 'DISCIPLIN'}

    df_rules_result = df_rules_result.with_columns([
        pl.col("id_specialita")
        .cast(pl.String)
        .str.split(".")
        .list.first()
        .fill_null("0")
        .alias("id_specialita_str"),

        pl.col("tipo_specialita")
        .cast(pl.Int64)
        .map_elements(lambda x: specialty_type_mapping.get(x) if x is not None else None, return_dtype=pl.String)
        .alias("specialty_type")
    ])

    df_specialties_mapping = df_specialties.select(
        pl.col("old_id").cast(pl.String),
        pl.col("specialty_type"),
        pl.col("id").alias("specialty_id")
    )

    df_rules_result = df_rules_result.join(
        df_specialties_mapping,
        left_on=["id_specialita_str", "specialty_type"],
        right_on=["old_id", "specialty_type"],
        how="left"
    ).drop(["id_specialita", "tipo_specialita", "id_specialita_str", "old_id", "specialty_type"])

    df_legal_inquiries_types_mapping = df_legal_inquiries_types.select(
        pl.col("old_id"),
        pl.col("id").alias("legal_inquiries_type_id")
    )

    df_rules_result = df_rules_result.join(
        df_legal_inquiries_types_mapping,
        left_on="legal_inquiries_type_id_old",
        right_on="old_id",
        how="left"
    ).drop(["legal_inquiries_type_id_old", "old_id"])

    df_rule_types_mapping_old = df_rule_types_old.select(
        pl.col("TIPO").alias("tipo"),
        pl.col("DESCRIZIONE").alias("descrizione")
    )

    df_rules_result = df_rules_result.join(
        df_rule_types_mapping_old,
        left_on="tipo_regola",
        right_on="tipo",
        how="left"
    ).drop(["tipo_regola", "tipo"])

    df_rule_types_mapping_new = df_rule_types.select(
        pl.col("description"),
        pl.col("id").alias("rule_type_id")
    )

    df_rules_result = df_rules_result.join(
        df_rule_types_mapping_new,
        left_on="descrizione",
        right_on="description",
        how="left"
    ).drop(["descrizione", "description"])

    df_titolare_mapping = df_titolare.select(
        pl.col("ID_TITOLARE").alias("id_titolare"),
        pl.col("CLIENTID").alias("legal_entity_id")
    )

    df_rules_result = df_rules_result.join(
        df_titolare_mapping,
        left_on="id_soggetto_giuridico",
        right_on="id_titolare",
        how="left"
    ).drop(["id_soggetto_giuridico", "id_titolare"])

    df_rules_result = df_rules_result.with_columns(
        pl.lit(None).cast(pl.String).map_elements(lambda _: str(uuid.uuid4()), return_dtype=pl.String).alias("new_id")
    )

    df_rules_result = df_rules_result.with_columns(
        pl.col("new_id").alias("id")
    ).drop("new_id")

    ### LOAD ###
    load_data(ctx, df_rules_result, "rules")


def migrate_function_diagrams(ctx: ETLContext) -> None:
    """
    Migrate function diagrams from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_function_diagrams = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.FUNZIONIGRAMMA")
    df_titolare = extract_data(ctx, "SELECT * FROM AUAC_USR.TITOLARE_MODEL")

    ### TRANSFORM ###
    df_function_diagrams_result = df_function_diagrams.select(
        pl.col("ID").alias("id"),
        pl.col("DESCRIZIONE").str.strip_chars().alias("description"),
        pl.col("COORDINATORE").str.strip_chars().alias("coordinator"),
        pl.col("ID_SOGG_GIUR").alias("id_sogg_giur")
    )

    df_titolare_mapping = df_titolare.select(
        pl.col("ID_TITOLARE").alias("id_titolare"),
        pl.col("CLIENTID").alias("legal_entity_id")
    )

    df_function_diagrams_result = df_function_diagrams_result.join(
        df_titolare_mapping,
        left_on="id_sogg_giur",
        right_on="id_titolare",
        how="left"
    ).drop(["id_sogg_giur", "id_titolare"])

    ### LOAD ###
    load_data(ctx, df_function_diagrams_result, "function_diagrams")


def migrate_function_diagram_nodes(ctx: ETLContext) -> None:
    """
    Migrate function diagram nodes from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_function_diagram_nodes = extract_data(ctx, "SELECT * FROM AUAC_ORG_USR.FUNZ_NODI")

    ### TRANSFORM ###
    # Process function diagram nodes
    df_function_diagram_nodes_result = df_function_diagram_nodes.select(
        pl.col("FUNZ_ID").alias("function_diagram_id"),
        pl.col("NODO_ID").alias("node_id")
    )

    ### LOAD ###
    load_data(ctx, df_function_diagram_nodes_result, "function_diagram_nodes")


def migrate_organigram_attachments(ctx: ETLContext, storage_service_url: str, category: str) -> None:
    """
    Migrate organigram attachments from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context.
        storage_service_url: The url of the storage service.
        category: The category for save in the storage service.
    """
    ### EXTRACT ###
    df = extract_data(ctx, "SELECT organigramma_id, id_allegato_blob, descrizione FROM AUAC_ORG_USR.ALLEGATI")

    ### TRANSFORM ###
    df_clean = (
        df
        .filter(pl.col("organigramma_id").is_not_null())
        .rename({"organigramma_id": "organigram_id"})
    )

    # Save attachments via helper
    save_organigram_with_attachments(
        df_clean.select(["organigram_id", "id_allegato_blob", "descrizione"]),
        ctx.oracle_engine,
        storage_service_url,
        category,
        max_workers=20
    )
    df_final = df_clean.drop(["id_allegato_blob", "descrizione"])

    ### LOAD ###
    load_data(ctx, df_final, "organigram_attachments")


def migrate_regulation(ctx: ETLContext, storage_service_url: str, category: str) -> None:
    """
    Migrate regulations and attachments from source databases to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections and extract/load methods.
    """
    ### EXTRACT ###
    df_resolution_type = extract_data(
        ctx,
        f"SELECT id, name FROM resolution_types",
        source="pg"
    )
    df_normative = extract_data(
        ctx,
        f"SELECT id, oggetto, tipo, data, titolare FROM AUAC_ORG_USR.NORMATIVE",
        source="poa"
    )
    df_titolare = extract_data(
        ctx,
        f"SELECT clientid, id_titolare FROM AUAC_USR.TITOLARE_MODEL",
        source="oracle"
    )
    df_attachments = extract_data(
        ctx,
        f"SELECT normativa_id, id_allegato_blob, descrizione AS descrizione_allegato FROM AUAC_ORG_USR.ALLEGATI",
        source="poa"
    )

    ### TRANSFORM ###
    # Ensure ORGCHART resolution type exists
    df_res_types = df_resolution_type.filter(
        pl.col("name").str.contains("ORGCHART", case=False)
    )
    if df_res_types.is_empty():
        df_res_types = pl.DataFrame({"id": [uuid.uuid4()], "name": ["ORGCHART"]})
        load_data(
            ctx,
            df_res_types,
            "resolution_types",
        )
    res_type_id = df_res_types[0, "id"]

    # Prepare normative DataFrame
    df = (
        df_normative
        .select(["id", "oggetto", "tipo", "data", "titolare"])
        .with_column(
            pl.col("titolare").map_dict(
                dict(zip(
                    df_titolare["id_titolare"].to_list(),
                    df_titolare["clientid"].to_list()
                ))
            ).alias("company_id")
        )
        .join(
            df_attachments,
            left_on="id",
            right_on="normativa_id",
            how="left"
        )
    )
    save_regulation_with_attachments(
        df.select(["id", "id_allegato_blob", "descrizione_allegato"]),
        ctx.oracle_engine,
        storage_service_url,
        category,
        max_workers=20
    )

    # Initialize parent/child columns
    df = df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias("parent_resolution_id"),
        pl.lit(None).alias("new_id")
    ])
    # Assign relationships
    for grp in df.groupby("id"):
        rid = grp.rows()[0][grp.find_idx_by_name("id")]
        mask = pl.col("id") == rid
        # parent line: keep parent_resolution_id = None
        # children: set parent_resolution_id and assign new UUID
        df = df.with_columns([
            pl.when(mask.logical_not())
            .then(pl.lit(rid))
            .otherwise(pl.col("parent_resolution_id"))
            .alias("parent_resolution_id"),
            pl.when(mask.logical_not())
            .then(pl.Series([uuid.uuid4() for _ in range(df.filter(mask.logical_not()).height)]))
            .otherwise(pl.col("new_id"))
            .alias("new_id")
        ])
    # Replace id for children
    df = df.with_column(
        pl.when(pl.col("new_id").is_null())
        .then(pl.col("id"))
        .otherwise(pl.col("new_id"))
        .alias("id")
    )

    # Finalize and clean up columns
    df = (
        df
        .with_column(pl.lit(res_type_id).alias("resolution_type_id"))
        .with_column(pl.lit("ORGCHART").alias("category"))
        .with_column(
            pl.when(pl.col("parent_resolution_id").is_not_null())
            .then(pl.lit(None))
            .otherwise(pl.col("oggetto"))
            .alias("name")
        )
        .rename({"data": "valid_from"})
        .drop(["normativa_id", "id_allegato_blob", "descrizione_allegato", "new_id"])
    )

    ### LOAD ###
    load_data(
        ctx,
        df,
        "resolutions"
    )


def migrate_parameter(ctx: ETLContext) -> None:
    """
    Migrate function diagram nodes from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    data = {
        'key': ['EMAIL_NOTIFICA_ORG_REGIONE', "SEND_EMAIL_ORGANIGRAMMA_CAMBIO_STATO", "APPROVATORE_EMAIL"],
        'value': ["ufficio.regione@3di.it", "True", "approvatore@3di.it"],
        'annotations': [None, None, None]
    }

    ### TRANSFORM ###
    df_paramentri = pl.DataFrame(data)

    ### LOAD ###
    load_data(ctx, df_paramentri, "function_diagram_nodes")
