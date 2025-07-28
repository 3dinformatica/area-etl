import json

import polars as pl

from utils import ETLContext, extract_data, handle_enum_mapping, handle_timestamps, load_data

COMPANY_BUSINESS_FORM_MAPPING = {
    "s.c.": "SOCIETA_SEMPLICE",
    "s.c.s": "SOCIETA_SEMPLICE",
    "s.c.s.": "SOCIETA_SEMPLICE",
    "s.s.": "SOCIETA_SEMPLICE",
    "s.n.c.": "SOCIETA_IN_NOME_COLLETTIVO",
    "s.a.s.": "SOCIETA_IN_ACCOMANDITA_SEMPLICE",
    "s.r.l.": "SOCIETA_A_RESPONSABILITA_LIMITATA",
    "s.r.l.s.": "SOCIETA_A_RESPONSABILITA_LIMITATA_SEMPLIFICATA",
    "s.p.a.": "SOCIETA_PER_AZIONI",
    "s.p.a": "SOCIETA_PER_AZIONI",
    "s.a.p.a.": "SOCIETA_IN_ACCOMANDITA_PER_AZIONI",
    "comunita' montana": "SOCIETA_IN_ACCOMANDITA_PER_AZIONI",
    "consorzio": "CONSORZIO",
    "societa' cooperativa": "SOCIETA_COOPERATIVA",
}


COMPANY_NATURE_MAPPING = {
    "pub": "PUBBLICO",
    "pri": "PRIVATO",
    "azsan": "AZIENDA_SANITARIA",
}


COMPANY_LEGAL_FORM_MAPPING = {
    "società": "SOCIETA",
    "societa'": "SOCIETA",
    "impresa individuale": "IMPRESA_INDIVIDUALE",
    "consorzio": "CONSORZIO",
    "studio professionale": "STUDIO_PROFESSIONALE",
    "ente pubblico": "ENTE_PUBBLICO",
    "ente morale di diritto privato": "ENTE_MORALE_DI_DIRITTO_PRIVATO",
    "associazione": "ASSOCIAZIONE",
    "associazione temporanea di impresa": "ASSOCIAZIONE_TEMPORANEA_DI_IMPRESA",
    "ente ecclesiastico civilmente riconosciuto": "ENTE_ECCLESIASTICO_CIVILMENTE_RICONOSCIUTO",
    "fondazione": "FONDAZIONE",
}


def migrate_company_types(ctx: ETLContext) -> None:
    """
    Migrate company types from ORACLE table "AUAC_USR.TIPO_TITOLARE_TEMPL" to PostgreSQL table "company_types".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_company_types = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_TITOLARE_TEMPL")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_company_types.select(
        pl.col("CLIENTID").str.to_lowercase().str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().alias("name"),
        pl.when(pl.col("SHOW_DICHIARAZIONE_DIR_SAN") == "S")
        .then(True)
        .otherwise(False)
        .alias("is_show_health_director_declaration_poa"),
        pl.when(pl.col("ORGANIGRAMMA_ATTIVO") == "S")
        .then(True)
        .otherwise(False)
        .alias("is_active_poa"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )

    ### LOAD ###
    load_data(ctx, df_result, "company_types")


def migrate_companies(ctx: ETLContext) -> None:
    """
    Migrate companies from ORACLE table "AUAC_USR.TITOLARE_MODEL" to PostgreSQL table "companies".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_titolare_model = extract_data(ctx, "SELECT * FROM AUAC_USR.TITOLARE_MODEL")
    df_tipologia_richiedente = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPOLOGIA_RICHIEDENTE")
    df_natura_titolare_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.NATURA_TITOLARE_TEMPL")
    df_municipalities = extract_data(ctx, "SELECT * FROM municipalities", source="pg")

    ### TRANSFORM ###
    df_tipologia_richiedente_tr = df_tipologia_richiedente.select(
        pl.col("CLIENTID"),
        pl.col("DESCR").str.strip_chars().str.to_lowercase().alias("legal_form"),
    )
    df_natura_titolare_templ_tr = df_natura_titolare_templ.select(
        pl.col("CLIENTID"),
        pl.col("NOME").str.strip_chars().str.to_lowercase().alias("nature"),
    )
    df_municipalities_tr = df_municipalities.select(
        pl.col("id").alias("municipality_id"),
        pl.col("istat_code"),
    )
    df_result = df_titolare_model.join(
        df_tipologia_richiedente_tr,
        left_on="ID_TIPO_RICH_FK",
        right_on="CLIENTID",
        how="left",
    )
    df_result = df_result.join(
        df_natura_titolare_templ_tr,
        left_on="ID_NATURA_FK",
        right_on="CLIENTID",
        how="left",
    )
    df_result = df_result.join(
        df_municipalities_tr,
        left_on="COD_COMUNE_ESTESO",
        right_on="istat_code",
        how="left",
    )

    timestamp_exprs = handle_timestamps()

    df_result = df_result.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("CODICEUNIVOCO").str.strip_chars().alias("code"),
        pl.col("RAG_SOC").str.strip_chars().alias("business_name"),
        handle_enum_mapping(
            source_col="FORMA_SOCIETARIA",
            target_col="business_form",
            mapping_dict=COMPANY_BUSINESS_FORM_MAPPING,
        ),
        handle_enum_mapping(
            source_col="legal_form",
            target_col="legal_form",
            mapping_dict=COMPANY_LEGAL_FORM_MAPPING,
        ),
        handle_enum_mapping(
            source_col="nature",
            target_col="nature",
            mapping_dict=COMPANY_NATURE_MAPPING,
            default="PRIVATO",
        ).fill_null("PRIVATO"),
        pl.col("CFISC").str.strip_chars().alias("tax_code"),
        pl.col("PIVA").str.strip_chars().alias("vat_number"),
        pl.col("EMAIL").str.strip_chars().alias("email"),
        pl.col("PEC").alias("certified_email"),
        pl.col("TELEFONO").str.strip_chars().alias("phone"),
        pl.col("CELLULARE").str.strip_chars().alias("mobile_phone"),
        pl.col("URL").str.strip_chars().alias("website_url"),
        pl.col("VIA_PIAZZA").str.strip_chars().alias("street_name"),
        pl.col("CIVICO").str.strip_chars().alias("street_number"),
        pl.col("CAP").alias("zip_code"),
        pl.col("municipality_id"),
        pl.col("ID_TIPO_FK").str.strip_chars().alias("company_type_id"),
        pl.col("ID_TOPONIMO_FK").str.strip_chars().alias("toponym_id"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    ### LOAD ###
    load_data(ctx, df_result, "companies")


def migrate_physical_structures(ctx: ETLContext) -> None:
    """
    Migrate companies' physical structures from ORACLE to PostgreSQL.

    Transfers data from ORACLE table "AUAC_USR.STRUTTURA_MODEL" to PostgreSQL table
    "physical_structures".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_struttura_model = extract_data(ctx, "SELECT * FROM AUAC_USR.STRUTTURA_MODEL")

    ### TRANSFORM ###
    # Get timestamp expressions
    timestamp_exprs = handle_timestamps()

    df_result = df_struttura_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("CODICE_PF").str.strip_chars().alias("code"),
        pl.col("CODICE_PF_SECONDARIO").str.strip_chars().alias("secondary_code"),
        pl.col("ID_DISTRETTO_FK").str.strip_chars().alias("district_id"),
        pl.col("ID_TITOLARE_FK").str.strip_chars().alias("company_id"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
        pl.struct(
            [
                pl.col("ID_FASCICOLO_DOCWAY").alias("docway_file_id"),
                pl.col("ID_COMPRENSORIO_FK").alias("area_id"),
            ]
        ).alias("extra"),
    )

    df_result = df_result.with_columns(
        pl.col("extra").map_elements(
            lambda x: (
                "{}" if x["docway_file_id"] is None and x["area_id"] is None else json.dumps(x)
            ),
            return_dtype=pl.String,
        )
    )

    ### LOAD ###
    load_data(ctx, df_result, "physical_structures")


def migrate_operational_offices(ctx: ETLContext) -> None:
    """
    Migrate companies' operational offices from ORACLE to PostgreSQL.

    Transfers data from ORACLE table "AUAC_USR.SEDE_OPER_MODEL" to PostgreSQL table
    "operational_offices".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_sede_oper_model = extract_data(ctx, "SELECT * FROM AUAC_USR.SEDE_OPER_MODEL")
    df_municipalities = extract_data(ctx, "SELECT * FROM municipalities", source="pg")
    df_tipo_punto_fisico_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_PUNTO_FISICO_TEMPL")

    ### TRANSFORM ###
    df_municipalities_tr = df_municipalities.select(
        pl.col("id").alias("municipality_id"),
        pl.col("istat_code"),
    )
    df_tipo_punto_fisico_templ_tr = df_tipo_punto_fisico_templ.select(
        pl.col("CLIENTID"),
        pl.col("NOME"),
    )
    df_result = df_sede_oper_model.join(
        df_municipalities_tr,
        left_on="ISTAT",
        right_on="istat_code",
        how="left",
    )
    df_result = df_result.join(
        df_tipo_punto_fisico_templ_tr,
        left_on="ID_TIPO_PUNTO_FISICO_FK",
        right_on="CLIENTID",
        how="left",
    )

    timestamp_exprs = handle_timestamps()

    df_result = df_result.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("ID_STRUTTURA_FK").str.strip_chars().alias("physical_structure_id"),
        pl.col("VIA_PIAZZA").str.strip_chars().alias("street_name"),
        pl.col("CIVICO").str.strip_chars().alias("street_number"),
        pl.col("CAP").alias("zip_code"),
        pl.when(pl.col("FLAG_INDIRIZZO_PRINCIPALE") == "S")
        .then(True)
        .otherwise(False)
        .alias("is_main_address"),
        pl.col("NOME").alias("physical_point_type"),
        pl.col("LATITUDINE").cast(pl.Float64).alias("lat"),
        pl.col("LONGITUDINE").cast(pl.Float64).alias("lon"),
        pl.col("ID_TOPONIMO_FK").str.strip_chars().alias("toponym_id"),
        pl.col("municipality_id"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    ### LOAD ###
    load_data(ctx, df_result, "operational_offices")


def migrate_buildings(ctx: ETLContext) -> None:
    """
    Migrate companies' buildings from ORACLE table "AUAC_USR.EDIFICIO_STR_TEMPL" to PostgreSQL table "buildings".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_edificio_str_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.EDIFICIO_STR_TEMPL")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_edificio_str_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("CODICE").str.strip_chars().alias("code"),
        pl.col("ID_STRUTTURA_FK").str.strip_chars().alias("physical_structure_id"),
        pl.col("CF_DI_PROPRIETA").str.strip_chars().alias("owner_tax_code"),
        pl.col("COGNOME_DI_PROPRIETA").str.strip_chars().alias("owner_last_name"),
        pl.col("NOME_DI_PROPRIETA").str.strip_chars().alias("owner_first_name"),
        pl.col("RAGIONE_SOCIALE_DI_PROPRIETA").str.strip_chars().alias("owner_business_name"),
        pl.col("PIVA_DI_PROPRIETA").str.strip_chars().alias("owner_vat_number"),
        pl.when(pl.col("FLAG_DI_PROPRIETA") == 1)
        .then(True)
        .otherwise(False)
        .alias("is_own_property"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
        pl.struct(
            [
                pl.col("ID_FASCICOLO_DOCWAY").alias("docway_file_id"),
            ]
        ).alias("extra"),
    )

    # Convert extra column to JSON
    df_result = df_result.with_columns(
        pl.col("extra").map_elements(
            lambda x: "{}" if x["docway_file_id"] is None else json.dumps(x),
            return_dtype=pl.String,
        )
    )

    # TODO: Capire perchè non ha la physical struture associata
    df_result = df_result.filter(pl.col("id") != "51830E93-379D-7D6D-E050-A8C083673C0F")

    ### LOAD ###
    load_data(ctx, df_result, "buildings")
