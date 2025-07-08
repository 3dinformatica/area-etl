import json
import logging
from datetime import datetime, timezone

import polars as pl

from core import ETLContext, extract_data, load_data


MUNICIPALITY_MAPPING = {
    "masera' di padova": "maserà di padova",
    "dolce'": "dolcè",
    "codogne'": "codognè",
    "arsie'": "arsiè",
    "arqua' polesine": "arquà polesine",
    "arqua' petrarca": "arquà petrarca",
    "carre'": "carrè",
    "mansu'": "mansuè",
    "erbe'": "erbè",
    "fosso'": "fossò",
    "palu'": "palù",
    "ponte san nicolo'": "ponte san nicolò",
    "portobuffole'": "portobuffolè",
    "ronca'": "roncà",
    "rosa'": "rosà",
    "roveredo di gua'": "roveredo di guà",
    "rovere' veronese": "roverè veronese",
    "san dona' di piave": "san donà di piave",
    "san nicolo' di comelico": "san nicolò di comelico",
    "scorze'": "scorzè",
    "sorga'": "sorgà",
    "zane'": "zanè",
    "zoppe' di cadore": "zoppè di cadore",
    "mansue'": "mansué",
}


def map_company_form(value: str) -> str | None:
    match value.lower().strip():
        case "s.c." | "s.c.s" | "s.c.s." | "s.s.":
            return "SOCIETA_SEMPLICE"
        case "s.n.c.":
            return "SOCIETA_IN_NOME_COLLETTIVO"
        case "s.a.s.":
            return "SOCIETA_IN_ACCOMANDITA_SEMPLICE"
        case "s.r.l.":
            return "SOCIETA_A_RESPONSABILITA_LIMITATA"
        case "s.r.l.s.":
            return "SOCIETA_A_RESPONSABILITA_LIMITATA_SEMPLIFICATA"
        case "s.p.a." | "s.p.a":
            return "SOCIETA_PER_AZIONI"
        case "s.a.p.a.":
            return "SOCIETA_IN_ACCOMANDITA_PER_AZIONI"
        case "comunita' montana":
            return "SOCIETA_IN_ACCOMANDITA_PER_AZIONI"
        case "consorzio":
            return "CONSORZIO"
        case "societa' cooperativa":
            return "SOCIETA_COOPERATIVA"
        case _:
            return None


def map_company_nature(value: str | None) -> str:
    value = value.lower().strip()
    if value.startswith("pub"):
        return "PUBBLICO"
    elif value.startswith("pri"):
        return "PRIVATO"
    elif value.startswith("azi"):
        return "AZIENDA_SANITARIA"
    else:
        return "PRIVATO"


def map_company_legal_form(value: str) -> str | None:
    match value.lower().strip():
        case "società" | "societa'":
            return "SOCIETA"
        case "impresa individuale":
            return "IMPRESA_INDIVIDUALE"
        case "consorzio":
            return "CONSORZIO"
        case "studio professionale":
            return "STUDIO_PROFESSIONALE"
        case "ente pubblico":
            return "ENTE_PUBBLICO"
        case "ente morale di diritto privato":
            return "ENTE_MORALE_DI_DIRITTO_PRIVATO"
        case "associazione":
            return "ASSOCIAZIONE"
        case "associazione temporanea di impresa":
            return "ASSOCIAZIONE_TEMPORANEA_DI_IMPRESA"
        case "ente ecclesiastico civilmente riconosciuto":
            return "ENTE_ECCLESIASTICO_CIVILMENTE_RICONOSCIUTO"
        case "fondazione":
            return "FONDAZIONE"
        case _:
            return None


def normalize_municipality_name(name: str) -> str:
    """Normalizza il nome del comune secondo le regole di mappatura."""
    if not name:
        return name
    name_lower = name.lower()
    return MUNICIPALITY_MAPPING.get(name_lower, name_lower)


def migrate_company_types(ctx: ETLContext) -> None:
    """
    Migrate company types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_company_types = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.TIPO_TITOLARE_TEMPL"
    )

    ### TRANSFORM ###
    df_result = df_company_types.select(
        pl.col("CLIENTID").str.to_lowercase().str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().alias("name"),
        pl.when(pl.col("SHOW_DICHIARAZIONE_DIR_SAN") == "S")
        .then(True)
        .otherwise(False)
        .alias("is_show_health_director_declaration"),
        pl.when(pl.col("ORGANIGRAMMA_ATTIVO") == "S")
        .then(True)
        .otherwise(False)
        .alias("is_active_organization_chart"),
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
    load_data(ctx, df_result, "company_types")


def migrate_companies(ctx: ETLContext) -> None:
    """
    Migrate companies from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_titolare_model = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.TITOLARE_MODEL"
    )

    df_tipologia_richiedente = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.TIPOLOGIA_RICHIEDENTE"
    ).select(
        pl.col("CLIENTID").alias("ID_TIPO_RICH_FK"),
        pl.col("DESCR").alias("company_legal_form"),
    )

    df_natura_titolare_templ = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.NATURA_TITOLARE_TEMPL"
    ).select(
        pl.col("CLIENTID").alias("ID_NATURA_FK"),
        pl.col("DESCR").alias("company_nature"),
    )

    df_municipalities = extract_data(
        ctx,
        "SELECT * FROM municipalities",
        source="pg"
    ).select(
        pl.col("id").alias("municipality_id"),
        pl.col("istat_code"),
    )

    ### TRANSFORM ###
    df_result = df_titolare_model.join(
        df_tipologia_richiedente,
        left_on="ID_TIPO_RICH_FK",
        right_on="ID_TIPO_RICH_FK",
        how="left",
    )
    df_result = df_result.join(
        df_natura_titolare_templ,
        left_on="ID_NATURA_FK",
        right_on="ID_NATURA_FK",
        how="left",
    )
    df_result = df_result.join(
        df_municipalities,
        left_on="COD_COMUNE_ESTESO",
        right_on="istat_code",
        how="left",
    )

    df_result = df_result.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("CODICEUNIVOCO").str.strip_chars().alias("code"),
        pl.col("RAG_SOC").str.strip_chars().alias("company_name"),
        pl.col("FORMA_SOCIETARIA")
        .str.to_lowercase()
        .str.strip_chars()
        .alias("company_form")
        .map_elements(map_company_form, return_dtype=pl.String),
        pl.col("company_legal_form")
        .str.to_lowercase()
        .str.strip_chars()
        .map_elements(map_company_legal_form, return_dtype=pl.String),
        pl.col("company_nature")
        .str.to_lowercase()
        .str.strip_chars()
        .map_elements(map_company_nature, return_dtype=pl.String)
        .fill_null("PRIVATO"),
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
    load_data(ctx, df_result, "companies")


def migrate_physical_structures(ctx: ETLContext) -> None:
    """
    Migrate physical structures from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_struttura_model = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.STRUTTURA_MODEL"
    )

    ### TRANSFORM ###
    df_result = df_struttura_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("CODICE_PF").str.strip_chars().alias("code"),
        pl.col("CODICE_PF_SECONDARIO").str.strip_chars().alias("secondary_code"),
        pl.col("ID_DISTRETTO_FK").str.strip_chars().alias("district_id"),
        pl.col("ID_TITOLARE_FK").str.strip_chars().alias("company_id"),
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
                "{}"
                if x["docway_file_id"] is None and x["area_id"] is None
                else json.dumps(x)
            ),
            return_dtype=pl.String,
        )
    )

    ### LOAD ###
    load_data(ctx, df_result, "physical_structures")


def migrate_operational_offices(ctx: ETLContext) -> None:
    """
    Migrate operational offices from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_sede_oper_model = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.SEDE_OPER_MODEL"
    )

    df_municipalities = extract_data(
        ctx,
        "SELECT * FROM municipalities",
        source="pg"
    ).select(
        pl.col("id"),
        pl.col("name").str.to_lowercase().alias("municipality_name"),
    )

    df_tipo_punto_fisico_templ = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.TIPO_PUNTO_FISICO_TEMPL"
    ).select(
        pl.col("CLIENTID"),
        pl.col("NOME"),
    )

    ### TRANSFORM ###
    df_sede_oper_model = df_sede_oper_model.with_columns(
        [
            pl.col("COMUNE")
            .str.to_lowercase()
            .map_elements(normalize_municipality_name, return_dtype=pl.String)
            .alias("normalized_comune"),
        ]
    )
    df_with_point_type = df_sede_oper_model.join(
        df_tipo_punto_fisico_templ,
        left_on="ID_TIPO_PUNTO_FISICO_FK",
        right_on="CLIENTID",
        how="left",
    )
    df_with_municipality = df_with_point_type.join(
        df_municipalities,
        left_on="normalized_comune",
        right_on="municipality_name",
        how="left",
    )
    df_result = df_with_municipality.select(
        [
            # ID e nome
            pl.col("CLIENTID").str.strip_chars().alias("id"),
            pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
            # Dati della struttura
            pl.col("ID_STRUTTURA_FK").str.strip_chars().alias("physical_structure_id"),
            # Dati dell'indirizzo
            pl.col("VIA_PIAZZA").str.strip_chars().alias("street_name"),
            pl.col("CIVICO").str.strip_chars().alias("street_number"),
            pl.col("CAP").alias("zip_code"),
            # Flag indirizzo principale
            pl.when(pl.col("FLAG_INDIRIZZO_PRINCIPALE") == "S")
            .then(True)
            .otherwise(False)
            .alias("is_main_address"),
            # Tipo di punto fisico
            pl.col("NOME").alias("physical_point_type"),
            # Coordinate
            pl.col("LATITUDINE").cast(pl.Float64).alias("lat"),
            pl.col("LONGITUDINE").cast(pl.Float64).alias("lon"),
            # ID di riferimento
            pl.col("ID_TOPONIMO_FK").str.strip_chars().alias("toponym_id"),
            pl.col("id").alias("municipality_id"),
            # Date
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
        ]
    )

    # Rimuovi i duplicati mantenendo il primo record per ogni ID
    df_result = df_result.unique(subset=["id"], keep="first")

    ### LOAD ###
    load_data(ctx, df_result, "operational_offices")


def migrate_buildings(ctx: ETLContext) -> None:
    """
    Migrate buildings from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_edificio_str_templ = extract_data(
        ctx,
        "SELECT * FROM AUAC_USR.EDIFICIO_STR_TEMPL"
    )

    ### TRANSFORM ###
    df_result = df_edificio_str_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("CODICE").str.strip_chars().alias("code"),
        pl.col("ID_STRUTTURA_FK").str.strip_chars().alias("physical_structure_id"),
        pl.col("CF_DI_PROPRIETA").str.strip_chars().alias("owner_tax_code"),
        pl.col("COGNOME_DI_PROPRIETA").str.strip_chars().alias("owner_last_name"),
        pl.col("NOME_DI_PROPRIETA").str.strip_chars().alias("owner_first_name"),
        pl.col("RAGIONE_SOCIALE_DI_PROPRIETA")
        .str.strip_chars()
        .alias("owner_business_name"),
        pl.col("PIVA_DI_PROPRIETA").str.strip_chars().alias("owner_vat_number"),
        pl.when(pl.col("FLAG_DI_PROPRIETA") == 1)
        .then(True)
        .otherwise(False)
        .alias("is_own_property"),
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
        pl.struct(
            [
                pl.col("ID_FASCICOLO_DOCWAY").alias("docway_file_id"),
            ]
        ).alias("extra"),
    )

    # Converti la colonna extra in JSON
    df_result = df_result.with_columns(
        pl.col("extra").map_elements(
            lambda x: "{}" if x["docway_file_id"] is None else json.dumps(x),
            return_dtype=pl.String,
        )
    )

    # Filtra il record specifico
    df_result = df_result.filter(pl.col("id") != "51830E93-379D-7D6D-E050-A8C083673C0F")

    ### LOAD ###
    load_data(ctx, df_result, "buildings")
