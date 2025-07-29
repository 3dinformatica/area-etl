import json
import logging
import os
import shutil
import zipfile
from pathlib import Path

import polars as pl

from settings import settings
from utils import (
    ETLContext,
    extract_data,
    extract_data_from_csv,
    handle_enum_mapping,
    handle_timestamps,
    load_data,
    truncate_pg_table,
)

CORE_TABLES = [
    "buildings",
    "companies",
    "company_types",
    "districts",
    "grouping_specialties",
    "municipalities",
    "operational_offices",
    "operational_units",
    "permissions",
    "physical_structures",
    "production_factor_types",
    "production_factors",
    "provinces",
    "regions",
    "resolution_types",
    "resolutions",
    "specialties",
    "toponyms",
    "udo_production_factors",
    "udo_specialties",
    "udo_type_classifications",
    "udo_type_production_factor_types",
    "udo_types",
    "udos",
    "udos_history",
    "ulss",
    "user_companies",
    "users",
]


def truncate_core_tables(ctx: ETLContext) -> None:
    """
    Truncate all the tables in the PostgreSQL database of A.Re.A. Core service.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    logging.info(f"Truncating all target tables in PostgreSQL {ctx.pg_engine_core}...")

    for table in CORE_TABLES:
        truncate_pg_table(ctx.pg_engine_core, table)


### LOCATION ###


def migrate_regions(ctx: ETLContext) -> None:
    """
    Migrate regions from a local seed CSV file to PostgreSQL table "regions".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_regions = extract_data_from_csv("seed/regions.csv")

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_regions, "regions")


def migrate_provinces(ctx: ETLContext) -> None:
    """
    Migrate regions from a local seed CSV file to PostgreSQL table "provinces".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_provinces = extract_data_from_csv("seed/provinces.csv")

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_provinces, "provinces")


def migrate_municipalities(ctx: ETLContext) -> None:
    """
    Migrate regions from a local seed CSV file to PostgreSQL table "municipalities".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    schema_overrides = {"istat_code": pl.String}
    df_municipalities = extract_data_from_csv(
        "seed/municipalities_new.csv", schema_overrides=schema_overrides
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_municipalities, "municipalities")


def migrate_toponyms(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_USR.TOPONIMO_TEMPL" to PostgreSQL table "toponyms".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_toponimo_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TOPONIMO_TEMPL"
    )

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_toponimo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "toponyms")


def migrate_ulss(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_USR.ULSS_TERRITORIALE" to PostgreSQL table "ulss".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_ulss_territoriale = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.ULSS_TERRITORIALE"
    )

    ### TRANSFORM ###
    df_result = df_ulss_territoriale.select(
        pl.col("DESCRIZIONE").str.strip_chars().alias("name"),
        pl.col("CODICE").alias("code"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "ulss")


def migrate_districts(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_USR.DISTRETTO_TEMPL" to PostgreSQL table "districts".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_toponimo_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.DISTRETTO_TEMPL"
    )

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_toponimo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("TITOLARE")
        .str.strip_chars()
        .str.strip_suffix("-")
        .str.replace("-", " - ")
        .alias("name"),
        pl.col("DISTRETTO").alias("code"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "districts")


### COMPANY ###


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
    df_company_types = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_TITOLARE_TEMPL"
    )

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
    load_data(ctx.pg_engine_core, df_result, "company_types")


def migrate_companies(ctx: ETLContext) -> None:
    """
    Migrate companies from ORACLE table "AUAC_USR.TITOLARE_MODEL" to PostgreSQL table "companies".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_titolare_model = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TITOLARE_MODEL"
    )
    df_tipologia_richiedente = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPOLOGIA_RICHIEDENTE"
    )
    df_natura_titolare_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.NATURA_TITOLARE_TEMPL"
    )
    df_municipalities = extract_data(ctx.pg_engine_core, "SELECT * FROM municipalities")

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
    load_data(ctx.pg_engine_core, df_result, "companies")


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
    df_struttura_model = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.STRUTTURA_MODEL"
    )

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
    load_data(ctx.pg_engine_core, df_result, "physical_structures")


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
    df_sede_oper_model = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.SEDE_OPER_MODEL"
    )
    df_municipalities = extract_data(ctx.pg_engine_core, "SELECT * FROM municipalities")
    df_tipo_punto_fisico_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_PUNTO_FISICO_TEMPL"
    )

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
    load_data(ctx.pg_engine_core, df_result, "operational_offices")


def migrate_buildings(ctx: ETLContext) -> None:
    """
    Migrate companies' buildings from ORACLE table "AUAC_USR.EDIFICIO_STR_TEMPL" to PostgreSQL table "buildings".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_edificio_str_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.EDIFICIO_STR_TEMPL"
    )

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

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "buildings")


### SPECIALTY ###


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
    df_ragg_discpl = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.RAGG_DISCPL")

    df_macroarea_programmazione = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.MACROAREA_PROGRAMMAZIONE"
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
    load_data(ctx.pg_engine_core, df_result, "grouping_specialties")


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
    df_disciplina_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.DISCIPLINA_TEMPL"
    )
    df_branca_templ = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BRANCA_TEMPL")
    df_artic_branca_altro_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.ARTIC_BRANCA_ALTRO_TEMPL"
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
    load_data(ctx.pg_engine_core, df_result, "specialties")


### RESOLUTION ###


def download_attachments(
    ctx: ETLContext,
    df: pl.DataFrame,
    chunk_size: int = 500,
) -> None:
    """
    Download resolution attachments from Oracle and save them as a ZIP archive.

    This function extracts attachment files from the Oracle database based on file IDs
    in the provided DataFrame, saves them to a directory structure organized by resolution ID,
    creates a ZIP archive of all attachments, and then deletes the original directory.

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    df: pl.DataFrame
        DataFrame containing resolution data with file_id column
    chunk_size: int, default=500
        Number of file IDs to process in each database query chunk
    """
    df_resolutions_with_files = df.drop_nulls("file_id").select(["id", "file_id"])
    attachments_dir = Path(settings.ATTACHMENTS_DIR) / "resolutions"
    attachments_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Downloading {df_resolutions_with_files.height} attachments to {attachments_dir}")
    ids = pl.Series(df_resolutions_with_files.select("file_id")).to_list()
    id_chunks = [ids[i : i + chunk_size] for i in range(0, len(ids), chunk_size)]
    logging.info(
        f"{len(ids)} attachments divided into {len(id_chunks)} chunks with size {chunk_size}"
    )
    dfs_attachments = [
        extract_data(
            ctx.oracle_engine_area,
            f"SELECT * FROM AUAC_USR.BINARY_ATTACHMENTS_APPL WHERE CLIENTID IN ({','.join([f"'{id}'" for id in chunk])})",
        )
        for chunk in id_chunks
    ]
    df_attachments = pl.concat(dfs_attachments, how="vertical_relaxed")
    df_result = df_resolutions_with_files.join(
        df_attachments,
        left_on="file_id",
        right_on="CLIENTID",
        how="left",
    ).select(["id", "ALLEGATO", "NOME", "TIPO"])

    for row in df_result.iter_rows():
        resolution_id = row[0]
        attachment_bytes = row[1]
        attachment_name = row[2]
        resolution_dir = attachments_dir / resolution_id
        resolution_dir.mkdir(parents=True, exist_ok=True)
        safe_attachment_name = attachment_name.replace("/", "_").replace("\\", "_")

        with open(resolution_dir / safe_attachment_name, "wb") as f:
            f.write(attachment_bytes)

    # Create a ZIP file of the attachments directory
    logging.info(f"Creating ZIP archive of {attachments_dir}")
    zip_file_path = str(attachments_dir) + ".zip"
    with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(attachments_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, attachments_dir)
                zipf.write(file_path, arcname)

    # Delete the original directory after successful zipping
    logging.info(f"Deleting original directory {attachments_dir}")
    shutil.rmtree(attachments_dir)
    logging.info(f"ZIP archive created at {zip_file_path}")


def migrate_resolution_types(ctx: ETLContext) -> None:
    """
    Migrate resolution types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipo_delibera = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_DELIBERA")
    df_tipo_atto = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_ATTO")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_tipo_delibera = df_tipo_delibera.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().str.to_uppercase().alias("name"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )
    df_tipo_atto = df_tipo_atto.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().str.to_uppercase().alias("name"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )
    df_result = pl.concat([df_tipo_delibera, df_tipo_atto], how="vertical")
    df_result = df_result.unique("name")

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "resolution_types")


def migrate_resolutions(ctx: ETLContext) -> None:
    """
    Migrate resolutions from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_delibera_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.DELIBERA_TEMPL"
    )
    df_atto_model = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.ATTO_MODEL")
    df_tipo_proc_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_PROC_TEMPL"
    )
    df_tipo_delibera = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_DELIBERA")
    df_tipo_atto = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_ATTO")
    df_resolution_types = extract_data(ctx.pg_engine_core, "SELECT * FROM resolution_types")

    ### TRANSFORM ###
    # Column "id" is read as an object and not as a string
    df_resolution_types = df_resolution_types.to_pandas()
    df_resolution_types["id"] = df_resolution_types["id"].astype("string")
    df_resolution_types = pl.from_pandas(df_resolution_types)

    timestamp_exprs = handle_timestamps()

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
        pl.col("ID_ALLEGATO_FK").alias("file_id"),  # Will be processed by download_attachments
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
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
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
        pl.col("ID_ALLEGATO_FK").alias("file_id"),  # Will be processed by download_attachments
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
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
    download_attachments(ctx, df_result, chunk_size=500)

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "resolutions")


### UDO ###


def migrate_operational_units(ctx: ETLContext) -> None:
    """
    Migrate operational units from Oracle to PostgreSQL.

    Transfers data from the Oracle table "AUAC_USR.UO_MODEL" to the PostgreSQL table
    "operational_units".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_uo_model = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.UO_MODEL")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_uo_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("COD_UNIVOCO_UO").str.strip_chars().alias("code"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("DESCR").str.strip_chars().alias("description"),
        pl.col("ID_TITOLARE_FK").str.strip_chars().alias("company_id"),
        # Get timestamp expressions
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "operational_units")


def migrate_production_factor_types(ctx: ETLContext) -> None:
    """
    Migrate production factor types from Oracle to PostgreSQL.

    Transfers data from the Oracle table "AUAC_USR.TIPO_FATTORE_PROD_TEMPL" to the PostgreSQL table
    "production_factor_types".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipo_fattore_prod_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_FATTORE_PROD_TEMPL"
    )

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_tipo_fattore_prod_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("DESCR").str.strip_chars().str.replace_all(r"\s+", " ").alias("code"),
        pl.col("TIPOLOGIA_FATT_PROD").str.strip_chars().alias("category"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "production_factor_types")


def migrate_production_factors(ctx: ETLContext) -> None:
    """
    Migrate production factors from Oracle to PostgreSQL.

    Transfers data from the Oracle table "AUAC_USR.FATT_PROD_UDO_MODEL" to the PostgreSQL table
    "production_factors".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_fatt_prod_udo_model = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.FATT_PROD_UDO_MODEL"
    )

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_fatt_prod_udo_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("ID_TIPO_FK").str.strip_chars().alias("production_factor_type_id"),
        pl.col("VALORE")
        .str.strip_chars()
        .replace(["", "?"], "0")
        .fill_null("0")
        .cast(pl.UInt16)
        .alias("num_beds"),
        pl.col("VALORE3")
        .str.strip_chars()
        .replace(["", "?"], "0")
        .fill_null("0")
        .cast(pl.UInt16)
        .alias("num_hospital_beds"),
        pl.col("VALORE2")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .replace(["NUL"], None)
        .str.replace_all("\x00", "")
        .alias("room_name"),
        pl.col("DESCR")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .replace(["NUL"], None)
        .str.replace_all("\x00", "")
        .alias("room_code"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "production_factors")


def migrate_udo_type_classifications(ctx: ETLContext) -> None:
    """
    Migrate UDO type classifications from Oracle to PostgreSQL.

    Transfers data from the Oracle table "AUAC_USR.CLASSIFICAZIONE_UDO_TEMPL" to the PostgreSQL table
    "udo_type_classifications".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_classificazione_udo_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.CLASSIFICAZIONE_UDO_TEMPL"
    )

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_classificazione_udo_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "udo_type_classifications")


def migrate_udo_types(ctx: ETLContext) -> None:
    """
    Migrate UDO types from Oracle to PostgreSQL.

    Transfers data from multiple Oracle tables related to UDO types to the PostgreSQL table
    "udo_types".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipo_udo_22_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.TIPO_UDO_22_TEMPL"
    )
    df_bind_tipo_22_ambito = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_TIPO_22_AMBITO"
    )
    df_ambito_templ = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.AMBITO_TEMPL")
    df_bind_tipo_22_natura = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_TIPO_22_NATURA"
    )
    df_natura_titolare_templ = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.NATURA_TITOLARE_TEMPL"
    )
    df_bind_tipo_22_flusso = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_TIPO_22_FLUSSO"
    )
    df_flusso_templ = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.FLUSSO_TEMPL")

    ### TRANSFORM ###
    df_tipo_udo_22_templ = df_tipo_udo_22_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("CLIENTID_TIPO_UDO_22_TEMPL"),
        pl.col("DESCR").str.strip_chars(),
        pl.col("CODICE_UDO").str.strip_chars(),
        pl.col("NOME_CODICE_UDO").str.strip_chars(),
        pl.col("SETTING").str.strip_chars(),
        pl.col("TARGET").str.strip_chars(),
        pl.col("ID_CLASSIFICAZIONE_UDO_FK"),
        pl.when(pl.col("OSPEDALIERO").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("OSPEDALIERO"),
        pl.when(pl.col("SALUTE_MENTALE").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("SALUTE_MENTALE"),
        pl.when(pl.col("POSTI_LETTO").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("POSTI_LETTO"),
        pl.col("DISABLED"),
        pl.col("CREATION"),
        pl.col("LAST_MOD"),
    )

    # Clean and transform the binding tables
    df_bind_tipo_22_ambito = df_bind_tipo_22_ambito.select(
        pl.col("ID_AMBITO_FK"),
        pl.col("ID_TIPO_22_FK"),
    )

    df_ambito_templ = df_ambito_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("CLIENTID_AMBITO_TEMPL"),
        pl.col("NOME").str.strip_chars().alias("AMBITO_NOME"),
        pl.col("DESCR").str.strip_chars().alias("AMBITO_DESCR"),
        pl.when(
            pl.col("AGGIUNGI_DISCIPLINE").str.strip_chars().str.to_lowercase().is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_DISCIPLINE"),
        pl.when(pl.col("AGGIUNGI_BRANCHE").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_BRANCHE"),
        pl.when(
            pl.col("AGGIUNGI_PRESTAZIONI").str.strip_chars().str.to_lowercase().is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_PRESTAZIONI"),
        pl.when(pl.col("AGGIUNGI_AMBITO").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_AMBITO"),
        pl.when(
            pl.col("AGGIUNGI_DISCIPLINE_AZ_SAN")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_DISCIPLINE_AZ_SAN"),
        pl.when(
            pl.col("AGGIUNGI_DISCIPLINE_PUB_PRIV")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_DISCIPLINE_PUB_PRIV"),
        pl.when(
            pl.col("AGGIUNGI_BRANCHE_AZ_SAN").str.strip_chars().str.to_lowercase().is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_BRANCHE_AZ_SAN"),
        pl.when(
            pl.col("AGGIUNGI_BRANCHE_PUB_PRIV")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_BRANCHE_PUB_PRIV"),
    )

    df_bind_tipo_22_natura = df_bind_tipo_22_natura.select(
        pl.col("ID_NATURA_FK"),
        pl.col("ID_TIPO_UDO_22_FK"),
    )

    df_natura_titolare_templ = df_natura_titolare_templ.select(
        pl.col("CLIENTID"),
        pl.col("NOME").str.strip_chars(),
    )

    df_bind_tipo_22_flusso = df_bind_tipo_22_flusso.select(
        pl.col("ID_FLUSSO_FK"),
        pl.col("ID_TIPO_UDO_22_FK"),
    )

    df_flusso_templ = df_flusso_templ.select(
        pl.col("CLIENTID"),
        pl.col("NOME").str.strip_chars(),
    )

    # Join tables to create the base result
    df_result = df_tipo_udo_22_templ.join(
        df_bind_tipo_22_ambito,
        left_on="CLIENTID_TIPO_UDO_22_TEMPL",
        right_on="ID_TIPO_22_FK",
        how="left",
    )
    df_result = df_result.join(
        df_ambito_templ,
        left_on="ID_AMBITO_FK",
        right_on="CLIENTID_AMBITO_TEMPL",
        how="left",
    )

    # Process company natures (nature)
    # First, get all natures for each UDO type
    df_natures = df_bind_tipo_22_natura.join(
        df_natura_titolare_templ,
        left_on="ID_NATURA_FK",
        right_on="CLIENTID",
        how="left",
    )

    # Group by UDO type and collect natures into a list
    df_natures_grouped = df_natures.group_by("ID_TIPO_UDO_22_FK").agg(
        pl.col("NOME").alias("NATURE")
    )

    # Map nature names to standardized values and ensure we're using Python lists, not NumPy arrays
    df_natures_grouped = df_natures_grouped.with_columns(
        pl.col("NATURE").map_elements(
            lambda x: [
                "AZIENDA_SANITARIA"
                if item == "AzSan"
                else "PUBBLICO"
                if item == "Pub"
                else "PRIVATO"
                if item == "Pri"
                else item
                for item in (list(x) if x is not None else [])
            ],
            return_dtype=pl.List,
        )
    )

    # Process ministerial flows (flussi)
    # First, get all flows for each UDO type
    df_flows = df_bind_tipo_22_flusso.join(
        df_flusso_templ,
        left_on="ID_FLUSSO_FK",
        right_on="CLIENTID",
        how="left",
    )

    # Group by UDO type and collect flows into a list
    df_flows_grouped = df_flows.group_by("ID_TIPO_UDO_22_FK").agg(pl.col("NOME").alias("FLUSSI"))

    # Clean and standardize flow names and ensure we're using Python lists, not NumPy arrays
    df_flows_grouped = df_flows_grouped.with_columns(
        pl.col("FLUSSI").map_elements(
            lambda x: [
                item.replace(" ", "_").replace(".", "_") for item in list(x) if item is not None
            ]
            if x is not None
            else [],
            return_dtype=pl.List,
        )
    )

    # Join natures and flows to the result
    df_result = df_result.join(
        df_natures_grouped,
        left_on="CLIENTID_TIPO_UDO_22_TEMPL",
        right_on="ID_TIPO_UDO_22_FK",
        how="left",
    )

    df_result = df_result.join(
        df_flows_grouped,
        left_on="CLIENTID_TIPO_UDO_22_TEMPL",
        right_on="ID_TIPO_UDO_22_FK",
        how="left",
    )

    # Fill null arrays with empty arrays
    df_result = df_result.with_columns(
        pl.col("NATURE").fill_null([]),
        pl.col("FLUSSI").fill_null([]),
    )

    # Filter out records with empty scope_name
    df_result = df_result.filter(
        pl.col("AMBITO_NOME").is_not_null() & (pl.col("AMBITO_NOME") != "")
    )

    timestamp_exprs = handle_timestamps()

    # Rename columns to match the target schema
    df_result = df_result.select(
        pl.col("CLIENTID_TIPO_UDO_22_TEMPL").alias("id"),
        pl.col("DESCR").alias("name"),
        pl.col("CODICE_UDO").alias("code"),
        pl.col("NOME_CODICE_UDO").alias("code_name"),
        pl.col("SETTING").alias("setting"),
        pl.col("TARGET").alias("target"),
        pl.col("ID_CLASSIFICAZIONE_UDO_FK").alias("udo_type_classification_id"),
        pl.col("OSPEDALIERO").alias("is_hospital"),
        pl.col("SALUTE_MENTALE").alias("is_mental_health"),
        pl.col("POSTI_LETTO").alias("has_beds"),
        pl.col("AMBITO_NOME").alias("scope_name"),
        pl.col("AMBITO_DESCR").alias("scope_description"),
        pl.col("AGGIUNGI_DISCIPLINE").alias("has_disciplines"),
        pl.col("AGGIUNGI_DISCIPLINE_AZ_SAN").alias("has_disciplines_only_healthcare_company"),
        pl.col("AGGIUNGI_DISCIPLINE_PUB_PRIV").alias(
            "has_disciplines_only_public_or_private_company"
        ),
        pl.col("AGGIUNGI_BRANCHE").alias("has_branches"),
        pl.col("AGGIUNGI_BRANCHE_AZ_SAN").alias("has_branches_only_healthcare_company"),
        pl.col("AGGIUNGI_BRANCHE_PUB_PRIV").alias("has_branches_only_public_or_private_company"),
        pl.col("AGGIUNGI_PRESTAZIONI").alias("has_services"),
        pl.col("AGGIUNGI_AMBITO").alias("has_scopes"),
        pl.col("NATURE").alias("company_natures"),
        pl.col("FLUSSI").alias("ministerial_flows"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    ### LOAD ###
    # Convert array columns to PostgreSQL array format to ensure compatibility
    df_result = df_result.with_columns(
        [
            pl.col("company_natures")
            .map_elements(
                lambda x: "{" + ",".join(f'"{item!s}"' for item in x if item is not None) + "}"
                if x is not None
                else "{}",
                return_dtype=pl.Utf8,
            )
            .alias("company_natures"),
            pl.col("ministerial_flows")
            .map_elements(
                lambda x: "{" + ",".join(f'"{item!s}"' for item in x if item is not None) + "}"
                if x is not None
                else "{}",
                return_dtype=pl.Utf8,
            )
            .alias("ministerial_flows"),
        ]
    )

    load_data(ctx.pg_engine_core, df_result, "udo_types")


def migrate_udos(ctx: ETLContext) -> None:
    """
    Migrates UDO (Unità di Offerta) data from source to target database.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_udo_model = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.UDO_MODEL")
    df_sede_oper_model = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.SEDE_OPER_MODEL"
    )
    df_struttura_model = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.STRUTTURA_MODEL"
    )
    df_uo_model = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.UO_MODEL")

    ### TRANSFORM ###
    timestamp_exprs = handle_timestamps()

    df_result = df_udo_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR")
        .str.strip_chars()
        .str.replace_all("\n", "")
        .str.replace_all("\r", "")
        .alias("name"),
        pl.col("STATO").str.strip_chars().str.to_uppercase().fill_null("NUOVA").alias("status"),
        pl.col("ID_UNIVOCO")
        .str.strip_chars()
        .str.replace_all("\n", "")
        .str.replace_all("\r", "")
        .alias("code"),
        pl.col("ID_TIPO_UDO_22_FK").str.strip_chars().alias("udo_type_id"),
        pl.col("ID_SEDE_FK").str.strip_chars().alias("operational_office_id"),
        pl.col("ID_EDIFICIO_STR_FK").str.strip_chars().alias("building_id"),
        pl.col("PIANO").str.strip_chars().alias("floor"),
        pl.col("BLOCCO").str.strip_chars().replace("-", None).alias("block"),
        pl.col("PROGRESSIVO").str.strip_chars().replace("-", None).alias("progressive"),
        pl.col("CODICE_FLUSSO_MINISTERIALE").str.strip_chars().alias("ministerial_code"),
        pl.col("COD_FAR_FAD").str.strip_chars().alias("farfad_code"),
        pl.when(pl.col("SIO").str.strip_chars().str.to_lowercase() == "y")
        .then(True)
        .otherwise(False)
        .alias("is_sio"),
        pl.col("STAREP").str.strip_chars().alias("starep_code"),
        pl.col("CDC").str.strip_chars().alias("cost_center"),
        pl.col("PAROLE_CHIAVE").str.strip_chars().alias("keywords"),
        pl.col("ANNOTATIONS")
        .str.strip_chars()
        .str.replace_all("\n", "")
        .str.replace_all("\r", "")
        .alias("notes"),
        pl.when(pl.col("WEEK").str.strip_chars().str.to_lowercase() == "y")
        .then(True)
        .otherwise(False)
        .alias("is_open_only_on_business_days"),
        pl.when(pl.col("AUAC") == 1).then(True).otherwise(False).alias("is_auac"),
        pl.when(pl.col("FLAG_MODULO").str.strip_chars().str.to_lowercase() == "y")
        .then(True)
        .otherwise(False)
        .alias("is_module"),
        pl.lit(None).alias("organigram_node_id"),  # TODO: Link with poa-service
        pl.when(pl.col("PROVENIENZA_UO") == "ORGANIGRAMMA_TREE")
        .then(None)
        .otherwise(pl.col("ID_UO"))
        .alias("ID_UO"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    df_1 = df_sede_oper_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("operational_office_id"),
        pl.col("ID_STRUTTURA_FK").str.strip_chars().alias("physical_structure_id"),
    )

    df_2 = df_struttura_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("physical_structure_id"),
        pl.col("ID_TITOLARE_FK").str.strip_chars().alias("company_id"),
    )

    df_x = df_1.join(df_2, on="physical_structure_id", how="left").select(
        pl.col("operational_office_id"),
        pl.col("company_id"),
    )

    df_result = df_result.join(df_x, on="operational_office_id", how="left")

    df_z = df_uo_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("operational_unit_id"),
        pl.col("ID_UO").str.strip_chars(),
    )

    df_result = df_result.join(df_z, on="ID_UO", how="left")

    df_result = df_result.drop("ID_UO")

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "udos")


def migrate_udo_production_factors(ctx: ETLContext) -> None:
    """
    Migrates the relationship between UDOs and production factors.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_udo_fatt_prod = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_UDO_FATT_PROD"
    )

    ### TRANSFORM ###
    df_result = df_bind_udo_fatt_prod.select(
        pl.col("ID_FATTORE_FK").str.strip_chars().alias("production_factor_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "udo_production_factors")


def migrate_udo_type_production_factor_types(ctx: ETLContext) -> None:
    """
    Migrates the relationship between UDO types and production factor types.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_tipo_22_tipo_fatt = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_TIPO_22_TIPO_FATT"
    )

    ### TRANSFORM ###
    df_result = df_bind_tipo_22_tipo_fatt.select(
        pl.col("ID_TIPO_UDO_22_FK").str.strip_chars().alias("udo_type_id"),
        pl.col("ID_TIPO_FATT_FK").str.strip_chars().alias("production_factor_type_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "udo_type_production_factor_types")


def migrate_udo_specialties(ctx: ETLContext) -> None:
    """
    Migrates specialty data related to UDOs.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_udo_branca = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_UDO_BRANCA"
    )
    df_bind_udo_branca_altro = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_UDO_BRANCA_ALTRO"
    )
    df_bind_udo_disciplina = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_UDO_DISCIPLINA"
    )
    df_uo_model = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.UO_MODEL")

    ### TRANSFORM ###
    df_bind_udo_branca_tr = df_bind_udo_branca.select(
        pl.when(pl.col("AUTORIZZATA").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("is_authorized"),
        pl.when(pl.col("ACCREDITATA").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("is_accredited"),
        pl.lit(None).alias("num_beds"),
        pl.lit(None).alias("num_extra_beds"),
        pl.lit(None).alias("num_mortuary_beds"),
        pl.lit(None).alias("num_accredited_beds"),
        pl.lit(None).alias("hsp12"),
        pl.lit(None).alias("clinical_operational_unit_id"),
        pl.lit(None).alias("clinical_poa_node_id"),
        pl.col("ID_BRANCA_FK").str.strip_chars().alias("specialty_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
    )
    df_bind_udo_branca_altro_tr = df_bind_udo_branca_altro.select(
        pl.lit(False).alias("is_authorized"),
        pl.lit(False).alias("is_accredited"),
        pl.lit(None).alias("num_beds"),
        pl.lit(None).alias("num_extra_beds"),
        pl.lit(None).alias("num_mortuary_beds"),
        pl.lit(None).alias("num_accredited_beds"),
        pl.lit(None).alias("hsp12"),
        pl.lit(None).alias("clinical_operational_unit_id"),
        pl.lit(None).alias("clinical_poa_node_id"),
        pl.col("ID_ARTIC_BRANCA_ALTRO_FK").str.strip_chars().alias("specialty_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
    )
    df_result_branches = pl.concat(
        [df_bind_udo_branca_tr, df_bind_udo_branca_altro_tr], how="vertical_relaxed"
    )

    df_bind_udo_disciplina_tr = df_bind_udo_disciplina.filter(
        pl.col(
            "ID_DISCIPLINA_FK"
        ).is_not_null()  # TODO: Siamo sicuri che sia giusto togliere tutti quelli con specialty_id null?
    ).select(
        pl.lit(False).alias("is_authorized"),
        pl.lit(False).alias("is_accredited"),
        pl.col("POSTI_LETTO").alias("num_beds"),
        pl.col("POSTI_LETTO_EXTRA").alias("num_extra_beds"),
        pl.col("POSTI_LETTO_OBI").alias("num_mortuary_beds"),
        pl.col("POSTI_LETTO_ACC").alias("num_accredited_beds"),
        pl.col("HSP12").str.strip_chars().alias("hsp12"),
        pl.lit(None).alias("clinical_poa_node_id"),
        pl.col("ID_DISCIPLINA_FK").str.strip_chars().alias("specialty_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
        pl.col("ID_UO").alias("ID_UO"),
        pl.col("PROVENIENZA_UO").alias("PROVENIENZA_UO"),
    )
    df_uo_model_tr = df_uo_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("clinical_operational_unit_id"),
        pl.col("ID_UO").alias("ID_UO"),
    )
    df_result_disciplines = df_bind_udo_disciplina_tr.join(
        df_uo_model_tr,
        on="ID_UO",
        how="left",
    )
    df_result_disciplines = df_result_disciplines.drop(["ID_UO", "PROVENIENZA_UO"])

    df_result = pl.concat([df_result_branches, df_result_disciplines], how="diagonal_relaxed")

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "udo_specialties")


def migrate_udo_resolutions(ctx: ETLContext) -> None:
    """
    Migrates resolution data for UDOs.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_atto_udo = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.BIND_ATTO_UDO")

    ### TRANSFORM ###
    df_result = df_bind_atto_udo.select(
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
        pl.col("ID_ATTO_FK").str.strip_chars().alias("resolution_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "udo_resolutions")


def migrate_udos_history(ctx: ETLContext) -> None:
    """
    Migrates UDO status history data.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    # Extract main status data
    df_stato_udo = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.STATO_UDO")

    # Extract UDO data for supply information
    df_udo = extract_data(
        ctx.oracle_engine_area,
        "SELECT CLIENTID, EROGAZIONE_DIRETTA, EROGAZIONE_INDIRETTA FROM AUAC_USR.UDO_MODEL",
    )

    # Extract bed history data
    df_beds = extract_data(
        ctx.oracle_engine_area,
        "SELECT ID_STATO_UDO_FK, PL, PLEX, PLOB FROM AUAC_USR.STORICO_POSTI_LETTO",
    )

    ### TRANSFORM ###
    # Clean and transform the main status data
    df_stato_udo = df_stato_udo.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
        pl.col("STATO").str.strip_chars().str.to_uppercase().alias("status"),
        pl.col("SCADENZA").alias("valid_to"),
        pl.col("DATA_INIZIO").alias("valid_from"),
        pl.col("CREATION")
        .fill_null(pl.col("LAST_MOD"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
    )

    # Replace specific status values
    df_stato_udo = df_stato_udo.with_columns(
        pl.col("status").replace("AUTORIZZATA/ACCREDITATA", "AUTORIZZATA")
    )

    # Map supply information from UDO data
    df_udo = df_udo.select(
        pl.col("CLIENTID").str.strip_chars().alias("udo_id"),
        pl.when(pl.col("EROGAZIONE_DIRETTA").str.strip_chars().str.to_lowercase() == "y")
        .then(True)
        .otherwise(False)
        .alias("is_direct_supply"),
        pl.when(pl.col("EROGAZIONE_INDIRETTA").str.strip_chars().str.to_lowercase() == "y")
        .then(True)
        .otherwise(False)
        .alias("is_indirect_supply"),
    )

    # Join with UDO data to get supply information
    df_result = df_stato_udo.join(
        df_udo,
        left_on="udo_id",
        right_on="udo_id",
        how="left",
    )

    # Map bed information from bed history data
    df_beds = df_beds.select(
        pl.col("ID_STATO_UDO_FK").str.strip_chars().alias("id"),
        pl.col("PL").cast(pl.UInt16, strict=False).fill_null(0).alias("beds"),
        pl.col("PLEX").cast(pl.UInt16, strict=False).fill_null(0).alias("extra_beds"),
        pl.col("PLOB").cast(pl.UInt16, strict=False).fill_null(0).alias("mortuary_beds"),
    )

    # Join with bed history data
    df_result = df_result.join(
        df_beds,
        left_on="id",
        right_on="id",
        how="left",
    )

    # Fill null values for bed columns
    df_result = df_result.with_columns(
        pl.col("beds").fill_null(0),
        pl.col("extra_beds").fill_null(0),
        pl.col("mortuary_beds").fill_null(0),
    )

    # Verify UDO IDs exist in the udos table
    try:
        df_udos = pl.read_database(
            "SELECT id FROM udos",
            connection=ctx.pg_engine_core,
            infer_schema_length=None,
        )
        logging.info("⛏️ Extracted UDO IDs from target database for validation")

        # Convert to a list for filtering
        valid_udo_ids = df_udos.select("id").to_series().to_list()

        # Filter to include only records with valid UDO IDs
        df_result = df_result.filter(pl.col("udo_id").is_in(valid_udo_ids))
        logging.info(f"Filtered to {df_result.height} records with valid UDO IDs")
    except Exception as e:
        logging.warning(f"Could not validate UDO IDs: {e}")

    # Let PostgreSQL generate new UUIDs for the records
    logging.info("Removing 'id' column to let PostgreSQL generate new UUIDs")
    if "id" in df_result.columns:
        df_result = df_result.drop("id")

    # No need to check for duplicates since we're generating new IDs

    # Skip if no records to insert
    if df_result.height == 0:
        logging.info("No new records to insert into udo_status_history")
        return

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "udo_status_history")


### USER ###


def map_user_role(value: str) -> str:
    """
    Map a user role string to a standardized format.

    Converts the input string to lowercase and maps it to one of the standard role types
    based on keyword matching.

    Parameters
    ----------
    value: str
        The user role string to be mapped

    Returns
    -------
    str
        The standardized role value: "REGIONAL_OPERATOR", "ADMIN", or "OPERATOR"
    """
    value = value.lower()

    if "region" in value:
        return "REGIONAL_OPERATOR"
    elif "amministratore" in value:
        return "ADMIN"
    else:
        return "OPERATOR"


def migrate_users(ctx: ETLContext) -> None:
    """
    Migrate users from Oracle to PostgreSQL.

    Extracts user data from Oracle tables "AUAC_USR.UTENTE_MODEL" and "AUAC_USR.ANAGRAFICA_UTENTE_MODEL",
    transforms it, and loads it into the PostgreSQL "users" table.

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_utente_model = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.UTENTE_MODEL")
    df_anagrafica_utente_model = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.ANAGRAFICA_UTENTE_MODEL"
    )
    df_municipalities = extract_data(ctx.pg_engine_core, "SELECT * FROM municipalities")

    ### TRANSFORM ###
    df_anagrafica_utente_model = df_anagrafica_utente_model.select(
        pl.col("CLIENTID").alias("ID_ANAGR_FK"),
        pl.col("NOME").str.strip_chars(),
        pl.col("COGNOME").str.strip_chars(),
        pl.col("CFISC").str.strip_chars(),
        pl.col("EMAIL").str.strip_chars().fill_null("-"),
        pl.col("DATA_NASCITA"),
        pl.col("COD_LUOGO_NASCITA").str.strip_chars(),
        pl.col("VIA_PIAZZA").str.strip_chars(),
        pl.col("CIVICO").str.strip_chars(),
        pl.col("TELEFONO").str.strip_chars(),
        pl.col("CELLULARE").str.strip_chars(),
        pl.col("CARTA_IDENT_NUM").str.strip_chars(),
        pl.col("CARTA_IDENT_SCAD"),
        pl.col("PROFESSIONE").str.strip_chars(),
    )
    df_municipalities = df_municipalities.select(
        pl.col("istat_code").alias("COD_LUOGO_NASCITA"),
        pl.col("name").alias("birth_place"),
    )
    df_joined = df_anagrafica_utente_model.join(
        df_municipalities,
        left_on="COD_LUOGO_NASCITA",
        right_on="COD_LUOGO_NASCITA",
        how="left",
    )
    timestamp_exprs = handle_timestamps(direct_disabled_col="DATA_DISABILITATO")

    df_result = (
        df_joined.join(
            df_utente_model,
            left_on="ID_ANAGR_FK",
            right_on="ID_ANAGR_FK",
            how="left",
        )
        .select(
            pl.col("USERNAME_CAS").str.strip_chars().alias("username"),
            pl.col("RUOLO")
            .str.strip_chars()
            .map_elements(map_user_role, return_dtype=pl.String)
            .fill_null("OPERATORE")
            .alias("role"),
            pl.col("NOME").str.strip_chars().alias("first_name"),
            pl.col("COGNOME").str.strip_chars().alias("last_name"),
            pl.col("CFISC").str.strip_chars().alias("tax_code"),
            pl.col("EMAIL").str.strip_chars().alias("email"),
            pl.col("DATA_NASCITA").alias("birth_date"),
            pl.col("birth_place"),
            pl.col("VIA_PIAZZA").str.strip_chars().alias("street_name"),
            pl.col("CIVICO").str.strip_chars().alias("street_number"),
            pl.col("TELEFONO").str.strip_chars().alias("phone"),
            pl.col("CELLULARE").str.strip_chars().alias("mobile_phone"),
            pl.col("CARTA_IDENT_NUM").str.strip_chars().alias("identity_doc_number"),
            pl.col("CARTA_IDENT_SCAD").alias("identity_doc_expiry_date"),
            pl.col("PROFESSIONE").str.strip_chars().alias("job"),
            # pl.col("ID_UO_FK").alias("operational_unit_id"), FIXME: Fix reference to table
            # Get timestamp expressions with direct_disabled_col
            timestamp_exprs["created_at"],
            timestamp_exprs["updated_at"],
            timestamp_exprs["disabled_at"],
        )
        .filter(pl.col("username").is_not_null())
    )

    ### LOAD ###
    load_data(ctx.pg_engine_core, df_result, "users")


def migrate_permissions(ctx: ETLContext) -> None:
    """
    Migrate permissions from CSV to PostgreSQL.

    Loads permission data from the seed/permissions.csv file into the PostgreSQL
    "permissions" table.

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df = pl.read_csv("seed/permissions.csv")

    ### LOAD ###
    df.write_database(
        table_name="permissions", connection=ctx.pg_engine_core, if_table_exists="append"
    )
    logging.info("Loaded seed data into permissions table")


def migrate_user_companies(ctx: ETLContext) -> None:
    """
    Migrate user-company relationships from Oracle to PostgreSQL.

    Extracts user-company relationship data from Oracle tables "AUAC_USR.UTENTE_MODEL" and
    "AUAC_USR.OPERATORE_MODEL", transforms it, and loads it into the PostgreSQL
    "user_companies" table.

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_utente_model = extract_data(ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.UTENTE_MODEL")
    df_operatore_model = extract_data(
        ctx.oracle_engine_area, "SELECT * FROM AUAC_USR.OPERATORE_MODEL"
    )

    ### TRANSFORM ###
    # Get timestamp expressions with direct_disabled_col
    timestamp_exprs = handle_timestamps(direct_disabled_col="DATA_DISABILITATO")

    df_user = df_utente_model.select(
        pl.col("CLIENTID").alias("id"),
        pl.col("CLIENTID").alias("user_id"),
        pl.lit("*").alias("company_id"),
        timestamp_exprs["disabled_at"],
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
    )

    # Get timestamp expressions for operator model
    operator_timestamp_exprs = handle_timestamps()

    df_operator = df_operatore_model.select(
        pl.col("CLIENTID").alias("id"),
        pl.col("ID_UTENTE_FK").alias("user_id"),
        pl.col("ID_TITOLARE_FK").alias("company_id"),
        operator_timestamp_exprs["disabled_at"],
        operator_timestamp_exprs["created_at"],
        operator_timestamp_exprs["updated_at"],
    )

    # Combine the dataframes
    df_combined = pl.concat([df_user, df_operator])

    # Fill missing values and add flag_legale
    df_combined = df_combined.with_columns(
        [
            pl.col("company_id").fill_null("*"),
            pl.lit(False).alias("is_legal_representative"),
        ]
    )

    # Sort by user_id and fill missing disabled_at values
    df_combined = df_combined.sort("user_id")
    df_combined = df_combined.with_columns(
        [
            pl.col("disabled_at").fill_null(pl.col("disabled_at").forward_fill().backward_fill()),
            pl.col("created_at").fill_null(pl.col("updated_at")),
            pl.col("updated_at").fill_null(pl.col("created_at")),
        ]
    )

    # Handle duplicates
    df_duplicates = df_combined.filter(
        pl.col("user_id").is_duplicated() & pl.col("company_id").is_duplicated()
    )

    # Create id mapping for duplicates
    id_map = {}
    for _key, group_df in df_duplicates.group_by(["user_id", "company_id"]):
        kept_id = group_df["id"].first()
        deleted_ids = group_df["id"].tail(-1)
        for deleted_id in deleted_ids:
            id_map[deleted_id] = kept_id

    # Remove duplicates
    df_combined = df_combined.unique(subset=["user_id", "company_id"])

    ### LOAD ###
    df_combined.write_database(
        table_name="user_companies",
        connection=ctx.pg_engine_core,
        if_table_exists="append",
    )

    logging.info("Migrated user companies")
    return id_map


### ALL ###


def migrate_core(ctx: ETLContext) -> None:
    """
    Migrate data from source databases to the Core service database.

    This function orchestrates the complete ETL process for the Core service,
    first truncating all target tables and then migrating each entity type
    in the correct sequence.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    truncate_core_tables(ctx)
    migrate_regions(ctx)
    migrate_provinces(ctx)
    migrate_municipalities(ctx)
    migrate_toponyms(ctx)
    migrate_districts(ctx)
    migrate_ulss(ctx)
    migrate_company_types(ctx)
    migrate_companies(ctx)
    migrate_physical_structures(ctx)
    migrate_operational_offices(ctx)
    migrate_buildings(ctx)
    migrate_grouping_specialties(ctx)
    migrate_specialties(ctx)
    migrate_operational_units(ctx)
    migrate_production_factor_types(ctx)
    migrate_production_factors(ctx)
    migrate_udo_type_classifications(ctx)
    migrate_udo_types(ctx)
    migrate_udos(ctx)
    migrate_udo_production_factors(ctx)
    migrate_udo_type_production_factor_types(ctx)
    migrate_udo_specialties(ctx)
    migrate_udo_resolutions(ctx)
    migrate_udos_history(ctx)
    migrate_resolution_types(ctx)
    migrate_resolutions(ctx)
    migrate_users(ctx)
    migrate_user_companies(ctx)
