import logging
from datetime import datetime, timezone

import polars as pl

from core import ETLContext, extract_data, load_data


def migrate_production_factor_types(ctx: ETLContext) -> None:
    """
    Migrate production factor types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipo_fattore_prod_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_FATTORE_PROD_TEMPL")

    ### TRANSFORM ###
    # First, clean all string columns to remove NUL characters
    string_columns = ["CLIENTID", "NOME", "DESCR", "TIPOLOGIA_FATT_PROD"]
    for col in string_columns:
        if col in df_tipo_fattore_prod_templ.columns:
            df_tipo_fattore_prod_templ = df_tipo_fattore_prod_templ.with_columns(
                pl.col(col).str.replace_all("\x00", "").alias(col)
            )

    df_result = df_tipo_fattore_prod_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().alias("name"),
        pl.col("DESCR").str.strip_chars().alias("code"),
        pl.col("TIPOLOGIA_FATT_PROD").str.strip_chars().alias("category"),
        pl.col("CREATION")
        .fill_null(datetime.now(timezone.utc).replace(tzinfo=None))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
        pl.when(pl.col("DISABLED") == "S")
        .then(
            pl.col("LAST_MOD")
            .fill_null(pl.col("CREATION"))
            .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "production_factor_types")


def migrate_production_factors(ctx: ETLContext) -> None:
    """
    Migrate production factors from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_fatt_prod_udo_model = extract_data(ctx, "SELECT * FROM AUAC_USR.FATT_PROD_UDO_MODEL")

    ### TRANSFORM ###
    # First, clean all string columns to remove NUL characters
    string_columns = ["CLIENTID", "ID_TIPO_FK", "VALORE", "VALORE2", "VALORE3", "DESCR"]
    for col in string_columns:
        if col in df_fatt_prod_udo_model.columns:
            df_fatt_prod_udo_model = df_fatt_prod_udo_model.with_columns(
                pl.col(col).str.replace_all("\x00", "").alias(col)
            )

    df_result = df_fatt_prod_udo_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("ID_TIPO_FK").str.strip_chars().alias("production_factor_type_id"),
        pl.col("VALORE")
        .str.strip_chars()
        .replace(["", "?"], "0")
        .fill_null("0")
        .cast(pl.UInt16)
        .alias("beds"),
        pl.col("VALORE3")
        .str.strip_chars()
        .replace(["", "?"], "0")
        .fill_null("0")
        .cast(pl.UInt16)
        .alias("hospital_beds"),
        pl.col("VALORE2").str.strip_chars().replace(["NUL"], None).alias("room_name"),
        pl.col("DESCR").str.strip_chars().replace(["NUL"], None).alias("room_code"),
        pl.col("CREATION")
        .fill_null(datetime.now(timezone.utc).replace(tzinfo=None))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
        pl.when(pl.col("DISABLED") == "S")
        .then(
            pl.col("LAST_MOD")
            .fill_null(pl.col("CREATION"))
            .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "production_factors")


def migrate_udo_types(ctx: ETLContext) -> None:
    """
    Migrate UDO types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipo_udo_22_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_UDO_22_TEMPL")
    df_bind_tipo_22_ambito = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_TIPO_22_AMBITO")

    df_ambito_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.AMBITO_TEMPL")
    df_bind_tipo_22_natura = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_TIPO_22_NATURA")

    df_natura_titolare_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.NATURA_TITOLARE_TEMPL")
    df_bind_tipo_22_flusso = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_TIPO_22_FLUSSO")

    df_flusso_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.FLUSSO_TEMPL")

    ### TRANSFORM ###
    # Clean and transform the main table
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

    # Rename columns to match the target schema
    df_result = df_result.select(
        pl.col("CLIENTID_TIPO_UDO_22_TEMPL").alias("id"),
        pl.col("DESCR").alias("name"),
        pl.col("CODICE_UDO").alias("code"),
        pl.col("NOME_CODICE_UDO").alias("code_name"),
        pl.col("SETTING").alias("setting"),
        pl.col("TARGET").alias("target"),
        pl.col("ID_CLASSIFICAZIONE_UDO_FK").alias("classification_id"),
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
        pl.lit("{}").alias("extra"),
        pl.when(pl.col("DISABLED") == "S")
        .then(
            pl.col("LAST_MOD")
            .fill_null(pl.col("CREATION"))
            .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
        pl.col("CREATION")
        .fill_null(datetime.now(timezone.utc).replace(tzinfo=None))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
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

    load_data(ctx, df_result, "udo_types")


def migrate_udo_production_factors(ctx: ETLContext) -> None:
    """
    Migrate UDO production factors from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_udo_fatt_prod = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_UDO_FATT_PROD")

    ### TRANSFORM ###
    df_result = df_bind_udo_fatt_prod.select(
        pl.col("ID_FATTORE_FK").str.strip_chars().alias("production_factor_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "udo_production_factors")


def migrate_udo_type_production_factor_types(ctx: ETLContext) -> None:
    """
    Migrate UDO type production factor types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_tipo_22_tipo_fatt = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_TIPO_22_TIPO_FATT")

    ### TRANSFORM ###
    df_result = df_bind_tipo_22_tipo_fatt.select(
        pl.col("ID_TIPO_UDO_22_FK").str.strip_chars().alias("udo_type_id"),
        pl.col("ID_TIPO_FATT_FK").str.strip_chars().alias("production_factor_type_id"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "udo_type_production_factor_types")


def migrate_udo_specialties_from_branches(ctx: ETLContext) -> None:
    """
    Migrates branches data to specialties.
    This replaces the old migrate_udo_branches function as disciplines and branches
    have been merged into specialties.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_udo_branca = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_UDO_BRANCA")

    df_bind_udo_branca_altro = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_UDO_BRANCA_ALTRO")

    ### TRANSFORM ###
    # Process main branch data
    df_bind_udo_branca = df_bind_udo_branca.select(
        pl.col("ID_BRANCA_FK").str.strip_chars().alias("specialty_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
        pl.when(pl.col("AUTORIZZATA").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("is_authorized"),
        pl.when(pl.col("ACCREDITATA").str.strip_chars().str.to_lowercase().is_in(["s", "y"]))
        .then(True)
        .otherwise(False)
        .alias("is_accredited"),
    )

    # Process alternative branch data
    df_bind_udo_branca_altro = df_bind_udo_branca_altro.select(
        pl.col("ID_ARTIC_BRANCA_ALTRO_FK").str.strip_chars().alias("specialty_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
    )

    # Add default values for missing columns in the alternative branch data
    df_bind_udo_branca_altro = df_bind_udo_branca_altro.with_columns(
        pl.lit(False).alias("is_authorized"),
        pl.lit(False).alias("is_accredited"),
    )

    # Combine both dataframes
    df_result = pl.concat([df_bind_udo_branca, df_bind_udo_branca_altro])

    ### LOAD ###
    load_data(ctx, df_result, "udo_specialties")


def migrate_udo_specialties_from_disciplines(ctx: ETLContext) -> None:
    """
    Migrates disciplines data to specialties.
    Disciplines and branches have been merged into specialties.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_udo_disciplina = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_UDO_DISCIPLINA")

    ### TRANSFORM ###
    # Filter out rows with null discipline IDs
    df_bind_udo_disciplina = df_bind_udo_disciplina.filter(pl.col("ID_DISCIPLINA_FK").is_not_null())

    # Process UO_MODEL data for clinical operational units
    # We'll use a direct query approach to avoid the need for oracle_poa_engine
    df_uo_model_map = None
    try:
        df_uo_model_map = extract_data(ctx, "SELECT ID_UO, CLIENTID FROM AUAC_USR.UO_MODEL")
    except Exception as e:
        logging.warning(f"Could not extract UO_MODEL data: {e}")
        df_uo_model_map = pl.DataFrame({"ID_UO": [], "CLIENTID": []})

    # Transform the main data
    df_result = df_bind_udo_disciplina.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("ID_DISCIPLINA_FK").str.strip_chars().alias("specialty_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
        pl.col("POSTI_LETTO").cast(pl.UInt16, strict=False).fill_null(0).alias("beds"),
        pl.col("POSTI_LETTO_EXTRA").cast(pl.UInt16, strict=False).fill_null(0).alias("extra_beds"),
        pl.col("POSTI_LETTO_OBI").cast(pl.UInt16, strict=False).fill_null(0).alias("mortuary_beds"),
        pl.col("POSTI_LETTO_ACC")
        .cast(pl.UInt16, strict=False)
        .fill_null(0)
        .alias("accredited_beds"),
        pl.col("HSP12").str.strip_chars().alias("hsp12"),
        pl.col("ID_UO").alias("ID_UO"),
        pl.col("PROVENIENZA_UO").alias("PROVENIENZA_UO"),
    )

    # Join with UO_MODEL data if available
    if df_uo_model_map is not None and df_uo_model_map.height > 0:
        df_result = df_result.join(
            df_uo_model_map,
            left_on="ID_UO",
            right_on="ID_UO",
            how="left",
        ).with_columns(pl.col("CLIENTID").alias("clinical_operational_unit_id"))

    # Since we don't have access to the V_NODI table through oracle_poa_engine,
    # we'll skip that part and just set clinical_organigram_node_id to null

    # Drop unnecessary columns and rename the rest
    df_result = df_result.drop(["ID_UO", "PROVENIENZA_UO"])

    # Ensure CLIENTID from UO_MODEL is not included in the final dataframe
    if "CLIENTID" in df_result.columns:
        df_result = df_result.drop("CLIENTID")

    # Handle any null values in the clinical_operational_unit_id column
    if "clinical_operational_unit_id" not in df_result.columns:
        df_result = df_result.with_columns(pl.lit(None).alias("clinical_operational_unit_id"))

    # Add clinical_organigram_node_id column with null values
    df_result = df_result.with_columns(pl.lit(None).alias("clinical_organigram_node_id"))

    ### LOAD ###
    load_data(ctx, df_result, "udo_specialties")


def migrate_udo_resolutions(ctx: ETLContext) -> None:
    """
    Migrates resolution data for UDOs.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_bind_atto_udo = extract_data(ctx, "SELECT * FROM AUAC_USR.BIND_ATTO_UDO")

    ### TRANSFORM ###
    df_result = df_bind_atto_udo.select(
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
        pl.col("ID_ATTO_FK").str.strip_chars().alias("resolution_id"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "udo_resolutions")


def migrate_udo_status_history(ctx: ETLContext) -> None:
    """
    Migrates UDO status history data.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    # Extract main status data
    df_stato_udo = extract_data(ctx, "SELECT * FROM AUAC_USR.STATO_UDO")

    # Extract UDO data for supply information
    df_udo = extract_data(
        ctx,
        "SELECT CLIENTID, EROGAZIONE_DIRETTA, EROGAZIONE_INDIRETTA FROM AUAC_USR.UDO_MODEL",
    )

    # Extract bed history data
    df_beds = extract_data(
        ctx, "SELECT ID_STATO_UDO_FK, PL, PLEX, PLOB FROM AUAC_USR.STORICO_POSTI_LETTO"
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

    # Add extra column as empty JSON
    df_result = df_result.with_columns(pl.lit("{}").alias("extra"))

    # Verify UDO IDs exist in the udos table
    try:
        df_udos = pl.read_database(
            "SELECT id FROM udos",
            connection=ctx.pg_engine,
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
    load_data(ctx, df_result, "udo_status_history")


def migrate_udos(ctx: ETLContext) -> None:
    """
    Migrates UDO data from UDO_MODEL to udos table.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    # Extract main UDO data
    df_udo_model = extract_data(ctx, "SELECT * FROM AUAC_USR.UDO_MODEL")

    # Extract operational office data
    df_sede_oper = extract_data(ctx, "SELECT * FROM AUAC_USR.SEDE_OPER_MODEL")

    # Extract structure data
    df_struttura = extract_data(ctx, "SELECT * FROM AUAC_USR.STRUTTURA_MODEL")

    # Extract company data
    df_titolare = extract_data(ctx, "SELECT * FROM AUAC_USR.TITOLARE_MODEL")

    # Extract UO_MODEL data for operational units
    try:
        df_uo_model = extract_data(ctx, "SELECT ID_UO, CLIENTID FROM AUAC_USR.UO_MODEL")
    except Exception as e:
        logging.warning(f"Could not extract UO_MODEL data: {e}")
        df_uo_model = pl.DataFrame({"ID_UO": [], "CLIENTID": []})

    ### TRANSFORM ###
    # Clean string columns in the main UDO data
    string_columns = [
        "CLIENTID",
        "DESCR",
        "ID_UNIVOCO",
        "ID_TIPO_UDO_22_FK",
        "ID_SEDE_FK",
        "ID_EDIFICIO_STR_FK",
        "PIANO",
        "STATO",
        "BLOCCO",
        "PROGRESSIVO",
        "CODICE_FLUSSO_MINISTERIALE",
        "COD_FAR_FAD",
        "STAREP",
        "CDC",
        "PAROLE_CHIAVE",
        "ANNOTATIONS",
    ]

    for col in string_columns:
        if col in df_udo_model.columns:
            df_udo_model = df_udo_model.with_columns(
                pl.col(col).str.replace_all("\x00", "").alias(col)
            )

    # Transform the main UDO data
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
        pl.col("PROVENIENZA_UO").alias("PROVENIENZA_UO"),
        pl.col("ID_UO").alias("ID_UO"),
        pl.col("CREATION")
        .fill_null(datetime.now(timezone.utc).replace(tzinfo=None))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
        pl.when(pl.col("DISABLED") == "S")
        .then(
            pl.col("LAST_MOD")
            .fill_null(pl.col("CREATION"))
            .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
    )

    # Join with operational office, structure, and company data
    df_sede_struttura = df_sede_oper.select(
        pl.col("CLIENTID").alias("operational_office_id"),
        pl.col("ID_STRUTTURA_FK").alias("id_struttura_fk"),
    )

    df_struttura_titolare = df_struttura.select(
        pl.col("CLIENTID").alias("id_struttura_fk"),
        pl.col("ID_TITOLARE_FK").alias("id_titolare_fk"),
    )

    df_titolare_id = df_titolare.select(pl.col("CLIENTID").alias("id_titolare_fk"))

    # Join to get company_id
    df_joined = df_sede_struttura.join(df_struttura_titolare, on="id_struttura_fk", how="left")

    df_joined = df_joined.join(df_titolare_id, on="id_titolare_fk", how="left")

    df_company_map = df_joined.select(
        pl.col("operational_office_id"), pl.col("id_titolare_fk").alias("company_id")
    )

    # Join with company map
    df_result = df_result.join(df_company_map, on="operational_office_id", how="left")

    # Process operational unit data
    # For UO_MODEL source
    df_uo_filtered = df_result.filter(pl.col("PROVENIENZA_UO") == "UO_MODEL")

    if df_uo_model.height > 0 and df_uo_filtered.height > 0:
        df_uo_filtered = df_uo_filtered.join(
            df_uo_model, left_on="ID_UO", right_on="ID_UO", how="left"
        ).with_columns(pl.col("CLIENTID").alias("operational_unit_id"))

        # Update the main result with operational unit IDs
        df_result = df_result.join(
            df_uo_filtered.select("id", "operational_unit_id"), on="id", how="left"
        )

    # Since we don't have access to the V_NODI table through oracle_poa_engine,
    # we'll skip that part and just set organigram_node_id to null

    # Add organigram_node_id column with null values
    df_result = df_result.with_columns(pl.lit(None).alias("organigram_node_id"))

    # Drop unnecessary columns
    df_result = df_result.drop(["PROVENIENZA_UO", "ID_UO"])

    # Add extra column as empty JSON
    df_result = df_result.with_columns(pl.lit("{}").alias("extra"))

    # Filter out invalid UDO types
    invalid_udo_types = [
        "3E2436FA-B18B-3B51-F0EC-A2DB6B8E8AD0",
        "9802C27D-1001-BC4C-7C84-59E8CD1832CE",
        "292B63D5-E3C3-8B12-B565-7919E3D86ABE",
        "6DEBDC16-90D5-D3F3-82FA-F548381CBB51",
        "53F0D51B-FC0C-305D-2090-1D2A48573497",
    ]

    df_result = df_result.filter(~pl.col("udo_type_id").is_in(invalid_udo_types))

    ### LOAD ###
    load_data(ctx, df_result, "udos")


def migrate_operational_units(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_uo_model = extract_data(ctx, "SELECT * FROM AUAC_USR.UO_MODEL")

    ### TRANSFORM ###
    df_result = df_uo_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("COD_UNIVOCO_UO").str.strip_chars().alias("code"),
        pl.col("DENOMINAZIONE").str.strip_chars().alias("name"),
        pl.col("DESCR").str.strip_chars().alias("description"),
        pl.col("ID_TITOLARE_FK").str.strip_chars().alias("company_id"),
        pl.col("CREATION")
        .fill_null(datetime.now(timezone.utc).replace(tzinfo=None))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
        pl.when(pl.col("DISABLED") == "S")
        .then(
            pl.col("LAST_MOD")
            .fill_null(pl.col("CREATION"))
            .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
    )

    ### LOAD ###
    load_data(ctx, df_result, "operational_units")
