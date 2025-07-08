import logging
from datetime import datetime, timezone

import polars as pl
import pandas as pd
import json

from core import ETLContext


def migrate_production_factor_types(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_tipo_fattore_prod_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.TIPO_FATTORE_PROD_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_tipo_fattore_prod_templ.height} from table "AUAC_USR.TIPO_FATTORE_PROD_TEMPL"'
    )

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
    df_result.write_database(
        table_name="production_factor_types",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info(f'⬆️ Loaded {df_result.height} rows to table "production_factor_types"')


def migrate_production_factors(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_fatt_prod_udo_model = pl.read_database(
        "SELECT * FROM AUAC_USR.FATT_PROD_UDO_MODEL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_fatt_prod_udo_model.height} from table "AUAC_USR.FATT_PROD_UDO_MODEL"'
    )

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
    df_result.write_database(
        table_name="production_factors",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info(f'⬆️ Loaded {df_result.height} rows to table "production_factors"')


def migrate_udo_types(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_tipo_udo_22_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.TIPO_UDO_22_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_tipo_udo_22_templ.height} from table "AUAC_USR.TIPO_UDO_22_TEMPL"'
    )
    df_bind_tipo_22_ambito = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_TIPO_22_AMBITO",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_bind_tipo_22_ambito.height} from table "AUAC_USR.BIND_TIPO_22_AMBITO"'
    )
    df_ambito_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.AMBITO_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_ambito_templ.height} from table "AUAC_USR.AMBITO_TEMPL"'
    )
    df_bind_tipo_22_natura = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_TIPO_22_NATURA",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_bind_tipo_22_natura.height} from table "AUAC_USR.BIND_TIPO_22_NATURA"'
    )
    df_natura_titolare_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.NATURA_TITOLARE_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_natura_titolare_templ.height} from table "AUAC_USR.NATURA_TITOLARE_TEMPL"'
    )
    df_bind_tipo_22_flusso = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_TIPO_22_FLUSSO",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_bind_tipo_22_flusso.height} from table "AUAC_USR.BIND_TIPO_22_FLUSSO"'
    )
    df_flusso_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.FLUSSO_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    logging.info(
        f'⛏️ Extracted {df_flusso_templ.height} from table "AUAC_USR.FLUSSO_TEMPL"'
    )

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
        pl.when(
            pl.col("OSPEDALIERO").str.strip_chars().str.to_lowercase().is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("OSPEDALIERO"),
        pl.when(
            pl.col("SALUTE_MENTALE")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("SALUTE_MENTALE"),
        pl.when(
            pl.col("POSTI_LETTO").str.strip_chars().str.to_lowercase().is_in(["s", "y"])
        )
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
            pl.col("AGGIUNGI_DISCIPLINE")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_DISCIPLINE"),
        pl.when(
            pl.col("AGGIUNGI_BRANCHE")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_BRANCHE"),
        pl.when(
            pl.col("AGGIUNGI_PRESTAZIONI")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
        )
        .then(True)
        .otherwise(False)
        .alias("AGGIUNGI_PRESTAZIONI"),
        pl.when(
            pl.col("AGGIUNGI_AMBITO")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
        )
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
            pl.col("AGGIUNGI_BRANCHE_AZ_SAN")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["s", "y"])
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
                "AZIENDA_SANITARIA" if item == "AzSan" else
                "PUBBLICO" if item == "Pub" else
                "PRIVATO" if item == "Pri" else item
                for item in (list(x) if x is not None else [])
            ],
            return_dtype=pl.List
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
    df_flows_grouped = df_flows.group_by("ID_TIPO_UDO_22_FK").agg(
        pl.col("NOME").alias("FLUSSI")
    )

    # Clean and standardize flow names and ensure we're using Python lists, not NumPy arrays
    df_flows_grouped = df_flows_grouped.with_columns(
        pl.col("FLUSSI").map_elements(
            lambda x: [
                item.replace(" ", "_").replace(".", "_") 
                for item in list(x) if item is not None
            ] if x is not None else [],
            return_dtype=pl.List
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
        pl.col("AGGIUNGI_DISCIPLINE_PUB_PRIV").alias("has_disciplines_only_public_or_private_company"),
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
            .dt.replace_time_zone("Europe/Rome")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at"),
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
    )

    ### LOAD ###
    # Convert to pandas first to have more control over the data types
    logging.info("Converting to pandas dataframe")
    pandas_df = df_result.to_pandas()

    # Explicitly convert array columns to Python lists via JSON serialization/deserialization
    # This ensures we completely break any reference to NumPy arrays
    logging.info("Converting array columns to Python lists")

    def safe_convert_to_list(x):
        if x is None:
            return []
        try:
            # Convert to JSON and back to ensure we have pure Python types
            json_str = json.dumps([str(item) for item in x if item is not None])
            return json.loads(json_str)
        except Exception as e:
            logging.warning(f"Error converting to list: {e}, value: {repr(x)}, type: {type(x)}")
            # Fallback: try direct conversion
            try:
                if hasattr(x, 'tolist'):
                    return [str(item) for item in x.tolist() if item is not None]
                elif hasattr(x, '__iter__'):
                    return [str(item) for item in list(x) if item is not None]
                else:
                    return []
            except Exception as e2:
                logging.warning(f"Fallback conversion also failed: {e2}")
                return []

    # Log sample values before conversion
    for col in ['company_natures', 'ministerial_flows']:
        if not pandas_df.empty:
            sample = pandas_df[col].iloc[0]
            logging.info(f"Sample {col} before conversion: {repr(sample)}, type: {type(sample)}")

    # Apply the conversion
    pandas_df['company_natures'] = pandas_df['company_natures'].apply(safe_convert_to_list)
    pandas_df['ministerial_flows'] = pandas_df['ministerial_flows'].apply(safe_convert_to_list)

    # Log sample values after conversion
    for col in ['company_natures', 'ministerial_flows']:
        if not pandas_df.empty:
            sample = pandas_df[col].iloc[0]
            logging.info(f"Sample {col} after conversion: {repr(sample)}, type: {type(sample)}")

    # Log the data types to verify conversion
    logging.info(f"company_natures data type: {type(pandas_df['company_natures'].iloc[0]).__name__}")
    logging.info(f"ministerial_flows data type: {type(pandas_df['ministerial_flows'].iloc[0]).__name__}")

    # Final check: ensure array columns are properly formatted for PostgreSQL
    # PostgreSQL expects array columns to be Python lists, not NumPy arrays or other types
    logging.info("Final check of array columns")

    # Convert empty arrays to None to avoid PostgreSQL issues
    for col in ['company_natures', 'ministerial_flows']:
        pandas_df[col] = pandas_df[col].apply(lambda x: None if len(x) == 0 else x)

    # Create a copy of the dataframe with array columns converted to strings for fallback
    pandas_df_str = pandas_df.copy()
    pandas_df_str['company_natures'] = pandas_df_str['company_natures'].apply(lambda x: json.dumps(x) if x is not None else None)
    pandas_df_str['ministerial_flows'] = pandas_df_str['ministerial_flows'].apply(lambda x: json.dumps(x) if x is not None else None)

    try:
        # Try writing to database using pandas to_sql
        logging.info("Writing to database")
        pandas_df.to_sql(
            name="udo_types",
            con=ctx.pg_engine,
            schema=None,
            if_exists="append",
            index=False,
            method='multi'  # Use multi-row insert for better performance
        )
        logging.info(f'⬆️ Loaded {len(pandas_df)} rows to table "udo_types"')
    except Exception as e:
        logging.error(f"Error writing to udo_types table: {e}", exc_info=True)

        # Try fallback with string columns
        logging.warning("Trying fallback with string columns")
        try:
            pandas_df_str.to_sql(
                name="udo_types",
                con=ctx.pg_engine,
                schema=None,
                if_exists="append",
                index=False,
                method='multi'
            )
            logging.info(f'⬆️ Loaded {len(pandas_df_str)} rows to table "udo_types" using fallback method')
            return  # Exit function if fallback succeeds
        except Exception as e2:
            logging.error(f"Fallback also failed: {e2}", exc_info=True)

        # Additional error diagnostics
        try:
            # Sample the data to see what's causing the issue
            for col in ['company_natures', 'ministerial_flows']:
                if not pandas_df.empty:
                    sample_val = pandas_df[col].iloc[0]
                    logging.error(f"Sample value for {col}: {repr(sample_val)}, type: {type(sample_val)}")
                    if hasattr(sample_val, '__iter__') and not isinstance(sample_val, str):
                        for i, item in enumerate(sample_val):
                            logging.error(f"  Item {i}: {repr(item)}, type: {type(item)}")
        except Exception as diagnostic_error:
            logging.error(f"Error during diagnostics: {diagnostic_error}")

        raise


def migrate_udo_production_factors(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_bind_udo_fatt_prod = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_UDO_FATT_PROD",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )

    ### TRANSFORM ###
    df_result = df_bind_udo_fatt_prod.select(
        pl.col("ID_FATTORE_FK").str.strip_chars().alias("production_factor_id"),
        pl.col("ID_UDO_FK").str.strip_chars().alias("udo_id"),
    )

    ### LOAD ###
    df_result.write_database(
        table_name="udo_production_factors",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info("Migrated udo_production_factors")


def migrate_udo_type_production_factor_types(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_bind_tipo_22_tipo_fatt = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_TIPO_22_TIPO_FATT",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )

    ### TRANSFORM ###
    df_result = df_bind_tipo_22_tipo_fatt.select(
        pl.col("ID_TIPO_UDO_22_FK").str.strip_chars().alias("udo_type_id"),
        pl.col("ID_TIPO_FATT_FK").str.strip_chars().alias("production_factor_type_id"),
    )

    ### LOAD ###
    df_result.write_database(
        table_name="udo_type_production_factor_types",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info("Migrated udo_type_production_factor_types")


def migrate_udo_branches(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_bind_udo_branca = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_UDO_BRANCA",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_bind_udo_branca_altro = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_UDO_BRANCA_ALTRO",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )

    ### TRANSFORM ###
    df_bind_udo_branca = df_bind_udo_branca.select(
        pl.col("ID_BRANCA_FK"),
        pl.col("ID_UDO_FK"),
        pl.col("AUTORIZZATA"),
        pl.col("ACCREDITATA"),
    )
    df_bind_udo_branca_altro = df_bind_udo_branca_altro.select(
        pl.col("ID_UDO_FK"),
        pl.col("ID_ARTIC_BRANCA_ALTRO_FK").alias("ID_BRANCA_FK"),
    )
    df_result = df_bind_udo_branca.join(
        df_bind_udo_branca_altro,
        on=["ID_BRANCA_FK", "ID_UDO_FK"],
        how="left",
    )

    ### LOAD ###
    df_result.write_database(
        table_name="udo_branches",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info("Migrated udo_branches")
