import logging
from datetime import datetime, timezone

import polars as pl

from core import ETLContext


def migrate_production_factor_types(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_tipo_fattore_prod_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.TIPO_FATTORE_PROD_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
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
    logging.info("Migrated production_factor_types")


def migrate_production_factors(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_fatt_prod_udo_model = pl.read_database(
        "SELECT * FROM AUAC_USR.FATT_PROD_UDO_MODEL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
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
    logging.info("Migrated production_factors")


def migrate_udo_types(ctx: ETLContext) -> None:
    ### EXTRACT ###
    df_tipo_udo_22_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.TIPO_UDO_22_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_bind_tipo_22_ambito = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_TIPO_22_AMBITO",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_ambito_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.AMBITO_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_bind_tipo_22_natura = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_TIPO_22_NATURA",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_natura_titolare_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.NATURA_TITOLARE_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_bind_tipo_22_flusso = pl.read_database(
        "SELECT * FROM AUAC_USR.BIND_TIPO_22_FLUSSO",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )
    df_flusso_templ = pl.read_database(
        "SELECT * FROM AUAC_USR.FLUSSO_TEMPL",
        connection=ctx.oracle_engine.connect(),
        infer_schema_length=None,
    )

    ### TRANSFORM ###
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
    df_bind_tipo_22_ambito = df_bind_tipo_22_ambito.select(
        pl.col("ID_AMBITO_FK"),
        pl.col("ID_TIPO_22_FK"),
    )
    df_ambito_templ = df_ambito_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("CLIENTID_AMBITO_TEMPL"),
        pl.col("NOME").alias("AMBITO_NOME"),
        pl.col("DESCR").alias("AMBITO_DESCR"),
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
    df_result = df_result.join(
        df_bind_tipo_22_natura,
        left_on="CLIENTID_TIPO_UDO_22_TEMPL",
        right_on="ID_TIPO_UDO_22_FK",
        how="left",
    )
    df_result = df_result.join(
        df_natura_titolare_templ,
        left_on="ID_NATURA_FK",
        right_on="CLIENTID",
        how="left",
    )

    ### LOAD ###
    df_result.write_database(
        table_name="udo_types",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )
    logging.info("Migrated udo_types")


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
