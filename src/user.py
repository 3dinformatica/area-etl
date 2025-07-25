import logging
from datetime import datetime, timezone

import polars as pl

from core import ETLContext, extract_data, load_data


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
    df_utente_model = extract_data(ctx, "SELECT * FROM AUAC_USR.UTENTE_MODEL")
    df_anagrafica_utente_model = extract_data(ctx, "SELECT * FROM AUAC_USR.ANAGRAFICA_UTENTE_MODEL")
    df_municipalities = extract_data(ctx, "SELECT * FROM municipalities", source="postgresql")

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
            pl.col("DATA_DISABILITATO").alias("disabled_at"),
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
        .filter(pl.col("username").is_not_null())
    )

    ### LOAD ###
    load_data(ctx, df_result, "users")


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
    df.write_database(table_name="permissions", connection=ctx.pg_engine, if_table_exists="append")
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
    df_utente_model = extract_data(ctx, "SELECT * FROM AUAC_USR.UTENTE_MODEL")
    df_operatore_model = extract_data(ctx, "SELECT * FROM AUAC_USR.OPERATORE_MODEL")

    ### TRANSFORM ###
    df_user = df_utente_model.select(
        pl.col("CLIENTID").alias("id"),
        pl.col("CLIENTID").alias("user_id"),
        pl.lit("*").alias("company_id"),
        pl.col("DATA_DISABILITATO").cast(pl.Datetime).alias("disabled_at"),
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

    df_operator = df_operatore_model.select(
        pl.col("CLIENTID").alias("id"),
        pl.col("ID_UTENTE_FK").alias("user_id"),
        pl.col("ID_TITOLARE_FK").alias("company_id"),
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
    for group in df_duplicates.group_by(["user_id", "company_id"]):
        kept_id = group["id"].first()
        deleted_ids = group["id"].tail(-1)
        for deleted_id in deleted_ids:
            id_map[deleted_id] = kept_id

    # Remove duplicates
    df_combined = df_combined.unique(subset=["user_id", "company_id"])

    ### LOAD ###
    df_combined.write_database(
        table_name="user_companies",
        connection=ctx.pg_engine,
        if_table_exists="append",
    )

    logging.info("Migrated user companies")
    return id_map
