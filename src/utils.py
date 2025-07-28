import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import polars as pl
from cx_Oracle import init_oracle_client
from minio import Minio
from sqlalchemy import Engine, create_engine, text

from settings import settings

# List of tables used in multiple functions
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

# List of tables in the AUAC database
AUAC_TABLES = [
    "attachment_types",
    "procedure_attachments",
    "procedure_entities",
    "procedure_entity_requirements",
    "procedure_notes",
    "procedure_type_requirement_list_classification_mental",
    "procedure_type_requirement_list_comp_type_comp_class",
    "procedure_type_requirement_list_for_physical_structures",
    "procedure_type_requirement_list_udo_type",
    "procedures",
    "requirement_taxonomies",
    "requirements",
    "requirement_lists",
    "requirementlist_requirements",
]


@dataclass
class ETLContext:
    """
    Context object for ETL operations containing database connections.

    Attributes
    ----------
    oracle_engine : Engine
        SQLAlchemy engine for Oracle database connection
    pg_engine_core : Engine
        SQLAlchemy engine for PostgreSQL A.Re.A. core database connection
    pg_engine_auac : Engine
        SQLAlchemy engine for PostgreSQL A.Re.A. Au.Ac. database connection
    """

    oracle_engine: Engine
    pg_engine_core: Engine
    pg_engine_auac: Engine


def setup_logging() -> None:
    """
    Set up the logging configuration for the application.

    Creates a logs directory if it doesn't exist and configures logging
    to output to both console and a timestamped log file.
    """
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                f"logs/area_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                mode="a",
            ),
        ],
    )


def setup_connections() -> ETLContext:
    """
    Initialize database connections for ETL operations.

    Sets up an Oracle client and creates database engine connections
    for Oracle and both PostgreSQL databases (core and auac).

    Returns
    -------
    ETLContext
        Context object containing database connections
    """
    init_oracle_client(lib_dir=settings.ORACLE_CLIENT_LIB_DIR)
    oracle_engine = create_engine(settings.ORACLE_URI)
    pg_engine_core = create_engine(settings.PG_URI_CORE)
    pg_engine_auac = create_engine(settings.PG_URI_AUAC)
    return ETLContext(
        oracle_engine=oracle_engine, pg_engine_core=pg_engine_core, pg_engine_auac=pg_engine_auac
    )


def extract_data(ctx: ETLContext, query: str, source: str = "oracle") -> pl.DataFrame:
    """
    Extract data from a database and log the extraction.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    query : str
        The SQL query to execute
    source : str, optional
        The source database ('oracle', 'pg_core', or 'pg_auac'), by default "oracle"

    Returns
    -------
    pl.DataFrame
        A polars DataFrame containing the extracted data
    """
    if source == "oracle":
        engine = ctx.oracle_engine
    elif source == "pg_core":
        engine = ctx.pg_engine_core
    elif source == "pg_auac":
        engine = ctx.pg_engine_auac
    else:
        raise ValueError(f"Invalid source: {source}. Must be 'oracle', 'pg_core', or 'pg_auac'")

    df = pl.read_database(query, connection=engine.connect(), infer_schema_length=None)

    # Extract the table name from the input query for logging
    table_name = "unknown"
    if "FROM" in query.upper():
        parts = query.upper().split("FROM")
        if len(parts) > 1:
            table_parts = parts[1].strip().split()
            if table_parts:
                table_name = table_parts[0]

    logging.info(f"Extracted {df.height} rows from {source.upper()} table {table_name}")
    return df


def extract_data_from_csv(
    file_path: str | os.PathLike, schema_overrides: dict | None = None
) -> pl.DataFrame:
    """
    Extract data from a CSV file and log the extraction.

    Parameters
    ----------
    file_path : str
        The path to the CSV file
    schema_overrides : dict, optional
        Optional schema overrides for the CSV file, by default None

    Returns
    -------
    pl.DataFrame
        A polars DataFrame containing the extracted data
    """
    df = pl.read_csv(file_path, schema_overrides=schema_overrides)
    absolute_path = Path(file_path).absolute()
    logging.info(f"Extracted {df.height} rows from CSV file {absolute_path}")
    return df


def load_data(ctx: ETLContext, df: pl.DataFrame, table_name: str) -> None:
    """
    Load data to a PostgreSQL database and log the operation.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    df : pl.DataFrame
        The polars DataFrame to load
    table_name : str
        The name of the destination table
    """
    # Determine which database to use based on the table name
    if table_name in AUAC_TABLES:
        engine = ctx.pg_engine_auac
        db_name = "AUAC"
    else:
        engine = ctx.pg_engine_core
        db_name = "CORE"

    df.write_database(table_name=table_name, connection=engine, if_table_exists="append")
    logging.info(f"Loaded {df.height} rows into PostgreSQL {db_name} database table {table_name}")


def truncate_pg_table(engine: Engine, table: str) -> None:
    """
    Truncate a specific PostgreSQL table.

    This function executes a TRUNCATE TABLE command with CASCADE option on the specified table,
    which removes all rows from the table and resets any identity columns.

    Parameters
    ----------
    engine : Engine
        The SQLAlchemy engine connection to the PostgreSQL database
    table : str
        The name of the table to truncate
    """
    with engine.connect() as conn:
        logging.info(f'Truncating PostgreSQL {engine} database table "{table}"...')
        conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
        conn.commit()


def truncate_core_tables(ctx: ETLContext) -> None:
    """
    Truncate all core tables in the PostgreSQL database.

    This function iterates through all core tables defined in CORE_TABLES
    and truncates each one using the truncate_pg_table function.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    logging.info(f"Truncating all target tables in PostgreSQL {ctx.pg_engine_core}...")

    for table in CORE_TABLES:
        truncate_pg_table(ctx.pg_engine_core, table)


def truncate_auac_tables(ctx: ETLContext) -> None:
    """
    Truncate all AUAC (Authorization and Accreditation) tables in the PostgreSQL database.

    This function iterates through all AUAC tables defined in AUAC_TABLES
    and truncates each one using the truncate_pg_table function.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    logging.info(f'Truncating all target tables in PostgreSQL {ctx.pg_engine_auac}..."')

    for table in AUAC_TABLES:
        truncate_pg_table(ctx.pg_engine_core, table)


def export_tables_to_csv(ctx: ETLContext, export_dir: str = "export") -> None:
    """
    Export all PostgreSQL tables to CSV files in the specified directory.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    export_dir : str, optional
        The directory where CSV files will be saved, by default "export"
    """
    # Create export directory if it doesn't exist
    export_path = Path(export_dir)
    export_path.mkdir(parents=True, exist_ok=True)

    logging.info(f"Exporting all tables to CSV in directory: {export_dir}")

    # Export each table to CSV
    all_tables = CORE_TABLES + AUAC_TABLES
    for table in all_tables:
        try:
            # Determine which database to use based on the table name
            if table in AUAC_TABLES:
                engine = ctx.pg_engine_auac
                db_name = "AUAC"
            else:
                engine = ctx.pg_engine_core
                db_name = "CORE"

            # Use SQLAlchemy directly to query the data
            with engine.connect() as connection:
                query = text(f"SELECT * FROM {table}")
                result = connection.execute(query)

                # Convert to pandas DataFrame
                df_pandas = pd.DataFrame(result.fetchall(), columns=result.keys())

                # Save to CSV
                csv_path = export_path / f"{table}.csv"
                df_pandas.to_csv(csv_path, index=False)

                logging.info(
                    f"Exported {len(df_pandas)} rows from {db_name} database table {table} to {csv_path}"
                )
        except Exception as e:
            logging.error(f"Error exporting table {table}: {e!s}")

    logging.info(f"Export completed. CSV files saved in {export_dir} directory")


def handle_created_at(creation_col: str = "CREATION") -> pl.Expr:
    """
    Handle the created_at timestamp field transformation.

    This function applies the standard transformation for created_at fields:
    - Uses the specified creation column from the source
    - Fills null values with current UTC time
    - Replaces timezone from "Europe/Rome" to None

    Parameters
    ----------
    creation_col : str, optional
        The name of the creation timestamp column, by default "CREATION"

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    return (
        pl.col(creation_col)
        .fill_null(datetime.now(timezone.utc).replace(tzinfo=None))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at")
    )


def handle_updated_at(last_mod_col: str = "LAST_MOD", creation_col: str = "CREATION") -> pl.Expr:
    """
    Handle the updated_at timestamp field transformation.

    This function applies the standard transformation for updated_at fields:
    - Uses the specified last modification column from the source
    - Fills null values with the creation column
    - Replaces timezone from "Europe/Rome" to None

    Parameters
    ----------
    last_mod_col : str, optional
        The name of the last modification timestamp column, by default "LAST_MOD"
    creation_col : str, optional
        The name of the creation timestamp column, by default "CREATION"

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    return (
        pl.col(last_mod_col)
        .fill_null(pl.col(creation_col))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at")
    )


def handle_disabled_at(
    disabled_col: str = "DISABLED",
    disabled_value: str = "S",
    last_mod_col: str = "LAST_MOD",
    creation_col: str = "CREATION",
    direct_disabled_col: str | None = None,
) -> pl.Expr:
    """
    Handle the disabled_at timestamp field transformation.

    This function applies the standard transformation for disabled_at fields:
    - If direct_disabled_col is provided, uses that column directly
    - Otherwise, conditionally sets when disabled_col equals disabled_value
    - When condition is met, uses last_mod_col (with fallback to creation_col)
    - Replaces timezone from "Europe/Rome" to None
    - Otherwise sets to None

    Parameters
    ----------
    disabled_col : str, optional
        The name of the disabled flag column, by default "DISABLED"
    disabled_value : str, optional
        The value indicating that the record is disabled, by default "S"
    last_mod_col : str, optional
        The name of the last modification timestamp column, by default "LAST_MOD"
    creation_col : str, optional
        The name of the creation timestamp column, by default "CREATION"
    direct_disabled_col : str | None, optional
        The name of a column that directly contains the disabled timestamp by default None

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    if direct_disabled_col is not None:
        return pl.col(direct_disabled_col).alias("disabled_at")

    return (
        pl.when(pl.col(disabled_col) == disabled_value)
        .then(
            pl.col(last_mod_col)
            .fill_null(pl.col(creation_col))
            .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
            .dt.replace_time_zone(None)
        )
        .otherwise(None)
        .alias("disabled_at")
    )


def handle_timestamps(
    creation_col: str = "CREATION",
    last_mod_col: str = "LAST_MOD",
    disabled_col: str = "DISABLED",
    disabled_value: str = "S",
    direct_disabled_col: str | None = None,
) -> dict[str, pl.Expr]:
    """
    Handle all timestamp field transformations at once.

    This function applies the standard transformations for created_at, updated_at, and disabled_at fields.

    Parameters
    ----------
    creation_col : str, optional
        The name of the creation timestamp column, by default "CREATION"
    last_mod_col : str, optional
        The name of the last modification timestamp column, by default "LAST_MOD"
    disabled_col : str, optional
        The name of the disabled flag column, by default "DISABLED"
    disabled_value : str, optional
        The value indicating that the record is disabled, by default "S"
    direct_disabled_col : str | None, optional
        The name of a column that directly contains the disabled timestamp by default None

    Returns
    -------
    dict[str, pl.Expr]
        A dictionary of polars expressions that can be used in a select statement
    """
    return {
        "created_at": handle_created_at(creation_col),
        "updated_at": handle_updated_at(last_mod_col, creation_col),
        "disabled_at": handle_disabled_at(
            disabled_col, disabled_value, last_mod_col, creation_col, direct_disabled_col
        ),
    }


def handle_id(source_id_col: str = "CLIENTID", target_id_col: str = "id") -> pl.Expr:
    """
    Handle the ID field transformation.

    This function applies the standard transformation for ID fields:
    - Strips leading and trailing whitespaces from the ID
    - Converts the ID to lowercase
    - Aliases the result as "id"

    Parameters
    ----------
    source_id_col : str, optional
        The name of the ID column of the source table, by default "CLIENTID"
    target_id_col : str, optional
        The name of the ID column of the target table, by default "id"

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    return (
        pl.col(source_id_col)
        .cast(pl.String)
        .str.strip_chars()
        .str.to_lowercase()
        .alias(target_id_col)
    )


def map_value(value: str | None, mapping: dict[str, str], default: str | None = None) -> str | None:
    """
    Map values using a dictionary.

    Parameters
    ----------
    value : str or None
        The value to map
    mapping : dict[str, str]
        Dictionary containing the mapping from input values to standardized values
    default : str or None, optional
        Default value to return if no mapping exists. If None, returns None.

    Returns
    -------
    str or None
        The mapped value, or default if no mapping exists
    """
    if value is None:
        return default

    value = value.strip().lower()
    return mapping.get(value, default)


def handle_enum_mapping(
    source_col: str, target_col: str, mapping_dict: dict, default: str | None = None
) -> pl.Expr:
    """
    Map values from a source column to standardized values using a mapping dictionary.

    This function applies string transformations (strip and lowercase) before mapping
    and returns a polars expression that can be used in a select statement.

    Parameters
    ----------
    source_col : str
        The name of the source column containing values to be mapped
    target_col : str
        The name to give to the resulting column
    mapping_dict : dict
        Dictionary containing the mapping from input values to standardized values
    default : str or None, optional
        Default value to return if no mapping exists. If None, returns None.

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    return (
        pl.col(source_col)
        .str.strip_chars()
        .str.to_lowercase()
        .map_elements(lambda x: map_value(x, mapping_dict, default=default), return_dtype=pl.String)
        .alias(target_col)
    )


def format_elapsed_time(start_time: datetime) -> str:
    """
    Calculate and format the elapsed time since start_time.

    Parameters
    ----------
    start_time : datetime
        The starting time

    Returns
    -------
    str
        Formatted string representing elapsed time in hours, minutes, and seconds
    """
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"


def create_bucket(bucket_name: str) -> None:
    """
    Create a MinIO bucket if it doesn't already exist.

    Parameters
    ----------
    bucket_name : str
        The name of the bucket to create

    Returns
    -------
    None
    """
    client = Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_ACCESS_KEY,
        secure=False,
    )

    found = client.bucket_exists(bucket_name)

    if not found:
        client.make_bucket(bucket_name)
        logging.info(f'Created MinIO bucket "{bucket_name}"')
    else:
        logging.info(f'Bucket MinIO "{bucket_name}" already exists')
