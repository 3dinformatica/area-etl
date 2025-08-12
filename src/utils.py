import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
from cx_Oracle import init_oracle_client
from minio import Minio
from sqlalchemy import Engine, create_engine, text

from settings import settings


@dataclass
class ETLContext:
    """
    Context object for ETL operations containing database connections.

    Attributes
    ----------
    oracle_engine_area : Engine
        SQLAlchemy engine for Oracle database connection for main A.Re.A. services
    oracle_engine_poa : Engine
        SQLAlchemy engine for Oracle database connection for POA A.Re.A. services
    pg_engine_core : Engine
        SQLAlchemy engine for PostgreSQL A.Re.A. Core service database connection
    pg_engine_poa : Engine
        SQLAlchemy engine for PostgreSQL A.Re.A. POA service database connection
    pg_engine_cronos : Engine
        SQLAlchemy engine for PostgreSQL A.Re.A. Cronos service database connection
    pg_engine_auac : Engine
        SQLAlchemy engine for PostgreSQL A.Re.A. Au.Ac. service database connection
    minio_client : Minio
        MinIO client for object storage operations
    """

    oracle_engine_area: Engine
    oracle_engine_poa: Engine
    pg_engine_core: Engine
    pg_engine_poa: Engine
    pg_engine_cronos: Engine
    pg_engine_auac: Engine
    minio_client: Minio


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

    This function:

    - Initializes the Oracle client.
    - Creates SQLAlchemy engines for Oracle (area, poa) and PostgreSQL (core, poa, cronos, auac).
    - Initializes a MinIO client for object storage.

    Configuration (environment variables):

    - ``ORACLE_CLIENT_LIB_DIR``: Absolute path to Oracle Instant Client directory.
    - ``ORACLE_URI_AREA``: SQLAlchemy URI for Oracle (e.g., 'oracle://user:pass@host:1521/SERVICE').
    - ``ORACLE_URI_POA``: SQLAlchemy URI for Oracle.
    - ``PG_URI_CORE`` | ``POA`` | ``CRONOS`` | ``AUAC``: SQLAlchemy URIs for PostgreSQL databases.
    - ``MINIO_ENDPOINT``: MinIO endpoint which may be provided as 'host[:port]' or full URL with scheme. Any path
      component will be ignored. Examples: 'localhost:9000', 'https://minio.company.it'.
    - ``MINIO_SECURE``: 'true'/'false' to force HTTPS (True) or HTTP (False). If ``MINIO_ENDPOINT`` contains a scheme,
      that scheme takes precedence.
    - ``MINIO_ACCESS_KEY``: Access key for MinIO.
    - ``MINIO_SECRET_KEY``: Secret key for MinIO.
    - ``ATTACHMENTS_DIR``: Directory for storing attachments.

    MinIO security:

    If your MinIO instance requires HTTPS/TLS, either use an https:// MINIO_ENDPOINT or set MINIO_SECURE=true.

    Returns
    -------
    ETLContext
        Context object containing all database connections and the MinIO client.
    """
    init_oracle_client(lib_dir=settings.ORACLE_CLIENT_LIB_DIR)
    oracle_engine_area = create_engine(settings.ORACLE_URI_AREA)
    oracle_engine_poa = create_engine(settings.ORACLE_URI_POA)
    pg_engine_core = create_engine(settings.PG_URI_CORE)
    pg_engine_poa = create_engine(settings.PG_URI_POA)
    pg_engine_cronos = create_engine(settings.PG_URI_CRONOS)
    pg_engine_auac = create_engine(settings.PG_URI_AUAC)

    # Build MinIO client with robust endpoint handling
    raw_endpoint = settings.MINIO_ENDPOINT.strip()
    parsed = urlparse(raw_endpoint if "://" in raw_endpoint else f"//{raw_endpoint}", scheme="http")
    # If scheme is missing, urlparse will treat it as netloc due to '//'.
    scheme = parsed.scheme if parsed.scheme and parsed.scheme != "" else "http"
    netloc = parsed.netloc if parsed.netloc else parsed.path
    # Remove any accidental path/query/fragment
    endpoint = netloc

    # Determine secure flag: https -> True; else fallback to settings.MINIO_SECURE
    secure = True if scheme.lower() == "https" else bool(settings.MINIO_SECURE)

    if parsed.path not in ("", "/"):
        logging.warning(
            "MINIO_ENDPOINT contains a path '%s' which will be ignored. Using endpoint '%s' with secure=%s",
            parsed.path,
            endpoint,
            secure,
        )

    minio_client = Minio(
        endpoint,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=secure,
        http_client=None,
    )

    return ETLContext(
        oracle_engine_area=oracle_engine_area,
        oracle_engine_poa=oracle_engine_poa,
        pg_engine_core=pg_engine_core,
        pg_engine_poa=pg_engine_poa,
        pg_engine_cronos=pg_engine_cronos,
        pg_engine_auac=pg_engine_auac,
        minio_client=minio_client,
    )


def extract_data(engine: Engine, query: str) -> pl.DataFrame:
    """
    Extract data from a database using a SQL query.

    This function executes the provided SQL query against the database connection
    and returns the results as a Polars DataFrame.

    Parameters
    ----------
    engine : Engine
        The SQLAlchemy engine connection to the database
    query : str
        The SQL query to execute

    Returns
    -------
    pl.DataFrame
        A polars DataFrame containing the query results
    """
    df = pl.read_database(query, connection=engine.connect(), infer_schema_length=None)

    # Extract the table name from the input query for logging
    table_name = "unknown"
    if "FROM" in query.upper():
        parts = query.upper().split("FROM")
        if len(parts) > 1:
            table_parts = parts[1].strip().split()
            if table_parts:
                table_name = table_parts[0]

    logging.info(f'Extracted {df.height} rows from {engine} table "{table_name}"')
    return df


def extract_data_from_csv(file_path: str | os.PathLike, schema_overrides: dict | None = None) -> pl.DataFrame:
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


def load_data(engine: Engine, df: pl.DataFrame, table_name: str) -> None:
    """
    Load data from a Polars DataFrame into a database table.

    This function writes the contents of the provided DataFrame to the specified
    database table, appending to the table if it already exists.

    Parameters
    ----------
    engine : Engine
        The SQLAlchemy engine connection to the database
    df : pl.DataFrame
        The Polars DataFrame containing the data to load
    table_name : str
        The name of the target database table
    """
    df.write_database(table_name=table_name, connection=engine, if_table_exists="append")
    logging.info(f'Loaded {df.height} rows in {engine} table "{table_name}"')


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


def export_tables_to_csv(engine: Engine, tables: list[str], export_dir: str = "export") -> None:
    """
    Export database tables to CSV files.

    This function extracts data from the specified database tables and saves each
    table as a separate CSV file in the specified export directory.

    Parameters
    ----------
    engine : Engine
        The SQLAlchemy engine connection to the database
    tables : list[str]
        A list of table names to export
    export_dir : str, optional
        The directory where CSV files will be saved, by default "export"
    """
    export_path = Path(export_dir).absolute()
    export_path.mkdir(parents=True, exist_ok=True)

    logging.info(f"Exporting selected tables to CSV in directory: {export_path}")

    for table in tables:
        df = extract_data(engine, f"SELECT * FROM {table}")
        csv_path = export_path / f"{table}.csv"
        df.to_pandas().to_csv(csv_path, index=False)
        logging.info(f"Exported {df.height} rows from {engine} database table {table} to {csv_path}")

    logging.info(f"Export completed. CSV files saved in {export_path} directory")


def handle_created_at(creation_col: str = "CREATION", current_time: datetime | None = None) -> pl.Expr:
    """
    Handle the created_at timestamp field transformation.

    This function applies the standard transformation for created_at fields:
    - Uses the specified creation column from the source
    - Fills null values with provided current_time or generates a new UTC time if not provided
    - Replaces timezone from "Europe/Rome" to None

    Parameters
    ----------
    creation_col : str, optional
        The name of the creation timestamp column, by default "CREATION"
    current_time : datetime, optional
        A timestamp to use for null values, by default None (will use current time)

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    return (
        pl.col(creation_col)
        .fill_null(current_time)
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at")
    )


def handle_updated_at(
    last_mod_col: str = "LAST_MOD",
    creation_col: str = "CREATION",
    current_time: datetime | None = None,
) -> pl.Expr:
    """
    Handle the updated_at timestamp field transformation.

    This function applies the standard transformation for updated_at fields:
    - Uses the specified last modification column from the source
    - Fills null values with the creation column
    - If both last_mod_col and creation_col are null, uses the provided current_time or generates a new timestamp
    - Replaces timezone from "Europe/Rome" to None

    Parameters
    ----------
    last_mod_col : str, optional
        The name of the last modification timestamp column, by default "LAST_MOD"
    creation_col : str, optional
        The name of the creation timestamp column, by default "CREATION"
    current_time : datetime, optional
        A timestamp to use when both columns are null, by default None (will use current time)

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    return (
        pl.col(last_mod_col)
        .fill_null(pl.col(creation_col))
        .fill_null(current_time)
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
    It ensures that created_at and updated_at use the same timestamp when both source columns are null.

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
    # Generate a single timestamp to use for both created_at and updated_at when source columns are null
    current_time = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    return {
        "created_at": handle_created_at(creation_col, current_time),
        "updated_at": handle_updated_at(last_mod_col, creation_col, current_time),
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
    return pl.col(source_id_col).cast(pl.String).str.strip_chars().str.to_lowercase().alias(target_id_col)


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


def handle_enum_mapping(source_col: str, target_col: str, mapping_dict: dict, default: str | None = None) -> pl.Expr:
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


def handle_text(source_col: str, target_col: str) -> pl.Expr:
    """
    Clean and standardize text data from a source column.

    This function applies several text cleaning operations:
    - Converts to string type
    - Strips leading and trailing whitespaces
    - Removes newlines and carriage returns
    - Normalizes multiple whitespaces to a single space

    Parameters
    ----------
    source_col : str
        The name of the source column containing text to clean
    target_col : str
        The name to give to the resulting column

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    return (
        pl.col(source_col)
        .cast(pl.String)
        .str.strip_chars()
        .str.replace_all("\n", "")
        .str.replace_all("\r", "")
        .str.replace_all(r"\s+", " ")
        .alias(target_col)
    )


def handle_year(source_col: str, target_col: str) -> pl.Expr:
    """
    Convert a string column to an integer year.

    This function strips whitespace from the source column and casts it to a 32-bit integer.

    Parameters
    ----------
    source_col : str
        The name of the source column containing year data as string
    target_col : str
        The name to give to the resulting column

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    return pl.col(source_col).str.strip_chars().cast(pl.Int32).alias(target_col)


def handle_datetime(source_col: str, target_col: str) -> pl.Expr:
    """
    Standardize datetime column by removing timezone information.

    This function removes timezone information from a datetime column,
    resolving ambiguous times to the earliest possible time.

    Parameters
    ----------
    source_col : str
        The name of the source column containing datetime data
    target_col : str
        The name to give to the resulting column

    Returns
    -------
    pl.Expr
        A polars expression that can be used in a select statement
    """
    return pl.col(source_col).cast(pl.Datetime).dt.replace_time_zone(None, ambiguous="earliest").alias(target_col)
