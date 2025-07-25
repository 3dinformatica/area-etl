import inspect
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import polars as pl
from cx_Oracle import init_oracle_client
from sqlalchemy import Engine, create_engine, text

from src.settings import settings

# List of tables used in multiple functions
TABLES = [
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


@dataclass
class ETLContext:
    """
    Context object for ETL operations containing database connections.

    Attributes
    ----------
    oracle_engine : Engine
        SQLAlchemy engine for Oracle database connection
    pg_engine : Engine
        SQLAlchemy engine for PostgreSQL database connection
    """

    oracle_engine: Engine
    pg_engine: Engine


def get_caller_module() -> str:
    """
    Get the name of the calling module.

    Returns
    -------
    str
        The name of the calling module
    """
    frame = inspect.currentframe()
    if frame is None or frame.f_back is None:
        return "unknown"

    module = inspect.getmodule(frame.f_back)
    if module is None:
        return "unknown"

    return module.__name__.split(".")[-1]


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
        format="%(asctime)s | %(levelname)s | %(module)s | %(message)s",
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
    for both Oracle and PostgreSQL databases.

    Returns
    -------
    ETLContext
        Context object containing database connections
    """
    init_oracle_client(lib_dir=settings.ORACLE_CLIENT_LIB_DIR)
    oracle_engine = create_engine(settings.ORACLE_URI)
    pg_engine = create_engine(settings.PG_URI)
    return ETLContext(oracle_engine=oracle_engine, pg_engine=pg_engine)


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
        The source database ('oracle' or 'pg'), by default "oracle"

    Returns
    -------
    pl.DataFrame
        A polars DataFrame containing the extracted data
    """
    caller_module = get_caller_module()

    engine = ctx.oracle_engine if source == "oracle" else ctx.pg_engine
    df = pl.read_database(query, connection=engine.connect(), infer_schema_length=None)

    # Extract table name from query for logging
    table_name = "unknown"
    if "FROM" in query.upper():
        parts = query.upper().split("FROM")
        if len(parts) > 1:
            table_parts = parts[1].strip().split()
            if table_parts:
                table_name = table_parts[0]

    logging.info(
        f"[{caller_module}] Extracted {df.height} rows from {source.upper()} table {table_name}"
    )
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
    caller_module = get_caller_module()

    df = pl.read_csv(file_path, schema_overrides=schema_overrides)
    absolute_path = Path(file_path).absolute()
    logging.info(f"[{caller_module}] Extracted {df.height} rows from CSV file {absolute_path}")
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
    caller_module = get_caller_module()

    # Apply table prefix if set
    prefixed_table_name = f"{settings.PG_TABLE_PREFIX}{table_name}"

    df.write_database(
        table_name=prefixed_table_name, connection=ctx.pg_engine, if_table_exists="append"
    )
    logging.info(
        f"[{caller_module}] Loaded {df.height} rows into PostgreSQL table {prefixed_table_name}"
    )


def truncate_postgresql_tables(ctx: ETLContext) -> None:
    """
    Truncate all destination tables in PostgreSQL.

    Clears all data from the tables listed in the TABLES constant,
    resetting identity sequences and cascading the truncation.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    caller_module = get_caller_module()

    with ctx.pg_engine.connect() as conn:
        logging.info(f"[{caller_module}] Truncating all destination tables in PostgreSQL...")
        for table in TABLES:
            prefixed_table = f"{settings.PG_TABLE_PREFIX}{table}"
            conn.execute(text(f"TRUNCATE TABLE {prefixed_table} RESTART IDENTITY CASCADE"))
        conn.commit()


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
    caller_module = get_caller_module()

    # Create export directory if it doesn't exist
    export_path = Path(export_dir)
    export_path.mkdir(parents=True, exist_ok=True)

    logging.info(f"[{caller_module}] Exporting all tables to CSV in directory: {export_dir}")

    # Export each table to CSV
    for table in TABLES:
        try:
            # Use SQLAlchemy directly to query the data
            with ctx.pg_engine.connect() as connection:
                prefixed_table = f"{settings.PG_TABLE_PREFIX}{table}"
                query = text(f"SELECT * FROM {prefixed_table}")
                result = connection.execute(query)

                # Convert to pandas DataFrame
                df_pandas = pd.DataFrame(result.fetchall(), columns=result.keys())

                # Save to CSV
                csv_path = export_path / f"{table}.csv"  # Keep original table name for CSV file
                df_pandas.to_csv(csv_path, index=False)

                logging.info(
                    f"[{caller_module}] Exported {len(df_pandas)} rows from table {prefixed_table} to {csv_path}"
                )
        except Exception as e:
            logging.error(f"[{caller_module}] Error exporting table {table}: {e!s}")

    logging.info(f"[{caller_module}] Export completed. CSV files saved in {export_dir} directory")


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
        .dt.replace_time_zone("Europe/Rome")
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
        .dt.replace_time_zone("Europe/Rome")
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
        The name of a column that directly contains the disabled timestamp, by default None

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
            .dt.replace_time_zone("Europe/Rome")
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
        The name of a column that directly contains the disabled timestamp, by default None

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
