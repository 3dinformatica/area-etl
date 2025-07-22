import inspect
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from cx_Oracle import init_oracle_client
from sqlalchemy import Engine, create_engine, text

from src.settings import settings


@dataclass
class ETLContext:
    oracle_engine: Engine
    pg_engine: Engine


def setup_logging() -> None:
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
    init_oracle_client(lib_dir=settings.ORACLE_CLIENT_LIB_DIR)
    oracle_engine = create_engine(settings.ORACLE_URI)
    pg_engine = create_engine(settings.PG_URI)
    return ETLContext(oracle_engine=oracle_engine, pg_engine=pg_engine)


def extract_data(ctx: ETLContext, query: str, source: str = "oracle") -> pl.DataFrame:
    """
    Generic function to extract data from a database and log the extraction.

    Args:
        ctx: The ETL context containing database connections
        query: The SQL query to execute
        source: The source database ('oracle' or 'pg')

    Returns:
        A polars DataFrame containing the extracted data
    """

    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split(".")[-1]

    engine = ctx.oracle_engine if source == "oracle" else ctx.pg_engine
    df = pl.read_database(query, connection=engine.connect(), infer_schema_length=None)
    table_name = query.split("FROM")[1].strip().split()[0] if "FROM" in query.upper() else "unknown"
    logging.info(
        f"[{caller_module}] Extracted {df.height} rows from {source.upper()} table {table_name}"
    )
    return df


def extract_data_from_csv(file_path: str, schema_overrides: dict | None = None) -> pl.DataFrame:
    """
    Generic function to extract data from a CSV file and log the extraction.

    Args:
        file_path: The path to the CSV file
        schema_overrides: Optional schema overrides for the CSV file

    Returns:
        A polars DataFrame containing the extracted data
    """

    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split(".")[-1]

    df = pl.read_csv(file_path, schema_overrides=schema_overrides)
    logging.info(f"[{caller_module}] Extracted {df.height} rows from CSV file {file_path}")
    return df


def load_data(ctx: ETLContext, df: pl.DataFrame, table_name: str) -> None:
    """
    Generic function to load data to a PostgreSQL database and log the load.

    Args:
        ctx: The ETL context containing database connections
        df: The polars DataFrame to load
        table_name: The name of the destination table
    """

    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split(".")[-1]

    # Apply table prefix if set
    prefixed_table_name = f"{settings.PG_TABLE_PREFIX}{table_name}"

    df.write_database(
        table_name=prefixed_table_name, connection=ctx.pg_engine, if_table_exists="append"
    )
    logging.info(
        f"[{caller_module}] Loaded {df.height} rows into PostgreSQL table {prefixed_table_name}"
    )


def truncate_postgresql_tables(ctx: ETLContext) -> None:
    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split(".")[-1]

    with ctx.pg_engine.connect() as conn:
        logging.info(f"[{caller_module}] Truncating all destination tables in PostgreSQL...")
        tables = [
            "regions",
            "provinces",
            "municipalities",
            "toponyms",
            "districts",
            "company_types",
            "companies",
            "physical_structures",
            "operational_offices",
            "buildings",
            "grouping_specialties",
            "specialties",
            "users",
            "permissions",
            "user_companies",
            "production_factor_types",
            "production_factors",
            "udo_type_classifications",
            "udo_types",
            "operational_units",
            "udos",
            "udo_production_factors",
            "udo_type_production_factor_types",
            "udo_specialties",
            # "udo_resolutions",
            "udos_history",
            "ulss",
            "resolutions",
            "resolution_types",
        ]
        for table in tables:
            prefixed_table = f"{settings.PG_TABLE_PREFIX}{table}"
            conn.execute(text(f"TRUNCATE TABLE {prefixed_table} RESTART IDENTITY CASCADE"))
        conn.commit()


def export_tables_to_csv(ctx: ETLContext, export_dir: str = "export") -> None:
    """
    Export all PostgreSQL tables to CSV files in the specified directory.

    Args:
        ctx: The ETL context containing database connections
        export_dir: The directory where CSV files will be saved (default: "export")
    """

    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split(".")[-1]

    # Create export directory if it doesn't exist
    export_path = Path(export_dir)
    export_path.mkdir(parents=True, exist_ok=True)

    # Get list of tables
    tables = [
        "regions",
        "provinces",
        "municipalities",
        "toponyms",
        "districts",
        "company_types",
        "companies",
        "physical_structures",
        "operational_offices",
        "buildings",
        "grouping_specialties",
        "specialties",
        "users",
        "permissions",
        "user_companies",
        "production_factor_types",
        "production_factors",
        "udo_type_classifications",
        "udo_types",
        "operational_units",
        "udos",
        "udo_production_factors",
        "udo_type_production_factor_types",
        "udo_specialties",
        # "udo_resolutions",
        "udos_history",
        "ulss",
        "resolutions",
        "resolution_types",
    ]

    logging.info(f"[{caller_module}] Exporting all tables to CSV in directory: {export_dir}")

    # Export each table to CSV
    for table in tables:
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
