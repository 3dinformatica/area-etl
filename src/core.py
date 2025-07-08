import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl
from cx_Oracle import init_oracle_client
from sqlalchemy import create_engine, text, Engine

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
    import inspect
    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split('.')[-1]

    engine = ctx.oracle_engine if source == "oracle" else ctx.pg_engine
    df = pl.read_database(query, connection=engine.connect(), infer_schema_length=None)
    table_name = query.split("FROM")[1].strip().split()[0] if "FROM" in query.upper() else "unknown"
    logging.info(f'[{caller_module}] Extracted {df.height} rows from {source.upper()} table {table_name}')
    return df


def extract_data_from_csv(file_path: str, schema_overrides: dict = None) -> pl.DataFrame:
    """
    Generic function to extract data from a CSV file and log the extraction.

    Args:
        file_path: The path to the CSV file
        schema_overrides: Optional schema overrides for the CSV file

    Returns:
        A polars DataFrame containing the extracted data
    """
    import inspect
    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split('.')[-1]

    df = pl.read_csv(file_path, schema_overrides=schema_overrides)
    logging.info(f'[{caller_module}] Extracted {df.height} rows from CSV file {file_path}')
    return df


def load_data(ctx: ETLContext, df: pl.DataFrame, table_name: str) -> None:
    """
    Generic function to load data to a PostgreSQL database and log the load.

    Args:
        ctx: The ETL context containing database connections
        df: The polars DataFrame to load
        table_name: The name of the destination table
    """
    import inspect
    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split('.')[-1]

    df.write_database(table_name=table_name, connection=ctx.pg_engine, if_table_exists="append")
    logging.info(f'[{caller_module}] Loaded {df.height} rows into PostgreSQL table {table_name}')


def truncate_postgresql_tables(ctx: ETLContext) -> None:
    import inspect
    caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split('.')[-1]

    with ctx.pg_engine.connect() as conn:
        logging.info(f"[{caller_module}] Truncating all destination tables in PostgreSQL...")
        tables = [
            "regions",
            "provinces",
            "municipalities",
            "toponyms",
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
            "udo_types",
            "udos",
            "udo_production_factors",
            "udo_type_production_factor_types",
            "udo_specialties",
            "udo_resolutions",
            "udo_status_history",
            "resolutions",
            "resolution_types",
        ]
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
        conn.commit()
