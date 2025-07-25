import concurrent.futures
import inspect
import logging
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from cx_Oracle import init_oracle_client
from sqlalchemy import Engine, create_engine, text, inspect

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

    caller_module = "unknown"
    try:
        caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split(".")[-1]
    except AttributeError:
        logging.warning("Could not determine caller module")

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

    caller_module = "unknown"
    try:
        caller_module = inspect.getmodule(inspect.currentframe().f_back).__name__.split(".")[-1]
    except AttributeError:
        logging.warning("Could not determine caller module")

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
            # "regions",
            # "provinces",
            # "municipalities",
            # "toponyms",
            # "districts",
            # "company_types",
            # "companies",
            # "physical_structures",
            # "operational_offices",
            # "buildings",
            # "grouping_specialties",
            # "specialties",
            # "users",
            # "permissions",
            # "user_companies",
            # "production_factor_types",
            # "production_factors",
            # "udo_types",
            # "operational_units",
            # "udos",
            # "udo_production_factors",
            # "udo_type_production_factor_types",
            # "udo_specialties",
            # "udo_resolutions",
            # "udo_status_history",
            # "resolutions",
            # "resolution_types",
            "requirement_taxonomies",
            "requirement_lists",
            "requirementlist_requirements",
            "requirements",
            "procedure_type_requirement_list_classification_mental",
            "procedure_type_requirement_list_comp_type_comp_class",
            "procedure_type_requirement_list_udo_type",
            "procedure_type_requirement_list_for_physical_structures",
            "procedures",
            "procedure_entities",
            "procedure_entity_requirements",
            "areas",
            "sub_areas",
            "legal_inquiries_types",
            "legal_inquiries",
            "organigrams",
            "models",
            "organigram_emails",
            "notifications",
            "node_types",
            "nodes",
            "rule_types",
            "rules",
            "function_diagrams",
            "function_diagram_nodes",
            "organigram_attachments",
            "resolution_types",
            "resolutions",
            "function_diagram_nodes",
            "cronos_taxonomies",
            "dm70_taxonomies",
            "cronos_plan_grouping_specialties",
            "cronos_physical_structures",
            "healthcare_companies",
            "cronos_plan_specialty_aliases",
            "cronos_companies",
            "cronos_plan_specialties",
            "cronos_plans",
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
        "udo_types",
        "operational_units",
        "udos",
        "udo_production_factors",
        "udo_type_production_factor_types",
        "udo_specialties",
        "udo_resolutions",
        "udo_status_history",
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


def set_boolean_from_values_0_1(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """
    Convert columns with values 0/1 to boolean True/False.

    Args:
        df: The DataFrame to process
        columns: List of column names to convert

    Returns:
        DataFrame with converted boolean columns
    """
    for col in columns:
        if col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(col) == 1)
                .then(True)
                .otherwise(False)
                .alias(col)
            )
    return df


def process_single_regulation_attachment(args: tuple) -> tuple:
    """
    Process a single regulation attachment and return the result.

    Args:
        args: A tuple containing (index, row, poa_engine, storage_service_url, category)

    Returns:
        A tuple of (index, file_id) or (index, None) if there was an error
    """
    index, row, poa_engine, storage_service_url, category = args
    id_allegato_fk = row['id_allegato_blob']
    nome_allegato = row['descrizione_allegato']

    if not id_allegato_fk:
        return index, None

    try:
        query = f"SELECT allegato FROM allegati_blob WHERE id = '{id_allegato_fk}'"
        attachment_row = pl.read_database(query=query, connection=poa_engine)

        if attachment_row.height > 0:
            allegato_content = attachment_row[0, 0]
            allegato_file = BytesIO(allegato_content)

            files = {
                'file': (nome_allegato, allegato_file, 'application/octet-stream')
            }

            response = requests.post(
                f"{storage_service_url}/api/storage/{category}",
                files=files
            )
            response.raise_for_status()

            file_id = response.json().get('key', None)

            return index, file_id
    except Exception as e:
        print(f"Error processing attachment: {e}")

    return index, None


def save_regulation_with_attachments(df: pl.DataFrame, poa_engine, storage_service_url: str, category: str,
                                     max_workers: int = 10) -> pl.DataFrame:
    """
    Process regulation attachments in parallel.

    Args:
        df: DataFrame containing regulations with attachments
        poa_engine: SQLAlchemy engine for POA database
        storage_service_url: URL of the storage service
        category: Category for the storage service
        max_workers: Maximum number of parallel workers

    Returns:
        DataFrame with file_id column updated
    """
    df = df.with_columns(pl.lit(None).cast(pl.String).alias("file_id"))

    rows_with_attachments = df.filter(pl.col("id_allegato_blob").is_not_null())

    if rows_with_attachments.height == 0:
        return df

    args_list = []
    for i, row in enumerate(rows_with_attachments.iter_rows(named=True)):
        args_list.append((i, row, poa_engine, storage_service_url, category))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_single_regulation_attachment, args_list))

    for i, file_id in results:
        if file_id is not None:
            row_index = rows_with_attachments.row_index[i]
            df = df.with_row_index().with_columns(
                pl.when(pl.col("row_nr") == row_index)
                .then(pl.lit(file_id))
                .otherwise(pl.col("file_id"))
                .alias("file_id")
            ).drop("row_nr")

    return df


def process_single_organigram_attachment(args: tuple) -> tuple:
    """
    Process a single organigram attachment and return the result.

    Args:
        args: A tuple containing (index, row, poa_engine, storage_service_url, category)

    Returns:
        A tuple of (index, file_id) or (index, None) if there was an error
    """
    index, row, poa_engine, storage_service_url, category = args
    id_allegato_fk = row['id_allegato_blob']
    nome_allegato = row['descrizione']

    if not id_allegato_fk:
        return index, None

    try:
        query = f"SELECT allegato FROM allegati_blob WHERE id = '{id_allegato_fk}'"
        attachment_row = pl.read_database(query=query, connection=poa_engine)

        if attachment_row.height > 0:
            allegato_content = attachment_row[0, 0]
            allegato_file = BytesIO(allegato_content)

            files = {
                'file': (nome_allegato, allegato_file, 'application/octet-stream')
            }

            response = requests.post(
                f"{storage_service_url}/api/storage/{category}",
                files=files
            )
            response.raise_for_status()

            # Get the file ID from the response
            file_id = response.json().get('key', None)

            return index, file_id
    except Exception as e:
        print(f"Error processing attachment: {e}")

    return index, None


def save_organigram_with_attachments(df: pl.DataFrame, poa_engine, storage_service_url: str, category: str,
                                     max_workers: int = 10) -> pl.DataFrame:
    """
    Process orgranmigram attachments in parallel.

    Args:
        df: DataFrame containing orgranmigram with attachments
        poa_engine: SQLAlchemy engine for POA database
        storage_service_url: URL of the storage service
        category: Category for the storage service
        max_workers: Maximum number of parallel workers

    Returns:
        DataFrame with file_id column updated
    """
    df = df.with_columns(pl.lit(None).cast(pl.String).alias("file_id"))

    rows_with_attachments = df.filter(pl.col("id_allegato_blob").is_not_null())

    if rows_with_attachments.height == 0:
        return df

    args_list = []
    for i, row in enumerate(rows_with_attachments.iter_rows(named=True)):
        args_list.append((i, row, poa_engine, storage_service_url, category))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_single_regulation_attachment, args_list))

    for i, file_id in results:
        if file_id is not None:
            row_index = rows_with_attachments.row_index[i]
            df = df.with_row_index().with_columns(
                pl.when(pl.col("row_nr") == row_index)
                .then(pl.lit(file_id))
                .otherwise(pl.col("file_id"))
                .alias("file_id")
            ).drop("row_nr")

    return df


def create_tables(file_path, postgresql_engine, table_name):
    with postgresql_engine.connect() as connection:
        transaction = connection.begin()
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_script = file.read()
            logging.info(f"Creating table for {table_name}")
            connection.execute(text(sql_script))
            logging.info(f"Table for {table_name} created successfully!")
            transaction.commit()
        except Exception as create_error:
            logging.error(f"Error creating table for {table_name}: {create_error}")
            transaction.rollback()
            raise create_error


def check_table_exists(oracle_engine, source_table_name, oracle_schema):
    """
    Controlla se una tabella esiste nel database Oracle.

    Args:
        oracle_engine (Engine): Il motore SQLAlchemy per connettersi al database.
        source_table_name (str): Il nome della tabella da verificare.
        oracle_schema (str): Lo schema della tabella.

    Returns:
        bool: True se la tabella esiste, False altrimenti.
    """
    inspector = inspect(oracle_engine)

    if not inspector.has_table(source_table_name, schema=oracle_schema):
        logging.warning(f"The table {source_table_name} not exist, the script will be skipped")
        return False
    else:
        return True


def check_schema_exists(oracle_engine, schema_name):
    """
    Controlla se uno schema esiste nel database Oracle.

    Args:
        oracle_engine (Engine): Il motore SQLAlchemy per connettersi al database.
        schema_name (str): Il nome dello schema da verificare.

    Returns:
        bool: True se lo schema esiste, False altrimenti.
    """
    inspector = inspect(oracle_engine)
    schemas = inspector.get_schema_names()

    if schema_name.lower() not in [schema.lower() for schema in schemas]:
        logging.warning(f"The schema {schema_name} does not exist in the Oracle database. The script will not be executed.")
        return False
    else:
        return True


def is_table_empty(oracle_engine, source_table_name, oracle_schema):
    """
    Controlla se una tabella nel database Oracle è vuota.

    Args:
        oracle_engine (Engine): Il motore SQLAlchemy per connettersi al database.
        source_table_name (str): Il nome della tabella da controllare.
        oracle_schema (str): Lo schema della tabella.

    Returns:
        bool: True se la tabella è vuota, False se contiene almeno una riga.
    """
    with oracle_engine.connect() as connection:
        query = text(f"""
        SELECT 1 
        FROM {oracle_schema}.{source_table_name} 
        WHERE ROWNUM = 1
        """)

        result = connection.execute(query)

        if not result.fetchone():
            logging.warning(f"The table {source_table_name} is empty, the script will be skipped.")
            return True
        else:
            return False