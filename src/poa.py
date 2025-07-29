import logging

from utils import ETLContext, truncate_pg_table

POA_TABLES = [
    "areas",
    "function_diagram_nodes",
    "function_diagrams",
    "legal_inquiries",
    "legal_inquiries_types",
    "models",
    "node_types",
    "nodes",
    "notifications",
    "organigram_attachments",
    "organigram_emails",
    "organigrams",
    "parameters",
    "rule_types",
    "rules",
    "sub_areas",
]


def truncate_poa_tables(ctx: ETLContext) -> None:
    """
    Truncate all the tables in the PostgreSQL database of A.Re.A. POA service.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    logging.info(f"Truncating all target tables in PostgreSQL {ctx.pg_engine_poa}...")

    for table in POA_TABLES:
        truncate_pg_table(ctx.pg_engine_poa, table)


def migrate_poa(ctx: ETLContext) -> None:
    """
    Migrate data from source databases to the POA service database.

    This function orchestrates the ETL process for the POA service,
    currently only truncating all target tables.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    truncate_poa_tables(ctx)
