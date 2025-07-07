import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

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
        format="[%(asctime)s][%(levelname)s] - %(message)s",
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


def truncate_postgresql_tables(ctx: ETLContext) -> None:
    with ctx.pg_engine.connect() as conn:
        logging.info("Truncating destination tables...")
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
            "udo_production_factors",
            "udo_type_production_factor_types",
            "udo_branches",
            "resolutions",
            "resolution_types",
        ]
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
        conn.commit()
