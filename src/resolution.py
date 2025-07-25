import logging
import os
import shutil
import zipfile
from pathlib import Path

import polars as pl

from core import ETLContext, extract_data, handle_timestamps, load_data
from src.settings import settings


def download_attachments(
    ctx: ETLContext,
    df: pl.DataFrame,
    chunk_size: int = 500,
) -> None:
    """
    Download resolution attachments from Oracle and save them as a ZIP archive.

    This function extracts attachment files from the Oracle database based on file IDs
    in the provided DataFrame, saves them to a directory structure organized by resolution ID,
    creates a ZIP archive of all attachments, and then deletes the original directory.

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    df: pl.DataFrame
        DataFrame containing resolution data with file_id column
    chunk_size: int, default=500
        Number of file IDs to process in each database query chunk
    """
    df_resolutions_with_files = df.drop_nulls("file_id").select(["id", "file_id"])
    attachments_dir = Path(settings.ATTACHMENTS_DIR) / "resolutions"
    attachments_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Downloading {df_resolutions_with_files.height} attachments to {attachments_dir}")
    ids = pl.Series(df_resolutions_with_files.select("file_id")).to_list()
    id_chunks = [ids[i : i + chunk_size] for i in range(0, len(ids), chunk_size)]
    logging.info(
        f"{len(ids)} attachments divided into {len(id_chunks)} chunks with size {chunk_size}"
    )
    dfs_attachments = [
        extract_data(
            ctx,
            f"SELECT * FROM AUAC_USR.BINARY_ATTACHMENTS_APPL WHERE CLIENTID IN ({','.join([f"'{id}'" for id in chunk])})",
        )
        for chunk in id_chunks
    ]
    df_attachments = pl.concat(dfs_attachments, how="vertical_relaxed")
    df_result = df_resolutions_with_files.join(
        df_attachments,
        left_on="file_id",
        right_on="CLIENTID",
        how="left",
    ).select(["id", "ALLEGATO", "NOME", "TIPO"])

    for row in df_result.iter_rows():
        resolution_id = row[0]
        attachment_bytes = row[1]
        attachment_name = row[2]
        resolution_dir = attachments_dir / resolution_id
        resolution_dir.mkdir(parents=True, exist_ok=True)
        safe_attachment_name = attachment_name.replace("/", "_").replace("\\", "_")

        with open(resolution_dir / safe_attachment_name, "wb") as f:
            f.write(attachment_bytes)

    # Create a ZIP file of the attachments directory
    logging.info(f"Creating ZIP archive of {attachments_dir}")
    zip_file_path = str(attachments_dir) + ".zip"
    with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(attachments_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, attachments_dir)
                zipf.write(file_path, arcname)

    # Delete the original directory after successful zipping
    logging.info(f"Deleting original directory {attachments_dir}")
    shutil.rmtree(attachments_dir)
    logging.info(f"ZIP archive created at {zip_file_path}")


def migrate_resolution_types(ctx: ETLContext) -> None:
    """
    Migrate resolution types from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_tipo_delibera = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_DELIBERA")
    df_tipo_atto = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_ATTO")

    ### TRANSFORM ###
    # Get timestamp expressions
    timestamp_exprs = handle_timestamps()

    df_tipo_delibera = df_tipo_delibera.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().str.to_uppercase().alias("name"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )
    # Get timestamp expressions
    timestamp_exprs = handle_timestamps()

    df_tipo_atto = df_tipo_atto.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().str.to_uppercase().alias("name"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )
    df_result = pl.concat([df_tipo_delibera, df_tipo_atto], how="vertical")
    df_result = df_result.unique("name")

    ### LOAD ###
    load_data(ctx, df_result, "resolution_types")


def migrate_resolutions(ctx: ETLContext) -> None:
    """
    Migrate resolutions from Oracle to PostgreSQL.

    Args:
        ctx: The ETL context containing database connections
    """
    ### EXTRACT ###
    df_delibera_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.DELIBERA_TEMPL")
    df_atto_model = extract_data(ctx, "SELECT * FROM AUAC_USR.ATTO_MODEL")
    df_tipo_proc_templ = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_PROC_TEMPL")
    df_tipo_delibera = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_DELIBERA")
    df_tipo_atto = extract_data(ctx, "SELECT * FROM AUAC_USR.TIPO_ATTO")
    df_resolution_types = extract_data(ctx, "SELECT * FROM resolution_types", source="pg")

    ### TRANSFORM ###
    # Column "id" is read as an object and not as a string
    df_resolution_types = df_resolution_types.to_pandas()
    df_resolution_types["id"] = df_resolution_types["id"].astype("string")
    df_resolution_types = pl.from_pandas(df_resolution_types)

    # Get timestamp expressions
    timestamp_exprs = handle_timestamps()

    df_delibera_templ = df_delibera_templ.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().alias("name"),
        pl.col("TIPO_DELIBERA").str.strip_chars().fill_null("ALTRO").alias("category"),
        pl.col("NUMERO").str.strip_chars().alias("number"),
        pl.col("ANNO").str.strip_chars().alias("year"),
        pl.col("INIZIO_VALIDITA")
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("valid_from"),
        pl.col("FINE_VALIDITA")
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("valid_to"),
        pl.col("ID_ALLEGATO_FK").alias("file_id"),  # Will be processed by download_attachments
        pl.col("N_BUR").cast(pl.String).str.strip_chars().alias("bur_number"),
        pl.col("DATA_BUR")
        .dt.replace_time_zone("Europe/Rome")
        .dt.replace_time_zone(None)
        .alias("bur_date"),
        pl.col("LINK_DGR").str.strip_chars().alias("dgr_link"),
        pl.col("DIREZIONE").str.strip_chars().alias("direction"),
        pl.col("ID_TIPO_FK").str.strip_chars().alias("resolution_type_id"),
        pl.lit(None).alias("company_id"),
        pl.lit(None).alias("procedure_type"),
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )
    # Get timestamp expressions
    timestamp_exprs = handle_timestamps()

    df_atto_model = df_atto_model.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("ID_TIPO_FK").str.strip_chars(),
        pl.col("ID_TITOLARE_FK").str.strip_chars().alias("company_id"),
        pl.col("ID_TIPO_PROC_FK"),
        pl.col("ANNO").str.strip_chars().alias("year"),
        pl.col("NUMERO").str.strip_chars().alias("number"),
        pl.col("INIZIO_VALIDITA")
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("valid_from"),
        pl.col("FINE_VALIDITA")
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("valid_to"),
        pl.col("ID_ALLEGATO_FK").alias("file_id"),  # Will be processed by download_attachments
        timestamp_exprs["created_at"],
        timestamp_exprs["updated_at"],
        timestamp_exprs["disabled_at"],
    )
    df_tipo_proc_templ = df_tipo_proc_templ.select(
        pl.col("CLIENTID"),
        pl.col("DESCR")
        .str.strip_chars()
        .str.replace(r" ", "_")
        .str.replace(r"\.", "")
        .str.to_uppercase()
        .alias("procedure_type"),
    )
    df_tipo_delibera = df_tipo_delibera.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("NOME").str.strip_chars().str.to_uppercase().alias("resolution_type_name"),
    )
    df_tipo_atto = df_tipo_atto.select(
        pl.col("CLIENTID").str.strip_chars().alias("id"),
        pl.col("DESCR").str.strip_chars().str.to_uppercase().alias("resolution_type_name"),
    )
    df_tipo_delibera_concat_tipo_atto = pl.concat([df_tipo_delibera, df_tipo_atto], how="vertical")
    df_resolution_types = df_resolution_types.select(
        pl.col("id").alias("resolution_type_id"),
        pl.col("name"),
    )

    df_result = df_atto_model.join(
        df_tipo_proc_templ,
        left_on="ID_TIPO_PROC_FK",
        right_on="CLIENTID",
        how="left",
    )
    df_result = df_result.join(
        df_tipo_delibera_concat_tipo_atto,
        left_on="ID_TIPO_FK",
        right_on="id",
        how="left",
    )
    df_result = df_result.join(
        df_resolution_types,
        left_on="resolution_type_name",
        right_on="name",
        how="left",
    )

    df_result = df_result.with_columns(
        category=pl.lit("UDO"),
        name=pl.lit(None),
        bur_number=pl.lit(None),
        bur_date=pl.lit(None),
        dgr_link=pl.lit(None),
        direction=pl.lit(None),
    ).drop(["ID_TIPO_PROC_FK", "ID_TIPO_FK", "resolution_type_name"])

    sorted_cols = [
        "id",
        "name",
        "category",
        "number",
        "year",
        "valid_from",
        "valid_to",
        "file_id",
        "bur_number",
        "bur_date",
        "dgr_link",
        "direction",
        "resolution_type_id",
        "company_id",
        "procedure_type",
        "created_at",
        "updated_at",
        "disabled_at",
    ]

    df_delibera_templ = df_delibera_templ.select(sorted_cols)
    df_result = df_result.select(sorted_cols)
    df_result = pl.concat([df_delibera_templ, df_result], how="vertical_relaxed")
    download_attachments(ctx, df_result, chunk_size=500)

    ### LOAD ###
    load_data(ctx, df_result, "resolutions")
