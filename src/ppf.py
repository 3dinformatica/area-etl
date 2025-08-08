import concurrent.futures
import io
import logging
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
from tqdm import tqdm

from utils import (
    ETLContext,
    truncate_pg_table, extract_data, load_data,
)

PPF_TABLES = [
    "prescriptions",
    "prescription_pharmacy_data",
    "prescription_specializations",
    "prescription_specialties",
    "prescription_organigram_nodes",
    "prescription_hr_pers",
    "prescription_histories",
    "integration_requests",
    "integration_request_hr_pers",
    "prescr_hr_org_node_inbox",
    "prescr_hr_specialization_inbox",
    "specialization_hr_inbox"
]


def truncate_ppf_tables(ctx: ETLContext) -> None:
    """
    Truncate all the tables in the PostgreSQL database of A.Re.A. PPF service.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    logging.info(f"Truncating all target tables in PostgreSQL {ctx.pg_engine_ppf}...")

    for table in PPF_TABLES:
        truncate_pg_table(ctx.pg_engine_ppf, table)


def migrate_prescriptions(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.PRESCRIZIONE_MODEL" to PostgreSQL table "prescriptions".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescription = extract_data(ctx.oracle_engine_ppf, "SELECT * FROM AUAC_CENTR_PRESCR_USR.PRESCRIZIONE_MODEL")

    ### TRANSFORM ###
    df_result = df_prescription.select(
        pl.col("ID").alias("id"),
        pl.col("DESCR").str.strip_chars().alias("description"),
        pl.col("STATO_PRESCRIZIONE").str.strip_chars().alias("state"),
        pl.col("CRITERIO_PRESCRIZIONE").str.strip_chars().alias("criteria"),
        pl.col("INDICAZIONI_CLINICHE").str.strip_chars().alias("clinical_indications"),
        pl.col("NOTE").str.strip_chars().alias("annotation"),
        pl.col("VERSION").alias("version"),
        pl.col("ID_ATTO_FK").alias("resolution_id"),
        pl.col("ID_UTENTE_CREATOR_FK").alias("user_creator_id"),
        pl.col("ID_UTENTE_LAST_MOD_FK").alias("user_last_mod_id"),
        pl.col("CREATION")
        .fill_null(pl.col("LAST_MOD"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "prescriptions")


def migrate_prescription_pharmacy_data(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_FARMADATO" to PostgreSQL table "prescription_pharmacy_data".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescription_pharmacy_data = extract_data(ctx.oracle_engine_ppf,
                                                 "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_FARMADATO")

    ### TRANSFORM ###
    df_result = df_prescription_pharmacy_data.select(
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_FARMADATO_FK").alias("pharmacy_data_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "prescription_pharmacy_data")


def migrate_prescription_specializations(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_SPEC" to PostgreSQL table "prescription_specializations".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescription_specializations = extract_data(ctx.oracle_engine_ppf,
                                                   "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_SPEC")

    ### TRANSFORM ###
    df_result = df_prescription_specializations.select(
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_SPEC_FK").alias("specialization_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "prescription_specializations")


def migrate_prescription_specialties(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_SPECIALITA" to PostgreSQL table "prescription_specialties".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescription_specialties = extract_data(ctx.oracle_engine_ppf,
                                               "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_SPECIALITA")

    ### TRANSFORM ###
    df_result = df_prescription_specialties.select(
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_DISCIPLINA_FK").alias("specialty_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "prescription_specialties")


def migrate_prescription_organigram_nodes(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_UO" to PostgreSQL table "prescription_organigram_nodes".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescription_organigram_nodes = extract_data(ctx.oracle_engine_ppf,
                                                    "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_UO")

    ### TRANSFORM ###
    df_result = df_prescription_organigram_nodes.select(
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_UO_FK").alias("node_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "prescription_organigram_nodes")


def migrate_prescription_hr_pers(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_HR_PERS" to PostgreSQL table "prescription_hr_pers".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescription_hr_pers = extract_data(ctx.oracle_engine_ppf,
                                           "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_HR_PERS")
    df_prescription_hr_pers_specialization = extract_data(ctx.oracle_engine_ppf,
                                                          "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_HR_PERS_SPEC")
    df_prescription_hr_pers_uo = extract_data(ctx.oracle_engine_ppf,
                                              "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESCRIZIONI_HR_PERS_UO")

    group_type = ["SINGLE", "SPECIALIZATION", "ORG_NODE"]

    ### TRANSFORM ###
    df_prescription_hr_pers = df_prescription_hr_pers.select(
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_HR_PERS_FK").alias("hr_pers_id"),
    ).with_columns([
        pl.lit(group_type[0]).alias("group_type")
    ])

    df_prescription_hr_pers_specialization = df_prescription_hr_pers_specialization.select(
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_HR_PERS_FK").alias("hr_pers_id"),
    ).with_columns([
        pl.lit(group_type[1]).alias("group_type")
    ])

    df_prescription_hr_pers_uo = df_prescription_hr_pers_uo.select(
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_HR_PERS_FK").alias("hr_pers_id"),
    ).with_columns([
        pl.lit(group_type[2]).alias("group_type")
    ])

    df_combined = pl.concat(
        [df_prescription_hr_pers, df_prescription_hr_pers_specialization, df_prescription_hr_pers_uo])

    df_combined = df_combined.unique()

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_combined, "prescription_hr_pers")


def migrate_prescription_histories(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.STORICO_PRESCRIZIONE_MODEL" to PostgreSQL table "prescription_histories".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescription_histories = extract_data(ctx.oracle_engine_ppf,
                                             "SELECT * FROM AUAC_CENTR_PRESCR_USR.STORICO_PRESCRIZIONE_MODEL")

    ### TRANSFORM ###
    df_result = df_prescription_histories.select(
        pl.col("ID").alias("id"),
        pl.col("EVENTO").str.strip_chars().alias("event"),
        pl.col("VERSION").fill_null(0).cast(pl.Int64).alias("version"),
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_UTENTE_CREATOR_FK").alias("user_creator_id"),
        pl.col("ID_UTENTE_LAST_MOD_FK").alias("user_last_mod_id"),
        pl.col("CREATION")
        .fill_null(pl.col("LAST_MOD"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "prescription_histories")


def migrate_integration_requests(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.RICHIESTE_INTEGRAZIONE" to PostgreSQL table "integration_requests".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_integration_requests = extract_data(ctx.oracle_engine_ppf,
                                           "SELECT * FROM AUAC_CENTR_PRESCR_USR.RICHIESTE_INTEGRAZIONE")

    ### TRANSFORM ###
    df_result = df_integration_requests.select(
        pl.col("ID").alias("id"),
        pl.col("AUTORIZZATA").cast(pl.Boolean).alias("is_autorized"),
        pl.col("SCARTATA").cast(pl.Boolean).alias("is_rejected"),
        pl.col("NOTE").str.strip_chars().alias("annotations"),
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_UTENTE_CREATOR_FK").alias("user_creator_id"),
        pl.col("ID_UTENTE_LAST_MOD_FK").alias("user_last_mod_id"),
        pl.col("CREATION")
        .fill_null(pl.col("LAST_MOD"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "integration_requests")


def migrate_integration_request_hr_pers(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_INTEGRAZIONI_HR_PERS" to PostgreSQL table "integration_request_hr_pers".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_integration_request_hr_pers = extract_data(ctx.oracle_engine_ppf,
                                                  "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_INTEGRAZIONI_HR_PERS")

    ### TRANSFORM ###
    df_result = df_integration_request_hr_pers.select(
        pl.col("ID_INTEGRAZIONE_FK").alias("integration_request_id"),
        pl.col("ID_HR_PERS_FK").alias("hr_pers_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "integration_request_hr_pers")


def migrate_prescr_hr_org_node_inbox(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_PRESC_PERS_VASCHETTA" to PostgreSQL table "prescr_hr_org_node_inbox".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescr_hr_org_node_inbox = extract_data(ctx.oracle_engine_ppf,
                                               "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESC_PERS_VASCHETTA")

    ### TRANSFORM ###
    df_result = df_prescr_hr_org_node_inbox.select(
        pl.col("ID").alias("id"),
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_UO_FK").alias("organigram_node_id"),
        pl.col("ID_HR_PERS_FK").alias("hr_pers_id"),
        pl.col("FLAG_AUT_AZ").cast(pl.Boolean).alias("is_authorized_company"),
        pl.col("FLAG_AUT_REG").cast(pl.Boolean).alias("is_authorized_regionally"),
        pl.col("IGNORATO").cast(pl.Boolean).alias("ignored"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "prescr_hr_org_node_inbox")


def migrate_prescr_hr_specialization_inbox(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_PRESC_PERS_SPEC_VASCHETTA" to PostgreSQL table "prescr_hr_specialization_inbox".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_prescr_hr_specialization_inbox = extract_data(ctx.oracle_engine_ppf,
                                                     "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_PRESC_PERS_SPEC_VASCHETTA")

    ### TRANSFORM ###
    df_result = df_prescr_hr_specialization_inbox.select(
        pl.col("ID").alias("id"),
        pl.col("ID_PRESCRIZIONE_FK").alias("prescription_id"),
        pl.col("ID_HR_PERS_FK").alias("hr_pers_id"),
        pl.col("FLAG_AUT_AZ").cast(pl.Boolean).alias("is_authorized_company"),
        pl.col("FLAG_AUT_REG").cast(pl.Boolean).alias("is_authorized_regionally"),
        pl.col("IGNORATO").cast(pl.Boolean).alias("is_ignored"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "prescr_hr_specialization_inbox")


def migrate_specialization_hr_inbox(ctx: ETLContext) -> None:
    """
    Migrate toponyms from ORACLE table "AUAC_CENTR_PRESCR_USR.BIND_SPEC_PERSONA_VASCH" to PostgreSQL table "specialization_hr_inbox".

    Parameters
    ----------
    ctx: ETLContext
        The ETL context containing database connections
    """
    ### EXTRACT ###
    df_specialization_hr_inbox = extract_data(ctx.oracle_engine_ppf,
                                              "SELECT * FROM AUAC_CENTR_PRESCR_USR.BIND_SPEC_PERSONA_VASCH")

    ### TRANSFORM ###
    df_result = df_specialization_hr_inbox.select(
        pl.col("ID_BIND_PRESCR").alias("prescr_hr_specialization_inbox_id"),
        pl.col("SPECIALIZZAZIONE_ID").alias("specialization_id"),
    )

    ### LOAD ###
    load_data(ctx.pg_engine_ppf, df_result, "specialization_hr_inbox")


def migrate_resolutions(ctx: ETLContext, bucket_name: str = "area-resolutions") -> None:
    """
   Migrate act from the Oracle table "AUAC_CENTR_PRESCR_USR.ATTO_MODEL" to PostgreSQL.

   This function extracts act data from Oracle, transforms it, uploads attachments
   to MinIO, and loads the data into PostgreSQL.

   The file upload to MinIO is optimized for performance using parallel processing
   with ThreadPoolExecutor, which allows multiple files to be uploaded concurrently.
   Additional optimizations include:
   - Increased part_size for better throughput
   - Optimized Minio client configuration
   - Real-time progress tracking using tqdm
   - Performance metrics logging
   - Reliable file_id updates using a mapping dataframe approach
     (creates a mapping between original BINARY_ATTACHMENTS_CLIENT_ID and new MinIO object_id)
   - Deduplication of files: If multiple rows reference the same file (same ID_ALLEGATO_FK),
     the file is uploaded only once to MinIO, and all rows are updated to reference
     the same MinIO object_id

   Parameters
   ----------
   ctx : ETLContext
       The ETL context containing database connections
   bucket_name : str, default="area-resolutions"
       Name of the MinIO bucket to store attachments
   """
    ### EXTRACT ###
    df_resolution = extract_data(ctx.oracle_engine_ppf,
                                 "SELECT * FROM AUAC_CENTR_PRESCR_USR.ATTO_MODEL")
    df_attaches = extract_data(ctx.oracle_engine_ppf,
                               "SELECT * FROM AUAC_CENTR_PRESCR_USR.ATTACHES")
    df_resolution_types = extract_data(ctx.pg_engine_core, "SELECT * FROM resolution_types")
    df_resolution_pg = extract_data(ctx.pg_engine_core, "SELECT * FROM resolutions")
    new_resolution_types_id = None

    ### TRANSFORM ###
    # Filter out resolution_types that already exist in PostgreSQL
    res_type_existing = df_resolution_types.filter(
        pl.col("name") == "PRESCRIPTION_AUTORIZATION"
    )

    if res_type_existing.height > 0:
        ids = [str(x) for x in res_type_existing.get_column("id").to_list()]
        new_resolution_types_id = ids[0]  # scalare stringa
    else:
        new_resolution_types_id = str(uuid.uuid4())
        now = datetime.now(ZoneInfo("Europe/Rome"))

        new_record_df = pl.DataFrame([
            {
                "id": new_resolution_types_id,
                "name": "PRESCRIPTION_AUTORIZATION",
                "disabled_at": None,
                "created_at": now,
                "updated_at": now,
            }
        ])

        new_record_df = new_record_df.with_columns(
            pl.col("id").map_elements(str, return_dtype=pl.Utf8),
            pl.col("name").cast(pl.Utf8),
            pl.col("disabled_at").cast(pl.Datetime("us", "Europe/Rome")).fill_null(None),
            pl.col("created_at").cast(pl.Datetime("us", "Europe/Rome")),
            pl.col("updated_at").cast(pl.Datetime("us", "Europe/Rome")),
        )

        df_resolution_types = pl.concat([df_resolution_types, new_record_df], how="vertical_relaxed")

    # Filter out resolutions that already exist in PostgreSQL
    ids_pg_set = set(map(str, df_resolution_pg.get_column("id").to_list()))
    id_oracle = df_resolution.get_column("ID")
    if id_oracle.dtype == pl.Object:
        df_resolution = df_resolution.with_columns(
            pl.Series("ID_STR", map(str, id_oracle.to_list()), dtype=pl.Utf8)
        )
    else:
        df_resolution = df_resolution.with_columns(
            pl.col("ID").cast(pl.Utf8).alias("ID_STR")
        )

    df_resolution = df_resolution.filter(~pl.col("ID_STR").is_in(ids_pg_set)).drop("ID_STR")

    if df_resolution.height == 0:
        logging.info("No new resolutions to process, skipping migration.")
        return

    df_attaches = df_attaches.select(pl.col("ID").alias("ID_ALLEGATO_FK"),
                                     pl.col("FILE_NAME"),
                                     pl.col("MIME_TYPE"),
                                     pl.col("ID_ATTACH_CONTENT"))

    df_resolution = df_resolution.select(
        pl.col("ID").alias("id"),
        pl.col("ID_ALLEGATO_FK"),
        pl.col("DATA_INIZIO").alias("valid_from"),
        pl.col("DATA_FINE").alias("valid_to"),
        pl.col("DESCR").alias("name"),
        pl.col("CREATION")
        .fill_null(pl.col("LAST_MOD"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("created_at"),
        pl.col("LAST_MOD")
        .fill_null(pl.col("CREATION"))
        .dt.replace_time_zone("Europe/Rome", ambiguous="earliest")
        .dt.replace_time_zone(None)
        .alias("updated_at"),
    ).with_columns(
        pl.lit(new_resolution_types_id).alias("resolution_type_id"),
        pl.lit("PPF").alias("category"))

    df_join = df_resolution.join(df_attaches, on="ID_ALLEGATO_FK", how="left")

    df_result_with_files = df_join.filter(pl.col("ID_ALLEGATO_FK").is_not_null())
    df_result_without_files = df_join.filter(pl.col("ID_ALLEGATO_FK").is_null())
    logging.info(f"There are {df_result_with_files.height}/{df_join.height} files with attachments")

    # Use the MinIO client from the ETLContext
    found = ctx.minio_client.bucket_exists(bucket_name)

    if not found:
        ctx.minio_client.make_bucket(bucket_name)
        logging.info(f'Created MinIO bucket "{bucket_name}"')
    else:
        logging.info(f'MinIO bucket "{bucket_name}" already exists')

    # Define a function to process a single file
    def process_file(row_data):
        original_file_id = row_data["ID_ALLEGATO_FK"]

        try:
            binary_attachments_appl_row = pl.read_database(
                f"""SELECT binAtt.CONTENT, 
                           attaches.ID_ATTACH_CONTENT as ID_ALLEGATO_FK, 
                           attaches.MIME_TYPE, 
                           attaches.FILE_NAME 
                    FROM ATTACH_CONTENT binAtt
                    LEFT JOIN ATTACHES attaches ON attaches.ID_ATTACH_CONTENT = binAtt.ID
                    WHERE attaches.ID = '{original_file_id}'""",
                ctx.oracle_engine_ppf,
            )
            file_name = binary_attachments_appl_row.item(row=0, column="FILE_NAME")
            file_mime_type = binary_attachments_appl_row.item(row=0, column="MIME_TYPE")
            file_bytes = binary_attachments_appl_row.item(row=0, column="CONTENT")
            cleaned_file_name = file_name.replace("/", "_").replace("\\", "_").encode("ascii", "ignore").decode(
                "ascii")
            object_name = str(uuid.uuid4())
            content_type = MIME_TYPES_MAPPING.get(str(file_mime_type).strip(), "application/octet-stream")
            # Optimize part_size for better performance
            # Using a larger part_size can improve upload speed for large files
            # Default is 5MB, we're using 16MB for better throughput
            ctx.minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=io.BytesIO(file_bytes),
                length=-1,
                part_size=16 * 1024 * 1024,
                content_type=content_type,
                metadata={"name": cleaned_file_name},
            )
            # Return the original file_id and the new object_name
            return original_file_id, object_name
        except Exception as e:
            logging.error(f"Error processing file {original_file_id}: {e!s}")
            return original_file_id, None

    # Extract unique ID_ALLEGATO_FK values to avoid uploading duplicate files
    unique_file_ids = df_result_with_files.select("ID_ALLEGATO_FK").unique().to_series().to_list()
    total_unique_files = len(unique_file_ids)
    total_files = df_result_with_files.height

    logging.info(f"Found {total_unique_files} unique files out of {total_files} total attachments")

    # Create a list to store mapping between original file_ids and new object_ids
    file_id_mappings = []

    logging.info(f"Starting parallel upload of {total_unique_files} unique files to MinIO")
    start_time = time.time()

    # Process files in parallel with a ThreadPoolExecutor
    # Using 10 workers for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Create a list of dictionaries with just the ID_ALLEGATO_FK for each unique file
        unique_file_data = [{"ID_ALLEGATO_FK": file_id} for file_id in unique_file_ids]

        # Submit tasks only for unique files
        future_to_file_id = {
            executor.submit(process_file, file_data): file_data["ID_ALLEGATO_FK"] for file_data in unique_file_data
        }

        # Use tqdm for progress tracking
        with tqdm(total=total_unique_files, desc="Uploading unique files to MinIO", unit="file") as pbar:
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_file_id):
                original_file_id, object_name = future.result()
                if object_name:
                    # Store the mapping between original file_id and new object_id
                    file_id_mappings.append(
                        {"BINARY_ATTACHMENTS_CLIENT_ID": original_file_id, "file_id": object_name})

                # Update progress bar
                pbar.update(1)

    end_time = time.time()
    duration = end_time - start_time
    files_per_second = total_unique_files / duration if duration > 0 else 0
    logging.info(
        f"Completed upload of {len(file_id_mappings)}/{total_unique_files} unique files in {duration:.2f} seconds ({files_per_second:.2f} files/sec)"
    )

    # Log information about deduplication
    duplicates_saved = total_files - total_unique_files
    if duplicates_saved > 0:
        logging.info(f"Avoided uploading {duplicates_saved} duplicate files to MinIO")

    # Create a mapping dataframe with BINARY_ATTACHMENTS_CLIENT_ID and file_id columns
    df_file_id_mappings = pl.DataFrame(file_id_mappings)

    # Join the mapping dataframe with the original dataframe based on ID_ALLEGATO_FK
    df_result_with_files_minio = df_result_with_files.join(
        df_file_id_mappings, left_on="ID_ALLEGATO_FK", right_on="BINARY_ATTACHMENTS_CLIENT_ID", how="left"
    )

    # Verify that all rows have a file_id
    assert df_result_with_files_minio.filter(pl.col("file_id").is_not_null()).height == df_result_with_files.height

    df_result_with_files_minio = df_result_with_files_minio.drop("ID_ALLEGATO_FK")
    df_result_without_files = df_result_without_files.drop("ID_ALLEGATO_FK")

    df_join = pl.concat(
        [df_result_with_files_minio, df_result_without_files],
        how="diagonal_relaxed",
    )

    # Handle duplicate names by appending sequential numbers in parentheses
    # When the "name" column contains duplicates, append a sequential number in parentheses
    # to make each name unique. For example: "name", "name (1)", "name (2)", etc.
    # The first occurrence of a name remains unchanged.

    # Create a dictionary to track name occurrences
    name_counts = {}

    # Function to handle duplicate names
    def handle_duplicate_name(name):
        if name is None:
            return None

        if name in name_counts:
            name_counts[name] += 1
            return f"{name} ({name_counts[name]})"
        else:
            name_counts[name] = 0
            return name

    # Apply the function to handle duplicate names
    df_join = df_join.with_columns(pl.col("name").map_elements(handle_duplicate_name, return_dtype=pl.String))

    df_join = df_join.drop("FILE_NAME", "MIME_TYPE", "ID_ATTACH_CONTENT")

    ### LOAD ###
    if res_type_existing.height < 0:
        load_data(ctx.pg_engine_core, df_resolution_types, "resolution_types")
    load_data(ctx.pg_engine_core, df_join, "resolutions")


MIME_TYPES_MAPPING = {
    "PDF": "application/pdf",
    "xml": "application/xml",
}


### ALL ###
def migrate_ppf(ctx: ETLContext) -> None:
    """
    Migrate data from source databases to the PPF service database.

    This function orchestrates the complete ETL process for the PPF service,
    first truncating all target tables and then migrating each entity type
    in the correct sequence.

    Parameters
    ----------
    ctx : ETLContext
        The ETL context containing database connections
    """
    truncate_ppf_tables(ctx)
    migrate_prescriptions(ctx)
    migrate_prescription_pharmacy_data(ctx)
    migrate_prescription_specializations(ctx)
    migrate_prescription_specialties(ctx)
    migrate_prescription_organigram_nodes(ctx)
    migrate_prescription_hr_pers(ctx)
    migrate_prescription_histories(ctx)
    migrate_integration_requests(ctx)
    migrate_integration_request_hr_pers(ctx)
    migrate_prescr_hr_org_node_inbox(ctx)
    migrate_prescr_hr_specialization_inbox(ctx)
    migrate_specialization_hr_inbox(ctx)
    migrate_resolutions(ctx)
