from IPython.utils.PyColorize import pl

from core import ETLContext, extract_data, load_data, check_table_exists, is_table_empty


def migrate_cronos_taxonomies(ctx: ETLContext) -> None:
    """
        Migrate cronos taxonomies from Oracle to PostgreSQL
        Args:
            ctx: The ETL context containing database connections
    """
    ### CHECK ###
    if (not check_table_exists(ctx.oracle_engine, "CLASSIFICAZIONE_PROGRAMMAZIONE", "AUAC_USR")
            or is_table_empty(ctx.oracle_engine, "CLASSIFICAZIONE_PROGRAMMAZIONE", "AUAC_USR")):
        return

    ### EXTRACT ###
    df = extract_data(ctx, f"SELECT clientid, nome FROM AUAC_USR.CLASSIFICAZIONE_PROGRAMMAZIONE")

    ### TRANSFORM ###
    df = df.select([
        pl.col("clientid").alias("id"),
        pl.col("nome").str.strip().alias("name")
    ])
    df = (
        df.select([
            pl.col("clientid").alias("id"),
            pl.col("nome").str.strip().alias("name")
        ])
    )
    ### LOAD ###
    load_data(ctx, df, "cronos_taxonomies")


def migrate_dm70_taxonomies(ctx: ETLContext) -> None:
    """
        Migrate dm70_taxonomies from Oracle to PostgreSQ
        Args:
            ctx: The ETL context containing database connections
    """

    ### CHECK ###
    if not check_table_exists(ctx.oracle_engine, "CLASSIFICAZIONE_DM_70", "AUAC_USR") or is_table_empty(
            ctx.oracle_engine, "CLASSIFICAZIONE_DM_70", "AUAC_USR"):
        return

    ### EXTRACT ###
    df = extract_data(ctx, f"SELECT clientid, nome FROM AUAC_USR.CLASSIFICAZIONE_DM_70")

    ### TRANSFORM ###
    df = df.select([
        pl.col("clientid").alias("id"),
        pl.col("nome").str.strip().alias("name")
    ])
    ### LOAD ###
    load_data(ctx, df, "dm70_taxonomies")


def migrate_cronos_plan_grouping_specialties(ctx: ETLContext) -> None:
    """
        Migrate migrate_cronos_plan_grouping_specialties from Oracle to PostgreSQ
        Args:
            ctx: The ETL context containing database connections
    """

    ### CHECK ###
    if not check_table_exists(ctx.oracle_engine, "PROGRAMMAZIONE_AREE_DISC", "AUAC_USR") or is_table_empty(
            ctx.oracle_engine, "PROGRAMMAZIONE_AREE_DISC", "AUAC_USR"):
        return

    ### EXTRACT ###
    df = extract_data(ctx, f"SELECT clientid, nome FROM AUAC_USR.PROGRAMMAZIONE_AREE_DISC")

    ### TRANSFORM ###
    df = df.select([
        pl.col("clientid").alias("id"),
        pl.col("id_programmazione_fk").alias("cronos_plan_id"),
        pl.col("id_area_discipl_fk").alias("grouping_specialty_id"),
        pl.col("totale_plexr").alias("num_beds_extra_reg")
    ])
    ### LOAD ###
    load_data(ctx, df, "cronos_plan_grouping_specialties")


def migrate_cronos_physical_structures(ctx: ETLContext) -> None:
    """
        Migrate migrate_cronos_physical_structures from Oracle to PostgreSQ
        Args:
            ctx: The ETL context containing database connections
    """
    ### CHECK ###
    if not check_table_exists(ctx.oracle_engine, "OSPEDALE_PROGR", "AUAC_USR") or is_table_empty(ctx.oracle_engine,
                                                                                                 "OSPEDALE_PROGR",
                                                                                                 "AUAC_USR"):
        return
    ### EXTRACT ###
    df = extract_data(ctx,
                      f"SELECT clientid, nome, codice_hsp, id_titolare_fk, id_classif_dm_70, id_asl_nuove_comune_fk, id_struttura_model_fk FROM AUAC_USR.OSPEDALE_PROGR")
    muni = extract_data(ctx, f"SELECT id, istat_code FROM public.municipalities")
    comp = extract_data(ctx, f"SELECT id, area_company_id FROM public.cronos_companies")
    ### TRANSFORM ###
    df = df.with_columns([
        pl.col("clientid").alias("id"),
        pl.col("nome").str.strip().alias("name"),
        pl.col("codice_hsp").alias("hsp_code"),
        pl.col("id_classif_dm_70").alias("dm70_taxonomy_id"),
        pl.col("id_struttura_model_fk").alias("area_physical_structure_id"),
        pl.col("id_asl_nuove_comune_fk").map_dict(dict(zip(muni["istat_code"].to_list(), muni["id"].to_list()))).alias(
            "municipality_id"),
        pl.col("id_titolare_fk").str.upper().fill_null("").map_dict(
            dict(zip(comp["area_company_id"].to_list(), comp["id"].to_list()))).alias("cronos_company_id")
    ]).select(["id", "name", "hsp_code", "cronos_company_id", "dm70_taxonomy_id", "municipality_id",
               "area_physical_structure_id"])
    ### LOAD ###
    load_data(ctx, df, "cronos_physical_structures")


def migrate_healthcare_companies(ctx: ETLContext) -> None:
    """
        Migrate migrate_healthcare_companies from Oracle to PostgreSQ
        Args:
            ctx: The ETL context containing database connections
      """
    ### CHECK ###
    if not check_table_exists(ctx.oracle_engine, "AZIENDA_SANITARIA", "AUAC_USR") or is_table_empty(ctx.oracle_engine,
                                                                                                    "AZIENDA_SANITARIA",
                                                                                                    "AUAC_USR"):
        return
    ### EXTRACT ###
    df = extract_data(ctx, f"SELECT clientid, codice, descrizione FROM AUAC_USR.AZIENDA_SANITARIA")
    ulss = extract_data(ctx, f"SELECT id, code FROM public.ulss")
    ### TRANSFORM ###
    df = df.with_columns([
        pl.col("clientid").alias("id"),
        pl.col("descrizione").str.strip().alias("name"),
        pl.col("codice").map_dict(dict(zip(ulss["code"].to_list(), ulss["id"].to_list()))).alias("code")
    ]).select(["id", "code", "name"])
    ### LOAD ###
    load_data(ctx, df, "healthcare_companies")


def migrate_cronos_plan_specialty_aliases(ctx: ETLContext) -> None:
    """
        Migrate migrate_cronos_plan_specialty_aliases from Oracle to PostgreSQ
        Args:
            ctx: The ETL context containing database connections
    """

    ### CHECK ###
    if not check_table_exists(ctx.oracle_engine, "ALIAS_PROG_DISC", "AUAC_USR") or is_table_empty(
            ctx.oracle_engine, "ALIAS_PROG_DISC", "AUAC_USR"):
        return

    ### EXTRACT ###
    df = extract_data(ctx, f"SELECT clientid, nome FROM AUAC_USR.ALIAS_PROG_DISC")

    ### TRANSFORM ###
    df = df.select([
        pl.col("clientid").alias("id"),
        pl.col("nome").str.strip().alias("name"),
    ])
    ### LOAD ###
    load_data(ctx, df, "cronos_plan_specialty_aliases")


def migrate_cronos_companies(ctx: ETLContext) -> None:
    """
         Migrate migrate_cronos_companies from Oracle to PostgreSQ
         Args:
             ctx: The ETL context containing database connections
     """
    ### CHECK ###
    if not check_table_exists(ctx.oracle_engine, "PROGRAMMAZIONE", "AUAC_USR") or is_table_empty(ctx.oracle_engine,
                                                                                                 "PROGRAMMAZIONE",
                                                                                                 "AUAC_USR"):
        return
    ### EXTRACT ###
    df = extract_data(ctx, f"SELECT * FROM AUAC_USR.PROGRAMMAZIONE")
    comp = extract_data(ctx, f"SELECT id, area_company_id FROM public.cronos_companies")
    ulss = extract_data(ctx, f"SELECT id, code FROM public.ulss")
    ### TRANSFORM ###
    df = df.with_columns([
        pl.col("amb").cast(pl.Boolean).alias("is_ambulatory"),
        pl.coalesce([pl.col("id_disciplina_fk"), pl.col("id_branca_fk")]).alias("specialty_id"),
        pl.col("id_ulss_terr_fk").map_dict(dict(zip(ulss["code"].to_list(), ulss["id"].to_list()))).alias(
            "id_ulss_terr_fk"),
        pl.col("id_titolare_fk").map_dict(dict(zip(comp["area_company_id"].to_list(), comp["id"].to_list()))).alias(
            "id_titolare_fk")
    ]).rename({"clientid": "id", "id_programmazione_fk": "cronos_plan_id"})
    ### LOAD ###
    load_data(ctx, df, "cronos_companies")


def migrate_cronos_plan_specialties(ctx: ETLContext) -> None:
    """
          Migrate migrate_cronos_plan_specialties from Oracle to PostgreSQ
          Args:
              ctx: The ETL context containing database connections
      """
    ### CHECK ###
    if not check_table_exists(ctx.oracle_engine, "PROGRAMMAZIONE_DISC", "AUAC_USR") or is_table_empty(ctx.oracle_engine,
                                                                                                      "PROGRAMMAZIONE_DISC",
                                                                                                      "AUAC_USR"):
        return
    ### EXTRACT ###
    df = extract_data(ctx, f"SELECT * FROM AUAC_USR.PROGRAMMAZIONE_DISC")
    ### TRANSFORM ###
    df = (
        df.with_columns([
            pl.col("note").str.strip().alias("note"),
            pl.col("attivita").str.strip().alias("activities"),
            pl.col("amb").cast(pl.Boolean).alias("is_ambulatory"),
            pl.coalesce([pl.col("id_disciplina_fk"), pl.col("id_branca_fk")]).alias("specialty_id")
        ])
        .with_columns([
            pl.col("clientid").alias("id"),
            pl.col("id_programmazione_fk").alias("cronos_plan_id"),
            pl.col("apicalita").alias("num_uoc"),
            pl.col("canc_datetime").alias("deletion_resolution_insert_dt"),
            pl.col("canc_id_utente_fk").alias("deletion_resolution_insert_user_id"),
            pl.col("codice_univoco").alias("code"),
            pl.col("fine_validita").alias("validity_end_dt"),
            pl.col("id_alias_prog_disc_fk").alias("cronos_plan_specialty_alias_id"),
            pl.col("id_delibera_templ_canc").alias("deletion_resolution_id"),
            pl.col("id_delibera_templ_ins_val").alias("validity_resolution_id"),
            pl.col("inizio_validita").alias("validity_start_dt"),
            pl.col("ins_val_datetime").alias("validity_resolution_insert_dt"),
            pl.col("ins_val_id_utente_fk").alias("validity_resolution_insert_user_id"),
            pl.col("n_uos").alias("num_uos"),
            pl.col("posti_letto_regione").alias("num_beds_regional"),
            pl.col("n_uosd").alias("num_uosd"),
            pl.col("id_progr_disc_padre_fk").alias("parent_id"),
            pl.col("id_sub_disciplina_fk").alias("sub_specialty_id"),
            pl.col("posti_letto_dialisi").alias("num_beds_dialysis"),
            pl.col("posti_letto_obi").alias("num_beds_mortuary")
        ])
        .drop(["id_branca_fk", "id_disciplina_fk"])
        .sort(by=["parent_id"], nulls_last=False)
    )
    ### LOAD ###
    load_data(ctx, df, "cronos_plan_specialties")


def migrate_cronos_plans(ctx: ETLContext) -> None:
    """
          Migrate migrate_cronos_plans from Oracle to PostgreSQ
          Args:
              ctx: The ETL context containing database connections
      """
    ### CHECK ###
    if not check_table_exists(ctx.oracle_engine, "PROGRAMMAZIONE", "AUAC_USR") or is_table_empty(ctx.oracle_engine,
                                                                                                 "PROGRAMMAZIONE",
                                                                                                 "AUAC_USR"):
        return
    ### EXTRACT ###
    df = extract_data(ctx, f"SELECT * FROM AUAC_USR.PROGRAMMAZIONE")
    macro = extract_data(ctx, f"SELECT clientid, nome FROM AUAC_USR.MACROAREA_PROGRAMMAZIONE",
                         source="oracle", return_type="polars")
    amb = extract_data(ctx, f"SELECT clientid, nome FROM AUAC_USR.AMBITO_PROGRAMMAZIONE")
    ulss = extract_data(ctx, f"SELECT id, code FROM public.ulss")
    comp = extract_data(ctx, f"SELECT id, area_company_id FROM public.cronos_companies")
    ### TRANSFORM ###
    df = (
        df.with_columns([
            pl.col("salute_mentale").cast(pl.Boolean).alias("is_mental_health"),
            pl.col("rapporto_ssn").alias("ssn_relationship"),
            pl.col("stato_programmazione_enum").str.strip().alias("cronos_status"),
            pl.col("tipo_flusso").str.strip().alias("cronos_flow_type"),
            pl.col("tipologia").str.strip().alias("cronos_category"),
            pl.col("fine_validita").alias("validity_end_dt"),
            pl.col("id_delibera_cancellazione").alias("deletion_resolution_id"),
            pl.col("id_delibera_templ_ins_val").alias("validity_resolution_id"),
            pl.col("nota_organizzativa").str.strip().alias("org_note"),
            pl.col("nota_ospedale").str.strip().alias("osp_note"),
            pl.col("id_azienda_san_fk").alias("healthcare_company_id"),
            pl.col("id_osped_prog_fk").alias("cronos_physical_structure_id"),
            pl.col("id_class_prog_fk").alias("cronos_taxonomy_id"),
            pl.col("last_mod_datetime").alias("updated_at")
        ])
        .with_columns([
            pl.col("clientid").alias("id"),
            pl.col("id_ulss_terr_fk").map_dict(dict(zip(ulss["code"].to_list(), ulss["id"].to_list()))).alias(
                "ulss_id"),
            pl.col("id_titolare_fk").str.upper().fill_null("")
            .map_dict(dict(zip(comp["area_company_id"].to_list(), comp["id"].to_list())))
            .alias("cronos_company_id"),
            pl.col("id_macro_ar_prog_fk").map_dict(dict(
                zip(macro["clientid"].to_list(), macro["nome"].str.upper().str.replace(" ", "_").to_list()))).alias(
                "macro_area"),
            pl.col("id_ambito_prog_fk").map_dict(
                dict(zip(amb["clientid"].to_list(), amb["nome"].str.upper().str.replace(" ", "_").to_list()))).alias(
                "cronos_scope")
        ])
    )
    ### LOAD ###
    load_data(ctx, df, "cronos_plans")
