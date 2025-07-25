import argparse
import logging
from datetime import datetime

from auac import migrate_requirement_taxonomies, migrate_requirement_lists, migrate_requirementlist_requirements, \
    migrate_requirements, migrate_procedure_type_requirement_list_classification_mental, \
    migrate_procedure_type_requirement_list_comp_type_comp_class, migrate_procedure_type_requirement_list_udo_type, \
    migrate_procedure_type_requirement_list_for_physical_structures, migrate_udo_inst_in_procedure_entities, \
    migrate_uo_inst_in_procedure_entities, migrate_struttura_inst_in_procedure_entities, \
    migrate_edificio_inst_in_procedure_entities, migrate_comprensorio_inst_in_procedure_entities, \
    migrate_procedure_entity_requirements, migrate_procedures
from core import (
    export_tables_to_csv,
    setup_connections,
    setup_logging,
    truncate_postgresql_tables, create_tables,
)
from cronos import migrate_cronos_taxonomies, migrate_dm70_taxonomies, migrate_cronos_plan_grouping_specialties, \
    migrate_cronos_physical_structures, migrate_healthcare_companies, migrate_cronos_plan_specialty_aliases, \
    migrate_cronos_companies, migrate_cronos_plan_specialties, migrate_cronos_plans
from orgchart import migrate_areas_sub_areas, migrate_parameter, migrate_regulation, migrate_organigram_attachments, \
    migrate_function_diagram_nodes, migrate_function_diagrams, migrate_rules, migrate_rule_types, migrate_node, \
    migrate_node_types, migrate_notifications, migrate_organigram_emails, migrate_models, migrate_organigrams, \
    migrate_legal_inquiries_types_legal_inquiries


def parse_args():
    parser = argparse.ArgumentParser(description="A.Re.A. ETL process")
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export all PostgreSQL tables to CSV files",
    )
    parser.add_argument(
        "--export-dir",
        type=str,
        default="export",
        help="Directory where CSV files will be saved (default: 'export')",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging()
    start_time = datetime.now()

    try:
        ctx = setup_connections()

        if args.export_csv:
            logging.info("Starting CSV export process...")
            export_tables_to_csv(ctx, args.export_dir)
            end_time = datetime.now()
            elapsed_time = end_time - start_time
            hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            logging.info(f"Total export time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")
            logging.info("CSV export completed successfully")
            return

        # Regular ETL process
        logging.info("Starting A.Re.A. ETL process...")
        # truncate_postgresql_tables(ctx)
        # migrate_regions(ctx)
        # migrate_provinces(ctx)
        # migrate_municipalities(ctx)
        # migrate_toponyms(ctx)
        # migrate_districts(ctx)
        # migrate_resolution_types(ctx)
        # migrate_resolutions(ctx)
        # migrate_company_types(ctx)
        # migrate_companies(ctx)
        # migrate_physical_structures(ctx)
        # migrate_operational_offices(ctx)
        # migrate_buildings(ctx)
        # migrate_grouping_specialties(ctx)
        # migrate_specialties(ctx)
        # migrate_production_factor_types(ctx)
        # migrate_production_factors(ctx)
        # migrate_udo_types(ctx)
        # migrate_operational_units(ctx)
        # migrate_udos(ctx)
        # migrate_udo_production_factors(ctx)
        # migrate_udo_type_production_factor_types(ctx)
        # migrate_udo_specialties_from_branches(ctx)
        # migrate_udo_specialties_from_disciplines(ctx)
        # migrate_udo_resolutions(ctx)
        # migrate_udo_status_history(ctx)

        # create_tables("sql/init_auac.sql", ctx.pg_engine, "AUAC")
        # migrate_requirement_taxonomies(ctx)
        migrate_requirements(ctx)
        migrate_requirement_lists(ctx)
        migrate_requirementlist_requirements(ctx)
        migrate_procedure_type_requirement_list_classification_mental(ctx)
        migrate_procedure_type_requirement_list_comp_type_comp_class(ctx)
        migrate_procedure_type_requirement_list_udo_type(ctx)
        migrate_procedure_type_requirement_list_for_physical_structures(ctx)
        migrate_procedures(ctx)
        migrate_udo_inst_in_procedure_entities(ctx)
        migrate_uo_inst_in_procedure_entities(ctx)
        migrate_struttura_inst_in_procedure_entities(ctx)
        migrate_edificio_inst_in_procedure_entities(ctx)
        migrate_comprensorio_inst_in_procedure_entities(ctx)
        migrate_procedure_entity_requirements(ctx)
        # create_tables("sql/init_orgchart.sql", ctx.pg_engine, "ORGCHART")
        # migrate_areas_sub_areas(ctx)
        # migrate_legal_inquiries_types_legal_inquiries(ctx)
        # migrate_organigrams(ctx)
        # migrate_models(ctx)
        # migrate_organigram_emails(ctx)
        # migrate_notifications(ctx)
        # migrate_node_types(ctx)
        # migrate_node(ctx)
        # migrate_rule_types(ctx)
        # migrate_rules(ctx)
        # migrate_function_diagrams(ctx)
        # migrate_function_diagram_nodes(ctx)
        # migrate_organigram_attachments(ctx, "http://localhost:9010", "id-docs")
        # migrate_regulation(ctx, "http://localhost:9010", "avatars")
        # migrate_parameter(ctx)
        # create_tables("sql/init_cronos.sql", ctx.pg_engine, "CRONOS")
        # migrate_cronos_taxonomies(ctx)
        # migrate_dm70_taxonomies(ctx)
        # migrate_cronos_plan_grouping_specialties(ctx)
        # migrate_cronos_physical_structures(ctx)
        # migrate_healthcare_companies(ctx)
        # migrate_cronos_plan_specialty_aliases(ctx)
        # migrate_cronos_companies(ctx)
        # migrate_cronos_plan_specialties(ctx)
        # migrate_cronos_plans(ctx)

        end_time = datetime.now()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        logging.info(f"Total migration time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")
        logging.info("ETL process completed successfully")
    except Exception as e:
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        logging.error(
            f"Error during execution after {int(hours)}h {int(minutes)}m {seconds:.2f}s: {e!s}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    main()
