-- Sequences
CREATE SEQUENCE sq_procedures_progressive_code AS integer;

-- Tables
CREATE TABLE procedure_entities
(
    id                           UUID                     NOT NULL DEFAULT gen_random_uuid(),
    procedure_id                 UUID                     NOT NULL,
    object_type                  TEXT                     NOT NULL,
    object_reference             UUID                     NOT NULL,
    object_data                  JSONB                    NOT NULL,
    district_id                  UUID,
    udo_type_id                  UUID,
    outcome                      TEXT,
    outcome_start_date           DATE,
    outcome_end_date             DATE,
    outcome_notes                TEXT,
    outcome_resolution_id        UUID,
    outcome_user_id              UUID,
    outcome_user_last_first_name TEXT,
    outcome_created_at           TIMESTAMP WITH TIME ZONE,
    extra                        JSONB,
    created_at                   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_procedure_entities PRIMARY KEY (id)
);

CREATE TABLE procedure_entity_requirements
(
    id                        UUID                     NOT NULL DEFAULT gen_random_uuid(),
    root_procedure_id         UUID                     NOT NULL,
    procedure_entity_id       UUID                     NOT NULL,
    requirement_id            UUID                     NOT NULL,
    requirement_name          TEXT,
    requirement_text          TEXT,
    requirement_annotation    TEXT,
    requirement_response_type TEXT,
    requirement_taxonomy_id   UUID                     NOT NULL,
    requirement_taxonomy_name TEXT,
    response_assignee_user_id UUID,
    response                  TEXT,
    evidence                  TEXT,
    notes                     TEXT,
    extra                     JSONB,
    created_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_procedure_entity_requirements PRIMARY KEY (id)
);

CREATE TABLE procedure_type_requirement_list_classification_mental
(
    procedure_type             TEXT    NOT NULL,
    requirement_list_id        UUID    NOT NULL,
    udo_type_classification_id UUID    NOT NULL,
    is_mental_health           BOOLEAN NOT NULL,
    CONSTRAINT pk_procedure_type_requirement_list_classification_mental PRIMARY KEY (procedure_type,
                                                                                     requirement_list_id,
                                                                                     udo_type_classification_id,
                                                                                     is_mental_health)
);

CREATE TABLE procedure_type_requirement_list_comp_type_comp_class
(
    id                        UUID NOT NULL DEFAULT gen_random_uuid(),
    procedure_type            TEXT NOT NULL,
    requirement_list_id       UUID NOT NULL,
    company_type_id           UUID NOT NULL,
    company_classification_id UUID,
    CONSTRAINT pk_procedure_type_requirement_list_comp_type_comp_class PRIMARY KEY (id),
    CONSTRAINT uc_procedure_type_requirement_list_comp_type_comp_class UNIQUE NULLS NOT DISTINCT (procedure_type,
                                                                                                  requirement_list_id,
                                                                                                  company_type_id,
                                                                                                  company_classification_id)
);

CREATE TABLE procedure_type_requirement_list_for_physical_structures
(
    procedure_type      TEXT NOT NULL,
    requirement_list_id UUID NOT NULL,
    CONSTRAINT pk_procedure_type_requirement_list_for_physical_structures PRIMARY KEY (procedure_type, requirement_list_id)
);

CREATE TABLE procedure_type_requirement_list_udo_type
(
    procedure_type      TEXT NOT NULL,
    requirement_list_id UUID NOT NULL,
    udo_type_id         UUID NOT NULL,
    CONSTRAINT pk_procedure_type_requirement_list_udo_type PRIMARY KEY (procedure_type, requirement_list_id, udo_type_id)
);

CREATE TABLE procedures
(
    id                     UUID                     NOT NULL DEFAULT gen_random_uuid(),
    progressive_code       INTEGER                  NOT NULL DEFAULT nextval('sq_procedures_progressive_code'),
    company_id             UUID                     NOT NULL,
    procedure_type         TEXT                     NOT NULL,
    status                 TEXT                     NOT NULL,
    completion_date        DATE,
    sent_date              DATE,
    expiration_date        DATE,
    procedure_duration     INTEGER,
    max_procedure_duration INTEGER,
    procedure_number       TEXT,
    extra                  JSONB,
    disabled_at            TIMESTAMP WITH TIME ZONE,
    created_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_procedures PRIMARY KEY (id),
    CONSTRAINT uc_procedures_progressive_code UNIQUE (progressive_code)
);

CREATE TABLE requirement_taxonomies
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    is_readonly BOOLEAN                  NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_requirement_taxonomies PRIMARY KEY (id),
    CONSTRAINT uc_requirement_taxonomies_name UNIQUE (name)
);

CREATE TABLE requirements
(
    id                      UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                    TEXT                     NOT NULL,
    text                    TEXT                     NOT NULL,
    annotations             TEXT,
    state                   TEXT                     NOT NULL,
    is_required             BOOLEAN                  NOT NULL,
    requirement_taxonomy_id UUID                     NOT NULL,
    response_type           TEXT                     NOT NULL,
    extra                   JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at             TIMESTAMP WITH TIME ZONE,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_requirements PRIMARY KEY (id),
    CONSTRAINT uc_requirements_name UNIQUE (name)
);

CREATE TABLE requirement_lists
(
    id            UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name          TEXT                     NOT NULL,
    resolution_id UUID,
    extra         JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at   TIMESTAMP WITH TIME ZONE,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_requirement_lists PRIMARY KEY (id),
    CONSTRAINT uc_requirement_lists_name UNIQUE (name)
);

CREATE TABLE requirementlist_requirements
(
    requirement_list_id UUID NOT NULL,
    requirement_id      UUID NOT NULL,
    is_required         BOOLEAN,
    response_type       text,
    CONSTRAINT pk_requirementlist_requirements PRIMARY KEY (requirement_list_id, requirement_id)
);

-- Foreign key constraints
ALTER TABLE procedure_entities
    ADD CONSTRAINT fk_procedure_entities_procedures_id FOREIGN KEY (procedure_id)
        REFERENCES procedures (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE procedure_entity_requirements
    ADD CONSTRAINT fk_procedure_entity_requirements_procedure_entities_id FOREIGN KEY (procedure_entity_id)
        REFERENCES procedure_entities (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE procedure_type_requirement_list_classification_mental
    ADD CONSTRAINT fk_procedure_type_requirement_list_classification_mental_requirement_list_id FOREIGN KEY (requirement_list_id)
        REFERENCES requirement_lists (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE procedure_type_requirement_list_comp_type_comp_class
    ADD CONSTRAINT fk_procedure_type_requirement_list_comp_type_comp_class_requirement_list_id FOREIGN KEY (requirement_list_id)
        REFERENCES requirement_lists (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE procedure_type_requirement_list_for_physical_structures
    ADD CONSTRAINT fk_procedure_type_requirement_list_for_physical_structures_requirement_list_id FOREIGN KEY (requirement_list_id)
        REFERENCES requirement_lists (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE procedure_type_requirement_list_udo_type
    ADD CONSTRAINT fk_procedure_type_requirement_list_udo_type_requirement_list_id FOREIGN KEY (requirement_list_id)
        REFERENCES requirement_lists (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE requirementlist_requirements
    ADD CONSTRAINT fk_requirementlist_requirements_requirement_list_id FOREIGN KEY (requirement_list_id)
        REFERENCES requirement_lists (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE requirementlist_requirements
    ADD CONSTRAINT fk_requirementlist_requirements_requirement_id FOREIGN KEY (requirement_id)
        REFERENCES requirements (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE requirements
    ADD CONSTRAINT fk_requirements_requirement_taxonomy_id FOREIGN KEY (requirement_taxonomy_id)
        REFERENCES requirement_taxonomies (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

-- Alter Sequences
ALTER SEQUENCE sq_procedures_progressive_code OWNED BY procedures.progressive_code;