\c area_cronos_db

-- Tables
CREATE TABLE cronos_companies
(
    id              UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name            TEXT,
    code            TEXT                     NOT NULL,
    --   PUBBLICO, PRIVATO, AZIENDA_SANITARIA
    cronos_nature   VARCHAR                  NOT NULL,
    area_company_id UUID,
    extra           JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at     TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_cronos_companies PRIMARY KEY (id),
    CONSTRAINT uc_cronos_companies_code UNIQUE (code)
);

CREATE TABLE cronos_physical_structures
(
    id                         UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                       TEXT                     NOT NULL,
    hsp_code                   VARCHAR(8),
    municipality_id            UUID,
    area_physical_structure_id UUID,
    cronos_company_id          UUID,
    dm70_taxonomy_id           UUID,
    extra                      JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at                TIMESTAMP WITH TIME ZONE,
    created_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_cronos_physical_structures PRIMARY KEY (id)
);

CREATE TABLE cronos_plan_specialties
(
    id                                 UUID                     NOT NULL DEFAULT gen_random_uuid(),
    cronos_plan_id                     UUID                     NOT NULL,
    specialty_id                       UUID                     NOT NULL,
    is_ambulatory                      BOOLEAN                  NOT NULL DEFAULT FALSE,
    num_uoc                            INTEGER,
    deletion_resolution_insert_dt      TIMESTAMP WITH TIME ZONE,
    deletion_resolution_insert_user_id UUID,
    code                               INTEGER,
    validity_end_dt                    TIMESTAMP WITH TIME ZONE,
    cronos_plan_specialty_alias_id     UUID,
    deletion_resolution_id             UUID,
    validity_resolution_id             UUID,
    validity_start_dt                  TIMESTAMP WITH TIME ZONE,
    validity_resolution_insert_dt      TIMESTAMP WITH TIME ZONE,
    validity_resolution_insert_user_id UUID,
    num_uos                            INTEGER,
    note                               TEXT,
    num_beds_regional                  INTEGER,
    num_uosd                           INTEGER,
    activities                         TEXT,
    parent_id                          UUID,
    sub_specialty_id                   UUID,
    num_beds_dialysis                  INTEGER,
    num_beds_mortuary                  INTEGER,
    extra                              JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at                        TIMESTAMP WITH TIME ZONE,
    created_at                         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_cronos_plan_specialty PRIMARY KEY (id)
);

CREATE TABLE cronos_plan_specialty_aliases
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_cronos_plan_specialty_aliases PRIMARY KEY (id),
    CONSTRAINT uc_cronos_plan_specialty_aliases_name UNIQUE (name)
);

CREATE TABLE cronos_plan_grouping_specialties
(
    id                    UUID                     NOT NULL DEFAULT gen_random_uuid(),
    num_beds_extra_reg    INTEGER,
    cronos_plan_id        UUID                     NOT NULL,
    grouping_specialty_id UUID                     NOT NULL,
    extra                 JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at           TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_cronos_plan_grouping_specialties PRIMARY KEY (id)
);

CREATE TABLE cronos_plans
(
    id                           UUID                     NOT NULL DEFAULT gen_random_uuid(),
    is_mental_health             BOOLEAN                  NOT NULL DEFAULT FALSE,
    --   GESTIONE_PUBBLICA, GESTIONE_PRIVATA
    ssn_relationship             VARCHAR                  NOT NULL,
    --   BOZZA, VALIDO, CHIUSO
    cronos_status                VARCHAR                  NOT NULL,
    --   HSP, STS, RIA
    cronos_flow_type             VARCHAR,
    -- OSP, SM, SS
    cronos_category              VARCHAR,
    -- ACUTI, RIABILITAZIONE, INTERMEDIE
    macro_area                   VARCHAR                  NOT NULL,
    -- ATTIVITA_OSPEDALIERE, ATTIVITA_INTERMEDIE, ATTIVITA_TERRITORIALE
    cronos_scope                 VARCHAR                  NOT NULL,
    validity_end_dt              TIMESTAMP WITH TIME ZONE,
    deletion_resolution_id       UUID,
    validity_resolution_id       UUID,
    cronos_company_id            UUID,
    org_note                     TEXT,
    osp_note                     TEXT,
    healthcare_company_id        UUID                     NOT NULL,
    cronos_physical_structure_id UUID                     NOT NULL,
    cronos_taxonomy_id           UUID                     NOT NULL,
    ulss_id                      UUID                     NOT NULL,
    extra                        JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at                  TIMESTAMP WITH TIME ZONE,
    created_at                   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_cronos_plan PRIMARY KEY (id)
);

CREATE TABLE cronos_taxonomies
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_cronos_taxonomies PRIMARY KEY (id),
    CONSTRAINT uc_cronos_taxonomies_name UNIQUE (name)
);

CREATE TABLE dm70_taxonomies
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_dm70_taxonomies PRIMARY KEY (id),
    CONSTRAINT uc_dm70_taxonomies_name UNIQUE (name)
);

CREATE TABLE healthcare_companies
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    code        TEXT                     NOT NULL,
    name        TEXT                     NOT NULL,
    ulss_id     UUID,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_healthcare_companies PRIMARY KEY (id)
);

-- Foreign key constraints
ALTER TABLE cronos_physical_structures
    ADD CONSTRAINT fk_cronos_physical_structure_cronos_company_id FOREIGN KEY (cronos_company_id)
        REFERENCES cronos_companies (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE cronos_physical_structures
    ADD CONSTRAINT fk_cronos_physical_structure_dm70_taxonomy_id FOREIGN KEY (dm70_taxonomy_id)
        REFERENCES dm70_taxonomies (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE cronos_plan_specialties
    ADD CONSTRAINT fk_cronos_plan_specialty_cronos_plan_id FOREIGN KEY (cronos_plan_id)
        REFERENCES cronos_plans (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE cronos_plan_specialties
    ADD CONSTRAINT fk_cronos_plan_specialty_parent_id FOREIGN KEY (parent_id)
        REFERENCES cronos_plan_specialties (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE cronos_plan_grouping_specialties
    ADD CONSTRAINT fk_cron_plan_group_spec_plan_id FOREIGN KEY (cronos_plan_id)
        REFERENCES cronos_plans (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE cronos_plans
    ADD CONSTRAINT fk_cronos_plan_healthcare_company_id FOREIGN KEY (healthcare_company_id)
        REFERENCES healthcare_companies (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE cronos_plans
    ADD CONSTRAINT fk_cronos_plan_cronos_physical_structure_id FOREIGN KEY (cronos_physical_structure_id)
        REFERENCES cronos_physical_structures (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE cronos_plans
    ADD CONSTRAINT fk_cronos_plan_cronos_taxonomy_id FOREIGN KEY (cronos_taxonomy_id)
        REFERENCES cronos_taxonomies (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;