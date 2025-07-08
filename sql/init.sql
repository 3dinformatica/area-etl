----------------------------------------------------------USER----------------------------------------------------------
-- Tables
CREATE TABLE permissions
(
    id             UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name           TEXT                     NOT NULL,
    default_admin  TEXT,
    default_op_reg TEXT,
    default_op     TEXT,
    extra          JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at    TIMESTAMP WITH TIME ZONE,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_permissions PRIMARY KEY (id),
    CONSTRAINT uc_permissions_name UNIQUE (name)
);

CREATE TABLE user_companies
(
    id                      UUID                     NOT NULL DEFAULT gen_random_uuid(),
    user_id                 UUID                     NOT NULL,
    -- company_id type TEXT instead of UUID to allow using '*' as a value
    company_id              TEXT                     NOT NULL,
    is_legal_representative BOOLEAN                  NOT NULL DEFAULT FALSE,
    extra                   JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at             TIMESTAMP WITH TIME ZONE,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_user_companies PRIMARY KEY (id),
    CONSTRAINT uc_user_companies_user_id_and_company_id UNIQUE (user_id, company_id)
);

CREATE TABLE user_company_permissions
(
    id              UUID                     NOT NULL DEFAULT gen_random_uuid(),
    user_company_id UUID                     NOT NULL,
    permission_id   UUID                     NOT NULL,
    read            BOOLEAN                  NOT NULL DEFAULT FALSE,
    write           BOOLEAN                  NOT NULL DEFAULT FALSE,
    extra           JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at     TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_user_company_permissions PRIMARY KEY (id),
    CONSTRAINT uc_user_company_permissions_user_company_id_and_permission_id UNIQUE (user_company_id, permission_id)
);

CREATE TABLE users
(
    id                       UUID                     NOT NULL DEFAULT gen_random_uuid(),
    -- SUPER_USER, ADMIN, REGIONAL_OPERATOR, OPERATOR
    user_role                VARCHAR                  NOT NULL,
    first_name               TEXT                     NOT NULL,
    last_name                TEXT                     NOT NULL,
    tax_code                 TEXT                     NOT NULL,
    email                    TEXT                     NOT NULL,
    birth_date               DATE,
    birth_place              TEXT,
    street_name              TEXT,
    street_number            TEXT,
    phone                    TEXT,
    mobile_phone             TEXT,
    identity_doc_number      TEXT,
    identity_doc_expiry_date DATE,
    username                 TEXT                     NOT NULL,
    job                      TEXT,
    extra                    JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at              TIMESTAMP WITH TIME ZONE,
    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_users PRIMARY KEY (id),
    CONSTRAINT uc_users_username UNIQUE (username)
);

-- Foreign key constraints
ALTER TABLE user_companies
    ADD CONSTRAINT fk_user_companies_user_id FOREIGN KEY (user_id)
        REFERENCES users (id)
        ON DELETE CASCADE;

ALTER TABLE user_company_permissions
    ADD CONSTRAINT fk_user_company_permissions_user_company_id FOREIGN KEY (user_company_id)
        REFERENCES user_companies (id)
        ON DELETE CASCADE;

ALTER TABLE user_company_permissions
    ADD CONSTRAINT fk_user_company_permissions_permission_id FOREIGN KEY (permission_id)
        REFERENCES permissions (id)
        ON DELETE CASCADE;

----------------------------------------------------------COMPANY-------------------------------------------------------
-- Tables
CREATE TABLE buildings
(
    id                    UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                  TEXT                     NOT NULL,
    code                  VARCHAR(10)              NOT NULL,
    physical_structure_id UUID,
    is_own_property       BOOLEAN                  NOT NULL DEFAULT TRUE,
    owner_first_name      TEXT,
    owner_last_name       TEXT,
    owner_vat_number      TEXT,
    owner_business_name   TEXT,
    owner_tax_code        TEXT,
    extra                 JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at           TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_buildings PRIMARY KEY (id)
);

CREATE TABLE companies
(
    id                 UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name               TEXT,
    code               TEXT                     NOT NULL,
    company_name       TEXT                     NOT NULL,
    -- SOCIETA_SEMPLICE, SOCIETA_IN_NOME_COLLETTIVO, SOCIETA_IN_ACCOMANDITA_SEMPLICE, SOCIETA_A_RESPONSABILITA_LIMITATA, SOCIETA_A_RESPONSABILITA_LIMITATA_SEMPLIFICATA, SOCIETA_PER_AZIONI, SOCIETA_IN_ACCOMANDITA_PER_AZIONI, COOPERATIVA, REGIME_FORFETTARIO, SOCIETA_DI_CAPITALI_UNIPERSONALE, SOCIETA_EUROPEA, ODV, CONSORZIO, COMUNITA_MONTANA
    company_form       VARCHAR,
    -- SOCIETA, IMPRESA_INDIVIDUALE, STUDIO_PROFESSIONALE, ENTE_PUBBLICO, ASSOCIAZIONE, ASSOCIAZIONE_TEMPORANEA_DI_IMPRESA, ENTE_ECCLESIASTICO_CIVILMENTE_RICONOSCIUTO, FONDAZIONE, ENTE_MORALE_DI_DIRITTO_PRIVATO, CONSORZIO
    company_legal_form VARCHAR                  NOT NULL,
    -- PUBBLICO, PRIVATO, AZIENDA_SANITARIA
    company_nature     VARCHAR                  NOT NULL,
    tax_code           TEXT,
    vat_number         TEXT                     NOT NULL,
    email              TEXT                     NOT NULL,
    certified_email    TEXT,
    phone              TEXT,
    mobile_phone       TEXT,
    website_url        TEXT,
    street_name        TEXT,
    street_number      VARCHAR(20),
    zip_code           VARCHAR(5),
    company_type_id    UUID,
    municipality_id    UUID,
    toponym_id         UUID,
    extra              JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at        TIMESTAMP WITH TIME ZONE,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_companies PRIMARY KEY (id),
    CONSTRAINT uc_companies_code UNIQUE (code)
);

CREATE TABLE company_types
(
    id                                  UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                                TEXT                     NOT NULL,
    is_show_health_director_declaration BOOLEAN                  NOT NULL DEFAULT FALSE,
    is_active_organization_chart        BOOLEAN                  NOT NULL DEFAULT FALSE,
    extra                               JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at                         TIMESTAMP WITH TIME ZONE,
    created_at                          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_company_types PRIMARY KEY (id),
    CONSTRAINT uc_company_types_name UNIQUE (name)
);

CREATE TABLE operational_offices
(
    id                    UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                  TEXT                     NOT NULL,
    street_name           TEXT                     NOT NULL,
    street_number         TEXT                     NOT NULL,
    zip_code              VARCHAR(5),
    is_main_address       BOOLEAN                  NOT NULL DEFAULT FALSE,
    physical_point_type   TEXT,
    lat                   NUMERIC(8, 2),
    lon                   NUMERIC(8, 2),
    physical_structure_id UUID,
    toponym_id            UUID                     NOT NULL,
    municipality_id       UUID                     NOT NULL,
    extra                 JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at           TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_operational_offices PRIMARY KEY (id)
);

CREATE TABLE physical_structures
(
    id             UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name           TEXT                     NOT NULL,
    code           TEXT                     NOT NULL,
    secondary_code TEXT,
    company_id     UUID,
    district_id    UUID,
    extra          JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at    TIMESTAMP WITH TIME ZONE,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_physical_structures PRIMARY KEY (id),
    CONSTRAINT uc_physical_structures_code UNIQUE (code)
);

-- Foreign key constraints
ALTER TABLE companies
    ADD CONSTRAINT fk_companies_company_type_id FOREIGN KEY (company_type_id)
        REFERENCES company_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE physical_structures
    ADD CONSTRAINT fk_physical_structures_company_id FOREIGN KEY (company_id)
        REFERENCES companies (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE buildings
    ADD CONSTRAINT fk_buildings_physical_structure_id FOREIGN KEY (physical_structure_id)
        REFERENCES physical_structures (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE operational_offices
    ADD CONSTRAINT fk_operational_officeses_physical_structure_id FOREIGN KEY (physical_structure_id)
        REFERENCES physical_structures (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

----------------------------------------------------------LOCATION------------------------------------------------------
-- Tables
CREATE TABLE municipalities
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    istat_code  TEXT                     NOT NULL,
    province_id UUID                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_municipalities PRIMARY KEY (id),
    CONSTRAINT uc_municipalities_istat_code UNIQUE (istat_code)
);

CREATE TABLE provinces
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    acronym     TEXT                     NOT NULL,
    istat_code  TEXT                     NOT NULL,
    region_id   UUID                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_provinces PRIMARY KEY (id),
    CONSTRAINT uc_provinces_name UNIQUE (name),
    CONSTRAINT uc_provinces_istat_code UNIQUE (istat_code)
);

CREATE TABLE regions
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    istat_code  TEXT                     NOT NULL,
    img_url     TEXT,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_regions PRIMARY KEY (id),
    CONSTRAINT uc_regions_name UNIQUE (name),
    CONSTRAINT uc_regions_istat_code UNIQUE (istat_code)
);

CREATE TABLE toponyms
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_toponyms PRIMARY KEY (id),
    CONSTRAINT uc_toponyms_name UNIQUE (name)
);

CREATE TABLE ulss
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    code        TEXT                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_ulss PRIMARY KEY (id),
    CONSTRAINT uc_ulss_name UNIQUE (name),
    CONSTRAINT uc_ulss_code UNIQUE (code)
);

-- Foreign key constraints
ALTER TABLE municipalities
    ADD CONSTRAINT fk_municipalities_province_id FOREIGN KEY (province_id)
        REFERENCES provinces (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE provinces
    ADD CONSTRAINT fk_provinces_region_id FOREIGN KEY (region_id)
        REFERENCES regions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

----------------------------------------------------------UDO-----------------------------------------------------------
-- Tables
CREATE TABLE production_factor_types
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    code        TEXT                     NOT NULL,
    -- POSTI_LETTO, POSTI_LETTO_OBI, POSTI_LETTO_EXTRA, SALE_OPERATORIE, POSTI_PAGANTI, ALTRA_STANZA_NO_SIO, ALTRO, STANZA, ALTRA_STANZA
    category    VARCHAR                  NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_production_factor_types PRIMARY KEY (id),
    CONSTRAINT uc_production_factor_types_name UNIQUE (name),
    CONSTRAINT uc_production_factor_types_code UNIQUE (code)
);

CREATE TABLE production_factors
(
    id                        UUID                     NOT NULL DEFAULT gen_random_uuid(),
    beds                      INTEGER,
    hospital_beds             INTEGER,
    room_name                 TEXT,
    room_code                 TEXT,
    production_factor_type_id UUID                     NOT NULL,
    extra                     JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at               TIMESTAMP WITH TIME ZONE,
    created_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_production_factors PRIMARY KEY (id)
);

CREATE TABLE udo_specialties
(
    id                           UUID                     NOT NULL DEFAULT gen_random_uuid(),
    udo_id                       UUID                     NOT NULL,
    specialty_id                 UUID                     NOT NULL,
    clinical_operational_unit_id UUID,
    clinical_organigram_node_id  UUID,
    beds                         INTEGER,
    extra_beds                   INTEGER,
    mortuary_beds                INTEGER,
    accredited_beds              INTEGER,
    hsp12                        TEXT,
    is_authorized                BOOLEAN                  NOT NULL DEFAULT FALSE,
    is_accredited                BOOLEAN                  NOT NULL DEFAULT FALSE,
    extra                        JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at                  TIMESTAMP WITH TIME ZONE,
    created_at                   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT uc_udo_specialties UNIQUE (udo_id, specialty_id, clinical_operational_unit_id,
                                          clinical_organigram_node_id),
    CONSTRAINT check_one_clinical_id_or_none_set CHECK (
        (clinical_operational_unit_id IS NOT NULL AND clinical_organigram_node_id IS NULL) OR
        (clinical_operational_unit_id IS NULL AND clinical_organigram_node_id IS NOT NULL) OR
        (clinical_operational_unit_id IS NULL AND clinical_organigram_node_id IS NULL))
);

CREATE TABLE udo_production_factors
(
    udo_id               UUID NOT NULL,
    production_factor_id UUID NOT NULL,
    CONSTRAINT pk_udo_production_factors PRIMARY KEY (udo_id, production_factor_id)
);

CREATE TABLE udo_statuses
(
    id                 UUID                     NOT NULL DEFAULT gen_random_uuid(),
    udo_id             UUID                     NOT NULL,
    status             TEXT                     NOT NULL,
    beds               INTEGER,
    extra_beds         INTEGER,
    mortuary_beds      INTEGER,
    valid_from         DATE,
    valid_to           DATE,
    is_direct_supply   BOOLEAN                  NOT NULL DEFAULT FALSE,
    is_indirect_supply BOOLEAN                  NOT NULL DEFAULT FALSE,
    extra              JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at        TIMESTAMP WITH TIME ZONE,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_udo_statuses PRIMARY KEY (id)
);

CREATE TABLE udo_type_production_factor_types
(
    udo_type_id               UUID NOT NULL,
    production_factor_type_id UUID NOT NULL,
    CONSTRAINT pk_udo_type_production_factor_types PRIMARY KEY (udo_type_id, production_factor_type_id)
);

CREATE TABLE udo_types
(
    id                                             UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                                           TEXT                     NOT NULL,
    code                                           TEXT                     NOT NULL,
    code_name                                      TEXT                     NOT NULL,
    setting                                        TEXT,
    target                                         TEXT,
    is_hospital                                    BOOLEAN                  NOT NULL,
    is_mental_health                               BOOLEAN                  NOT NULL,
    has_beds                                       BOOLEAN                  NOT NULL,
    scope_name                                     TEXT                     NOT NULL,
    scope_description                              TEXT                     NOT NULL,
    has_disciplines                                BOOLEAN                  NOT NULL,
    has_disciplines_only_healthcare_company        BOOLEAN                  NOT NULL,
    has_disciplines_only_public_or_private_company BOOLEAN                  NOT NULL,
    has_branches                                   BOOLEAN                  NOT NULL,
    has_branches_only_healthcare_company           BOOLEAN                  NOT NULL,
    has_branches_only_public_or_private_company    BOOLEAN                  NOT NULL,
    has_services                                   BOOLEAN                  NOT NULL,
    has_scopes                                     BOOLEAN                  NOT NULL,
    company_natures                                VARCHAR[],
    -- FLUSSO_STS, FLUSSO_HSP, FLUSSO_RIA, IN_ATTESA, NON_PREVISTO
    ministerial_flows                              VARCHAR[],
    classification_id                              UUID,
    discipline_id                                  UUID,
    extra                                          JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at                                    TIMESTAMP WITH TIME ZONE,
    created_at                                     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                                     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_udo_types PRIMARY KEY (id),
    CONSTRAINT uc_udo_types_name UNIQUE (name),
    CONSTRAINT uc_udo_types_code UNIQUE (code),
    CONSTRAINT uc_udo_types_code_name UNIQUE (code_name)
);

CREATE TABLE udos
(
    id                            UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                          TEXT                     NOT NULL,
    code                          TEXT,
    floor                         TEXT,
    block                         TEXT,
    progressive                   TEXT,
    ministerial_code              TEXT,
    farfad_code                   TEXT,
    is_sio                        BOOLEAN,
    starep_code                   TEXT,
    cost_center                   TEXT,
    keywords                      TEXT,
    notes                         TEXT,
    is_open_only_on_business_days BOOLEAN,
    is_auac                       BOOLEAN,
    is_module                     BOOLEAN,
    udo_type_id                   UUID                     NOT NULL,
    operational_office_id         UUID,
    building_id                   UUID,
    operational_unit_id           UUID,
    organigram_node_id            UUID,
    company_id                    UUID                     NOT NULL,
    extra                         JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at                   TIMESTAMP WITH TIME ZONE,
    created_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_udos PRIMARY KEY (id),
    CONSTRAINT uc_udos_code UNIQUE (code)
);

-- Foreign key constraints
ALTER TABLE production_factors
    ADD CONSTRAINT fk_production_factors_production_factor_type_id FOREIGN KEY (production_factor_type_id)
        REFERENCES production_factor_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE udo_specialties
    ADD CONSTRAINT fk_udo_specialties_udo_id FOREIGN KEY (udo_id)
        REFERENCES udos (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE udo_production_factors
    ADD CONSTRAINT fk_udo_production_factors_udo_id FOREIGN KEY (udo_id)
        REFERENCES udos (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE udo_production_factors
    ADD CONSTRAINT fk_udo_production_factors_production_factor_id FOREIGN KEY (production_factor_id)
        REFERENCES production_factors (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE udo_statuses
    ADD CONSTRAINT fk_udo_statuses_udo_id FOREIGN KEY (udo_id)
        REFERENCES udos (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE udo_type_production_factor_types
    ADD CONSTRAINT fk_udo_type_production_factor_types_udo_type_id FOREIGN KEY (udo_type_id)
        REFERENCES udo_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE udo_type_production_factor_types
    ADD CONSTRAINT fk_udo_type_production_factor_types_production_factor_type_id FOREIGN KEY (production_factor_type_id)
        REFERENCES production_factor_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE udos
    ADD CONSTRAINT fk_udos_type_id FOREIGN KEY (udo_type_id)
        REFERENCES udo_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

-------------------------------------------------------SPECIALTY--------------------------------------------------------
-- Tables
CREATE TABLE grouping_specialties
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    macroarea   TEXT,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_grouping_specialties PRIMARY KEY (id),
    CONSTRAINT uc_grouping_specialties_name UNIQUE (name)
);

CREATE TABLE specialties
(
    id                    UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                  TEXT                     NOT NULL,
    description           TEXT,
    -- DISCIPLINE, BRANCH
    record_type           TEXT                     NOT NULL,
    -- ALTRO, TERRITORIALE, OSPEDALIERO, NON_OSPEDALIERO
    type                  TEXT,
    code                  TEXT                     NOT NULL,
    is_used_in_cronos     BOOLEAN                  NOT NULL DEFAULT TRUE,
    is_used_in_poa        BOOLEAN                  NOT NULL DEFAULT TRUE,
    grouping_specialty_id UUID,
    parent_specialty_id   UUID,
    -- TODO: Drop
    old_id                TEXT,
    extra                 JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at           TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_specialties PRIMARY KEY (id),
    CONSTRAINT uc_specialties_name_and_code UNIQUE (name, code)
);

-- Foreign key constraints
ALTER TABLE specialties
    ADD CONSTRAINT fk_specialties_grouping_discipline_id FOREIGN KEY (grouping_specialty_id)
        REFERENCES grouping_specialties (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE specialties
    ADD CONSTRAINT fk_specialties_parent_specialty_id FOREIGN KEY (parent_specialty_id)
        REFERENCES specialties (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

-------------------------------------------------------RESOLUTION-------------------------------------------------------
-- Tables
CREATE TABLE resolution_types
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_resolution_types PRIMARY KEY (id),
    CONSTRAINT uc_resolution_types_name UNIQUE (name)
);

CREATE TABLE resolutions
(
    id                   UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                 TEXT,
    -- UDO, GENERALE, PROGRAMMAZIONE, REQUISITI
    category             VARCHAR                  NOT NULL,
    -- AUTORIZZAZIONE, ACCREDITAMENTO, REVOCA_AUT, REVOCA_ACC
    procedure_type       VARCHAR,
    number               TEXT,
    year                 INTEGER,
    valid_from           TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to             TIMESTAMP WITH TIME ZONE,
    bur_number           INTEGER,
    bur_date             TIMESTAMP WITH TIME ZONE,
    dgr_link             TEXT,
    direction            TEXT,
    file_id              TEXT,
    resolution_type_id   UUID                     NOT NULL,
    parent_resolution_id UUID,
    company_id           UUID,
    extra                JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at          TIMESTAMP WITH TIME ZONE,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_resolutions PRIMARY KEY (id),
    CONSTRAINT uc_resolutions_name UNIQUE (name)
);

-- Foreign key constraints
ALTER TABLE resolutions
    ADD CONSTRAINT fk_resolutions_resolution_type_id FOREIGN KEY (resolution_type_id)
        REFERENCES resolution_types (id)
        ON DELETE CASCADE;


-------------------------------------------------------ORGCHART---------------------------------------------------------
-- Tables
CREATE TABLE areas
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description TEXT                     NOT NULL,
    old_id      TEXT,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_areas PRIMARY KEY (id),
    CONSTRAINT uc_areas_description UNIQUE (description)
);

CREATE TABLE function_diagram_nodes
(
    function_diagram_id UUID NOT NULL,
    node_id             UUID NOT NULL,
    CONSTRAINT pk_function_diagram_nodes PRIMARY KEY (function_diagram_id, node_id)
);

CREATE TABLE function_diagrams
(
    id              UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description     TEXT                     NOT NULL,
    coordinator     TEXT                     NOT NULL,
    legal_entity_id UUID                     NOT NULL,
    extra           JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at     TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_function_diagrams PRIMARY KEY (id)
);

CREATE TABLE legal_inquiries
(
    id                      UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description             TEXT                     NOT NULL,
    legal_inquiries_type_id UUID                     NOT NULL,
    old_id                  TEXT,
    extra                   JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at             TIMESTAMP WITH TIME ZONE,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_legal_inquiries PRIMARY KEY (id),
    CONSTRAINT uc_legal_inquiries_description_legal_inquiries_type_id UNIQUE (description, legal_inquiries_type_id)
);

CREATE TABLE legal_inquiries_types
(
    id                   UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description          TEXT                     NOT NULL,
    show_anagraphic_auac BOOLEAN                  NOT NULL DEFAULT TRUE,
    old_id               TEXT,
    extra                JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at          TIMESTAMP WITH TIME ZONE,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_legal_inquiries_types PRIMARY KEY (id)
);

CREATE TABLE models
(
    id              UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description     TEXT                     NOT NULL,
    version         INTEGER                  NOT NULL DEFAULT 0,
    legal_entity_id UUID                     NOT NULL,
    organigram_id   UUID                     NOT NULL,
    extra           JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at     TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_models PRIMARY KEY (id),
    CONSTRAINT uc_models_description_organigram_id UNIQUE (description, organigram_id)
);

CREATE TABLE node_types
(
    id                 UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description        TEXT                     NOT NULL,
    area_id            UUID,
    legal_inquiries_id UUID,
    extra              JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at        TIMESTAMP WITH TIME ZONE,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_node_types PRIMARY KEY (id),
    CONSTRAINT uc_node_types_description UNIQUE (description)
);

CREATE TABLE nodes
(
    id                            UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name                          TEXT                     NOT NULL,
    code                          TEXT                     NOT NULL,
    valid_from                    DATE,
    valid_to                      DATE,
    dismission_date               DATE,
    annotations                   TEXT,
    version                       INTEGER,
    progressive                   INTEGER,
    presidium                     TEXT,
    abbreviation_univocal_uo_code TEXT,
    is_coordination               BOOLEAN,
    is_acting_functionary         BOOLEAN,
    is_valid_organigram           BOOLEAN,
    role_of_responsible           TEXT,
    -- EROGA, NON_EROGA
    activity                      TEXT,
    -- IN_DISMISSIONE, NUOVA, ASSENTE
    state                         TEXT,
    profile                       TEXT,
    email                         TEXT,
    responsible_id                TEXT,
    parent_node_id                UUID,
    organigram_id                 UUID,
    node_type_id                  UUID,
    specialty_id                  UUID,
    sub_area_id                   UUID,
    extra                         JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at                   TIMESTAMP WITH TIME ZONE,
    created_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_nodes PRIMARY KEY (id)
);

CREATE TABLE notifications
(
    id                UUID                     NOT NULL DEFAULT gen_random_uuid(),
    message           TEXT                     NOT NULL,
    is_viewed         BOOLEAN                  NOT NULL DEFAULT FALSE,
    organigram_status TEXT,
    date_viewed       DATE,
    sender_id         TEXT                     NOT NULL,
    recipient_id      TEXT                     NOT NULL,
    organigram_id     UUID                     NOT NULL,
    extra             JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at       TIMESTAMP WITH TIME ZONE,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_notifications PRIMARY KEY (id)
);

CREATE TABLE organigram_attachments
(
    organigram_id UUID NOT NULL,
    file_id       UUID NOT NULL,
    CONSTRAINT pk_organigram_attachments PRIMARY KEY (organigram_id, file_id)
);

CREATE TABLE organigram_emails
(
    id            UUID                     NOT NULL DEFAULT gen_random_uuid(),
    address       TEXT                     NOT NULL,
    organigram_id UUID                     NOT NULL,
    extra         JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at   TIMESTAMP WITH TIME ZONE,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_organigram_emails PRIMARY KEY (id),
    CONSTRAINT uc_organigram_emails_address_organigram_id UNIQUE (address, organigram_id)
);

CREATE TABLE organigrams
(
    id              UUID                     NOT NULL DEFAULT gen_random_uuid(),
    version         INTEGER                  NOT NULL DEFAULT 0,
    -- BOZZA, RIFIUTATO, IN_APPROVAZIONE, VISTO, APPROVATO, ARCHIVIATO, APPROVATO_REGIONE,
    status_code     TEXT                     NOT NULL,
    validated_at    TIMESTAMP WITH TIME ZONE,
    is_from_model   BOOLEAN                  NOT NULL DEFAULT FALSE,
    legal_entity_id UUID,
    extra           JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at     TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_organigrams PRIMARY KEY (id)
);

CREATE TABLE parameters
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    key         TEXT                     NOT NULL,
    value       TEXT                     NOT NULL,
    annotations TEXT,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_parameters PRIMARY KEY (id)
);

CREATE TABLE rule_types
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    type        INTEGER                  NOT NULL,
    description TEXT                     NOT NULL,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_rule_types PRIMARY KEY (id),
    CONSTRAINT uc_rule_types_type UNIQUE (type)
);

CREATE TABLE rules
(
    id                      UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description             TEXT                     NOT NULL,
    max_num                 INTEGER,
    min_num                 INTEGER,
    max_relation            INTEGER,
    forbidden_child         BOOLEAN                  NOT NULL DEFAULT FALSE,
    forbidden_parent        BOOLEAN                  NOT NULL DEFAULT FALSE,
    presidium               TEXT,
    legal_inquiries_type_id UUID,
    specialty_id            UUID,
    node_type_id            UUID,
    rule_type_id            UUID,
    child_node_type_id      UUID,
    parent_node_type_id     UUID,
    legal_entity_id         UUID,
    extra                   JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at             TIMESTAMP WITH TIME ZONE,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_rules PRIMARY KEY (id)
);

CREATE TABLE sub_areas
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description TEXT                     NOT NULL,
    area_id     UUID                     NOT NULL,
    old_id      TEXT,
    extra       JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_sub_areas PRIMARY KEY (id),
    CONSTRAINT uc_sub_areas_description_area_id UNIQUE (description, area_id)
);

-- Foreign key constraints
ALTER TABLE function_diagram_nodes
    ADD CONSTRAINT fk_function_diagram_nodes_function_diagram_id FOREIGN KEY (function_diagram_id)
        REFERENCES function_diagrams (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE function_diagram_nodes
    ADD CONSTRAINT fk_function_diagram_nodes_node_id FOREIGN KEY (node_id)
        REFERENCES nodes (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE legal_inquiries
    ADD CONSTRAINT fk_legal_inquiries_legal_inquiries_type_id FOREIGN KEY (legal_inquiries_type_id)
        REFERENCES legal_inquiries_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE models
    ADD CONSTRAINT fk_models_organigram_id FOREIGN KEY (organigram_id)
        REFERENCES organigrams (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE node_types
    ADD CONSTRAINT fk_node_types_area_id FOREIGN KEY (area_id)
        REFERENCES areas (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE node_types
    ADD CONSTRAINT fk_node_legal_inquiries_id FOREIGN KEY (legal_inquiries_id)
        REFERENCES legal_inquiries (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE nodes
    ADD CONSTRAINT fk_nodes_organigram_id FOREIGN KEY (organigram_id)
        REFERENCES organigrams (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE nodes
    ADD CONSTRAINT fk_nodes_node_type_id FOREIGN KEY (node_type_id)
        REFERENCES node_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE nodes
    ADD CONSTRAINT fk_nodes_sub_area_id FOREIGN KEY (sub_area_id)
        REFERENCES sub_areas (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE organigram_attachments
    ADD CONSTRAINT fk_organigram_organigram_id FOREIGN KEY (organigram_id)
        REFERENCES organigrams (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE organigram_emails
    ADD CONSTRAINT fk_organigram_emails_organigram_id FOREIGN KEY (organigram_id)
        REFERENCES organigrams (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE rules
    ADD CONSTRAINT fk_rules_legal_inquiries_type_id FOREIGN KEY (legal_inquiries_type_id)
        REFERENCES legal_inquiries_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE rules
    ADD CONSTRAINT fk_rules_node_type_id FOREIGN KEY (node_type_id)
        REFERENCES node_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE rules
    ADD CONSTRAINT fk_rules_rule_type_id FOREIGN KEY (rule_type_id)
        REFERENCES rule_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE sub_areas
    ADD CONSTRAINT fk_sub_areas_area_id FOREIGN KEY (area_id)
        REFERENCES areas (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

-------------------------------------------------------CRONOS-----------------------------------------------------------
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
    sub_discipline_id                  UUID,
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

CREATE TABLE cronos_plan_grouping_disciplines
(
    id                     UUID                     NOT NULL DEFAULT gen_random_uuid(),
    num_beds_extra_reg     INTEGER,
    cronos_plan_id         UUID                     NOT NULL,
    grouping_discipline_id UUID                     NOT NULL,
    extra                  JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at            TIMESTAMP WITH TIME ZONE,
    created_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_cronos_plan_grouping_disciplines PRIMARY KEY (id)
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

ALTER TABLE cronos_plan_grouping_disciplines
    ADD CONSTRAINT fk_cron_plan_group_disc_plan_id FOREIGN KEY (cronos_plan_id)
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

-------------------------------------------------------SIGMA------------------------------------------------------------
-- Tables
CREATE TABLE employee_specializations
(
    employee_id       VARCHAR(10) NOT NULL,
    specialization_id UUID        NOT NULL,
    CONSTRAINT pk_employee_specializations PRIMARY KEY (employee_id, specialization_id)
);

CREATE TABLE employees
(
    id                         VARCHAR(10)              NOT NULL,
    hrgroup_code               VARCHAR(3),
    last_name                  TEXT,
    first_name                 TEXT,
    tax_code                   TEXT,
    date_of_birth              DATE,
    -- M, F
    gender                     VARCHAR(1),
    email                      TEXT,
    role_id                    INTEGER,
    profile_cod                TEXT,
    establishment_code         TEXT,
    department_code            TEXT,
    hire_date                  DATE,
    discharge_date             DATE,
    validity_segment_from_date DATE,
    validity_segment_to_date   DATE,
    operational_unit_primary   BOOLEAN                  NOT NULL DEFAULT false,
    work_context_id            UUID,
    created_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_employees PRIMARY KEY (id)
);

CREATE TABLE establishment_department_uo
(
    hrgroup_code              VARCHAR(3)               NOT NULL,
    establishment_code        TEXT                     NOT NULL,
    department_code           TEXT                     NOT NULL,
    department_description    TEXT,
    establishment_description TEXT,
    organigram_node_id        UUID,
    created_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_establishment_department_uo PRIMARY KEY (hrgroup_code, establishment_code, department_code)
);

CREATE TABLE hr_groups
(
    code        VARCHAR(3)               NOT NULL,
    description TEXT,
    company_id  UUID,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_hr_groups PRIMARY KEY (code)
);

CREATE TABLE profiles
(
    hrgroup_code             VARCHAR(3)               NOT NULL,
    code                     TEXT                     NOT NULL,
    profile_cod              TEXT,
    description              TEXT                     NOT NULL,
    job_position_cod         TEXT,
    job_position_description TEXT,
    qualification            TEXT,
    qualification2           TEXT,
    profile_gdl              TEXT                     NOT NULL,
    description_gdl          TEXT,
    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_profiles PRIMARY KEY (hrgroup_code, code)
);

CREATE TABLE roles
(
    id           INTEGER                  NOT NULL,
    hrgroup_code VARCHAR(3)               NOT NULL,
    description  TEXT,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_roles PRIMARY KEY (id, hrgroup_code)
);

CREATE TABLE shedlock
(
    name       VARCHAR(64)  NOT NULL,
    lock_until TIMESTAMP    NOT NULL,
    locked_at  TIMESTAMP    NOT NULL,
    locked_by  VARCHAR(255) NOT NULL,
    CONSTRAINT pk_shedlock PRIMARY KEY (name)
);

CREATE TABLE sigma_import_info
(
    id               UUID                     NOT NULL DEFAULT gen_random_uuid(),
    date_last_update DATE,
    scheduled        BOOLEAN                  NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_sigma_import_info PRIMARY KEY (id)
);

CREATE TABLE specializations
(
    id          UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT                     NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_specializations PRIMARY KEY (id),
    CONSTRAINT uc_specializations_name UNIQUE (name)
);

CREATE TABLE work_context_udos
(
    work_context_id UUID    NOT NULL,
    udo_id          UUID    NOT NULL,
    visibility_only BOOLEAN NOT NULL DEFAULT false,
    CONSTRAINT pk_work_context_udos PRIMARY KEY (work_context_id, udo_id)
);

CREATE TABLE work_contexts
(
    id                 UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description        TEXT                     NOT NULL,
    employees_number   INTEGER,
    hrgroup_code       VARCHAR(3),
    establishment_code TEXT,
    department_code    TEXT,
    disabled_at        TIMESTAMP WITH TIME ZONE,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_work_contexts PRIMARY KEY (id)
);

-- Foreign key constraints
ALTER TABLE employee_specializations
    ADD CONSTRAINT fk_employee_specializations_employees FOREIGN KEY (employee_id)
        REFERENCES employees (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE employee_specializations
    ADD CONSTRAINT fk_employee_specializations_specializations FOREIGN KEY (specialization_id)
        REFERENCES specializations (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE employees
    ADD CONSTRAINT fk_employees_hrgroups_code FOREIGN KEY (hrgroup_code)
        REFERENCES hr_groups (code)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE employees
    ADD CONSTRAINT fk_employees_roles FOREIGN KEY (hrgroup_code, role_id)
        REFERENCES roles (hrgroup_code, id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE employees
    ADD CONSTRAINT fk_employees_profiles FOREIGN KEY (hrgroup_code, profile_cod)
        REFERENCES profiles (hrgroup_code, code)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE employees
    ADD CONSTRAINT fk_employees_establishment_department_uo FOREIGN KEY (hrgroup_code, establishment_code, department_code)
        REFERENCES establishment_department_uo (hrgroup_code, establishment_code, department_code)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE employees
    ADD CONSTRAINT fk_employees_workcontexts FOREIGN KEY (work_context_id)
        REFERENCES work_contexts (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE establishment_department_uo
    ADD CONSTRAINT fk_establishment_department_uo_hr_groups_code FOREIGN KEY (hrgroup_code)
        REFERENCES hr_groups (code)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE profiles
    ADD CONSTRAINT fk_profiles_hrgroup_code FOREIGN KEY (hrgroup_code)
        REFERENCES hr_groups (code)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE roles
    ADD CONSTRAINT fk_roles_hrgroup_code FOREIGN KEY (hrgroup_code)
        REFERENCES hr_groups (code)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE work_context_udos
    ADD CONSTRAINT fk_work_context_udos_work_contexts FOREIGN KEY (work_context_id)
        REFERENCES work_contexts (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE work_contexts
    ADD CONSTRAINT fk_work_contexts_establishment_department_uo FOREIGN KEY (hrgroup_code, establishment_code, department_code)
        REFERENCES establishment_department_uo (hrgroup_code, establishment_code, department_code)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

--------------------------------------------------------AUAC------------------------------------------------------------
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
    CONSTRAINT pk_procedure_type_requirement_list_comp_type_comp_class PRIMARY KEY (id)

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