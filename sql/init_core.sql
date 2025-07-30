\c area_core_db

CREATE TABLE public.buildings
(
    id                    UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name                  TEXT                                               NOT NULL,
    code                  VARCHAR(10)                                        NOT NULL,
    is_own_property       BOOLEAN                                            NOT NULL,
    owner_first_name      TEXT,
    owner_last_name       TEXT,
    owner_vat_number      TEXT,
    owner_business_name   TEXT,
    owner_tax_code        TEXT,
    physical_structure_id UUID                                               NOT NULL,
    extra                 JSONB,
    disabled_at           TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_buildings PRIMARY KEY (id)
);

CREATE TABLE public.companies
(
    id              UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name            TEXT,
    code            TEXT                                               NOT NULL,
    business_name   TEXT                                               NOT NULL,
    business_form   VARCHAR(100),
    legal_form      VARCHAR(100)                                       NOT NULL,
    nature          VARCHAR(100)                                       NOT NULL,
    tax_code        TEXT,
    vat_number      TEXT                                               NOT NULL,
    email           TEXT                                               NOT NULL,
    certified_email TEXT,
    phone           TEXT,
    mobile_phone    TEXT,
    website_url     TEXT,
    street_name     TEXT,
    street_number   VARCHAR(20),
    zip_code        VARCHAR(5),
    company_type_id UUID,
    municipality_id UUID,
    toponym_id      UUID,
    disabled_at     TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_companies PRIMARY KEY (id),
    CONSTRAINT unique_companies_code UNIQUE (code)
);

COMMENT ON COLUMN public.companies.business_form IS 'Enum values: SOCIETA_SEMPLICE, SOCIETA_IN_NOME_COLLETTIVO, SOCIETA_IN_ACCOMANDITA_SEMPLICE, SOCIETA_A_RESPONSABILITA_LIMITATA, SOCIETA_A_RESPONSABILITA_LIMITATA_SEMPLIFICATA, SOCIETA_PER_AZIONI, SOCIETA_IN_ACCOMANDITA_PER_AZIONI, COOPERATIVA, REGIME_FORFETTARIO, SOCIETA_DI_CAPITALI_UNIPERSONALE, SOCIETA_EUROPEA, ODV, CONSORZIO, COMUNITA_MONTANA';

COMMENT ON COLUMN public.companies.legal_form IS 'Enum values: SOCIETA, IMPRESA_INDIVIDUALE, STUDIO_PROFESSIONALE, ENTE_PUBBLICO, ASSOCIAZIONE, ASSOCIAZIONE_TEMPORANEA_DI_IMPRESA, ENTE_ECCLESIASTICO_CIVILMENTE_RICONOSCIUTO, FONDAZIONE, ENTE_MORALE_DI_DIRITTO_PRIVATO, CONSORZIO';

COMMENT ON COLUMN public.companies.nature IS 'Enum values: PUBBLICO, PRIVATO, AZIENDA_SANITARIA';

CREATE TABLE public.company_types
(
    id                                      UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name                                    TEXT                                               NOT NULL,
    is_show_health_director_declaration_poa BOOLEAN                                            NOT NULL,
    is_active_poa                           BOOLEAN                                            NOT NULL,
    disabled_at                             TIMESTAMP WITH TIME ZONE,
    created_at                              TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at                              TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_company_types PRIMARY KEY (id),
    CONSTRAINT unique_company_types_name UNIQUE (name)
);

CREATE TABLE public.districts
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    code        INTEGER                                            NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_districts PRIMARY KEY (id),
    CONSTRAINT unique_districts_name UNIQUE (name)
);

CREATE TABLE public.grouping_specialties
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    macroarea   TEXT,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_grouping_specialties PRIMARY KEY (id),
    CONSTRAINT unique_grouping_specialties_name UNIQUE (name)
);

CREATE TABLE public.municipalities
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    istat_code  TEXT                                               NOT NULL,
    province_id UUID                                               NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_municipalities PRIMARY KEY (id),
    CONSTRAINT unique_municipalities_istat_code UNIQUE (istat_code)
);

CREATE TABLE public.operational_offices
(
    id                    UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name                  TEXT                                               NOT NULL,
    street_name           TEXT                                               NOT NULL,
    street_number         VARCHAR(20)                                        NOT NULL,
    zip_code              VARCHAR(5),
    is_main_address       BOOLEAN                                            NOT NULL,
    physical_point_type   TEXT,
    lat                   numeric(8, 2),
    lon                   numeric(8, 2),
    physical_structure_id UUID                                               NOT NULL,
    toponym_id            UUID                                               NOT NULL,
    municipality_id       UUID                                               NOT NULL,
    disabled_at           TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_operational_offices PRIMARY KEY (id)
);

CREATE TABLE public.operational_units
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    code        TEXT                                               NOT NULL,
    name        TEXT                                               NOT NULL,
    description TEXT,
    company_id  UUID                                               NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_operational_units PRIMARY KEY (id),
    CONSTRAINT unique_operational_units_code UNIQUE (code)
);

CREATE TABLE public.permissions
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    code        TEXT                                               NOT NULL,
    description TEXT,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_permissions PRIMARY KEY (id),
    CONSTRAINT unique_permissions_code UNIQUE (code)
);

CREATE TABLE public.physical_structures
(
    id             UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name           TEXT                                               NOT NULL,
    code           TEXT                                               NOT NULL,
    secondary_code TEXT,
    company_id     UUID                                               NOT NULL,
    district_id    UUID,
    extra          JSONB,
    disabled_at    TIMESTAMP WITH TIME ZONE,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_physical_structures PRIMARY KEY (id),
    CONSTRAINT unique_physical_structures_code UNIQUE (code)
);

CREATE TABLE public.production_factor_types
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    code        TEXT                                               NOT NULL,
    category    VARCHAR(100)                                       NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_production_factor_types PRIMARY KEY (id),
    CONSTRAINT unique_production_factor_types_name UNIQUE (name),
    CONSTRAINT unique_production_factor_types_code UNIQUE (code)
);

COMMENT ON COLUMN public.production_factor_types.category IS 'Enum values: POSTI_LETTO, POSTI_LETTO_OBI, POSTI_LETTO_EXTRA, SALE_OPERATORIE, POSTI_PAGANTI, ALTRA_STANZA_NO_SIO, ALTRO, STANZA, ALTRA_STANZA';

CREATE TABLE public.production_factors
(
    id                        UUID                     DEFAULT gen_random_uuid() NOT NULL,
    num_beds                  INTEGER,
    num_hospital_beds         INTEGER,
    room_name                 TEXT,
    room_code                 TEXT,
    production_factor_type_id UUID                                               NOT NULL,
    disabled_at               TIMESTAMP WITH TIME ZONE,
    created_at                TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at                TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_production_factors PRIMARY KEY (id)
);

CREATE TABLE public.provinces
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    acronym     TEXT                                               NOT NULL,
    istat_code  TEXT                                               NOT NULL,
    region_id   UUID                                               NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_provinces PRIMARY KEY (id),
    CONSTRAINT unique_provinces_name UNIQUE (name),
    CONSTRAINT unique_provinces_acronym UNIQUE (acronym),
    CONSTRAINT unique_provinces_istat_code UNIQUE (istat_code)
);

CREATE TABLE public.regions
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    istat_code  TEXT                                               NOT NULL,
    img_url     TEXT,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_regions PRIMARY KEY (id),
    CONSTRAINT unique_regions_name UNIQUE (name),
    CONSTRAINT unique_regions_istat_code UNIQUE (istat_code)
);

CREATE TABLE public.resolution_types
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_resolution_types PRIMARY KEY (id),
    CONSTRAINT unique_resolution_types_name UNIQUE (name)
);

CREATE TABLE public.resolutions
(
    id                   UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name                 TEXT                                               NOT NULL,
    category             VARCHAR(100)                                       NOT NULL,
    file_id              TEXT                                               NOT NULL,
    procedure_type       VARCHAR(100),
    number               TEXT,
    year                 INTEGER,
    valid_from           TIMESTAMP WITHOUT TIME ZONE                        NOT NULL,
    valid_to             TIMESTAMP WITHOUT TIME ZONE,
    bur_number           INTEGER,
    bur_date             TIMESTAMP WITHOUT TIME ZONE,
    dgr_link             TEXT,
    direction            TEXT,
    resolution_type_id   UUID                                               NOT NULL,
    parent_resolution_id UUID,
    company_id           UUID,
    disabled_at          TIMESTAMP WITH TIME ZONE,
    created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_resolutions PRIMARY KEY (id),
    CONSTRAINT unique_resolutions_name UNIQUE (name)
);

COMMENT ON COLUMN public.resolutions.category IS 'Enum values: UDO, GENERALE, PROGRAMMAZIONE, REQUISITI';

COMMENT ON COLUMN public.resolutions.procedure_type IS 'Enum values: AUTORIZZAZIONE, ACCREDITAMENTO, REVOCA_AUT, REVOCA_ACC';

CREATE TABLE public.specialties
(
    id                    UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name                  TEXT                                               NOT NULL,
    description           TEXT,
    record_type           VARCHAR(100)                                       NOT NULL,
    type                  VARCHAR(100),
    code                  TEXT                                               NOT NULL,
    is_used_in_cronos     BOOLEAN                                            NOT NULL,
    is_used_in_poa        BOOLEAN                                            NOT NULL,
    old_id                TEXT,
    grouping_specialty_id UUID,
    parent_specialty_id   UUID,
    disabled_at           TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_specialties PRIMARY KEY (id)
);

COMMENT ON COLUMN public.specialties.record_type IS 'Enum values: BRANCH, DISCIPLINE';

COMMENT ON COLUMN public.specialties.type IS 'Enum values: ALTRO, NON_OSPEDALIERO, OSPEDALIERO, TERRITORIALE';

CREATE TABLE public.toponyms
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_toponyms PRIMARY KEY (id),
    CONSTRAINT unique_toponyms_name UNIQUE (name)
);

CREATE TABLE public.udo_production_factors
(
    udo_id               UUID NOT NULL,
    production_factor_id UUID NOT NULL,
    CONSTRAINT pk_udo_production_factors PRIMARY KEY (udo_id, production_factor_id)
);

CREATE TABLE public.udo_specialties
(
    id                           UUID                     DEFAULT gen_random_uuid() NOT NULL,
    is_authorized                BOOLEAN                                            NOT NULL,
    is_accredited                BOOLEAN                                            NOT NULL,
    num_beds                     INTEGER,
    num_extra_beds               INTEGER,
    num_mortuary_beds            INTEGER,
    num_accredited_beds          INTEGER,
    hsp12                        TEXT,
    clinical_operational_unit_id UUID,
    clinical_poa_node_id         UUID,
    udo_id                       UUID                                               NOT NULL,
    specialty_id                 UUID                                               NOT NULL,
    disabled_at                  TIMESTAMP WITH TIME ZONE,
    created_at                   TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at                   TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_udo_specialties PRIMARY KEY (id)
);

CREATE TABLE public.udo_type_classifications
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_udo_type_classifications PRIMARY KEY (id),
    CONSTRAINT unique_udo_type_classifications_name UNIQUE (name)
);

CREATE TABLE public.udo_type_production_factor_types
(
    udo_type_id               UUID NOT NULL,
    production_factor_type_id UUID NOT NULL,
    CONSTRAINT pk_udo_type_production_factor_types PRIMARY KEY (udo_type_id, production_factor_type_id)
);

CREATE TABLE public.udo_types
(
    id                                             UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name                                           TEXT                                               NOT NULL,
    code                                           TEXT                                               NOT NULL,
    code_name                                      TEXT                                               NOT NULL,
    setting                                        TEXT,
    target                                         TEXT,
    is_hospital                                    BOOLEAN                                            NOT NULL,
    is_mental_health                               BOOLEAN                                            NOT NULL,
    has_beds                                       BOOLEAN                                            NOT NULL,
    scope_name                                     TEXT                                               NOT NULL,
    scope_description                              TEXT                                               NOT NULL,
    has_disciplines                                BOOLEAN                                            NOT NULL,
    has_disciplines_only_healthcare_company        BOOLEAN                                            NOT NULL,
    has_disciplines_only_public_or_private_company BOOLEAN                                            NOT NULL,
    has_branches                                   BOOLEAN                                            NOT NULL,
    has_branches_only_healthcare_company           BOOLEAN                                            NOT NULL,
    has_branches_only_public_or_private_company    BOOLEAN                                            NOT NULL,
    has_services                                   BOOLEAN                                            NOT NULL,
    has_scopes                                     BOOLEAN                                            NOT NULL,
    company_natures                                JSONB,
    ministerial_flows                              JSONB,
    udo_type_classification_id                     UUID,
    disabled_at                                    TIMESTAMP WITH TIME ZONE,
    created_at                                     TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at                                     TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_udo_types PRIMARY KEY (id),
    CONSTRAINT unique_udo_types_name UNIQUE (name),
    CONSTRAINT unique_udo_types_code UNIQUE (code),
    CONSTRAINT unique_udo_types_code_name UNIQUE (code_name)
);

CREATE TABLE public.udos
(
    id                            UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name                          TEXT                                               NOT NULL,
    code                          TEXT,
    status                        VARCHAR(100)                                       NOT NULL,
    floor                         TEXT,
    block                         TEXT,
    progressive                   TEXT,
    ministerial_code              TEXT,
    farfad_code                   TEXT,
    is_sio                        BOOLEAN                                            NOT NULL,
    starep_code                   TEXT,
    cost_center                   TEXT,
    keywords                      TEXT,
    notes                         TEXT,
    is_open_only_on_business_days BOOLEAN                                            NOT NULL,
    is_auac                       BOOLEAN                                            NOT NULL,
    is_module                     BOOLEAN                                            NOT NULL,
    organigram_node_id            UUID,
    udo_type_id                   UUID                                               NOT NULL,
    operational_office_id         UUID,
    building_id                   UUID,
    company_id                    UUID                                               NOT NULL,
    operational_unit_id           UUID,
    disabled_at                   TIMESTAMP WITH TIME ZONE,
    created_at                    TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at                    TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_udos PRIMARY KEY (id),
    CONSTRAINT unique_udos_code UNIQUE (code)
);

CREATE TABLE public.udos_history
(
    id                 UUID                     DEFAULT gen_random_uuid() NOT NULL,
    status             VARCHAR(100),
    num_beds           INTEGER,
    num_extra_beds     INTEGER,
    num_mortuary_beds  INTEGER,
    valid_from         date,
    valid_to           date,
    is_direct_supply   BOOLEAN                                            NOT NULL,
    is_indirect_supply BOOLEAN                                            NOT NULL,
    udo_id             UUID                                               NOT NULL,
    disabled_at        TIMESTAMP WITH TIME ZONE,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_udos_history PRIMARY KEY (id)
);

CREATE TABLE public.ulss
(
    id          UUID                     DEFAULT gen_random_uuid() NOT NULL,
    name        TEXT                                               NOT NULL,
    code        TEXT                                               NOT NULL,
    disabled_at TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_ulss PRIMARY KEY (id),
    CONSTRAINT unique_ulss_name UNIQUE (name),
    CONSTRAINT unique_ulss_code UNIQUE (code)
);

CREATE TABLE public.user_companies
(
    id                      UUID                     DEFAULT gen_random_uuid() NOT NULL,
    is_legal_representative BOOLEAN                                            NOT NULL,
    user_id                 UUID                                               NOT NULL,
    company_id              UUID                                               NOT NULL,
    disabled_at             TIMESTAMP WITH TIME ZONE,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_user_companies PRIMARY KEY (id)
);

CREATE TABLE public.user_company_permissions
(
    id            UUID                     DEFAULT gen_random_uuid() NOT NULL,
    user_id       UUID                                               NOT NULL,
    company_id    UUID,
    permission_id UUID                                               NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_user_company_permissions PRIMARY KEY (id)
);

CREATE TABLE public.users
(
    id                       UUID                     DEFAULT gen_random_uuid() NOT NULL,
    username                 TEXT                                               NOT NULL,
    role                     VARCHAR(100)                                       NOT NULL,
    first_name               TEXT                                               NOT NULL,
    last_name                TEXT                                               NOT NULL,
    tax_code                 TEXT                                               NOT NULL,
    email                    TEXT                                               NOT NULL,
    birth_date               date,
    birth_place              TEXT,
    street_name              TEXT,
    street_number            VARCHAR(20),
    phone                    TEXT,
    mobile_phone             TEXT,
    identity_doc_number      TEXT,
    identity_doc_expiry_date date,
    job                      TEXT,
    external_id              TEXT,
    operational_unit_id      UUID,
    disabled_at              TIMESTAMP WITH TIME ZONE,
    created_at               TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    updated_at               TIMESTAMP WITH TIME ZONE DEFAULT NOW()             NOT NULL,
    CONSTRAINT pk_users PRIMARY KEY (id),
    CONSTRAINT unique_users_username UNIQUE (username),
    CONSTRAINT unique_users_external_id UNIQUE (external_id)
);

COMMENT ON COLUMN public.users.role IS 'Enum values: ADMIN, REGIONAL_OPERATOR, OPERATOR';

ALTER TABLE public.buildings
    ADD CONSTRAINT fk_buildings_physical_structure_id FOREIGN KEY (physical_structure_id) REFERENCES public.physical_structures (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.companies
    ADD CONSTRAINT fk_companies_company_type_id FOREIGN KEY (company_type_id) REFERENCES public.company_types (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.companies
    ADD CONSTRAINT fk_companies_municipality_id FOREIGN KEY (municipality_id) REFERENCES public.municipalities (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.companies
    ADD CONSTRAINT fk_companies_toponym_id FOREIGN KEY (toponym_id) REFERENCES public.toponyms (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.municipalities
    ADD CONSTRAINT fk_municipalities_province_id FOREIGN KEY (province_id) REFERENCES public.provinces (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.operational_offices
    ADD CONSTRAINT fk_operational_offices_physical_structure_id FOREIGN KEY (physical_structure_id) REFERENCES public.physical_structures (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.operational_offices
    ADD CONSTRAINT fk_operational_offices_toponym_id FOREIGN KEY (toponym_id) REFERENCES public.toponyms (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.operational_offices
    ADD CONSTRAINT fk_operational_offices_municipality_id FOREIGN KEY (municipality_id) REFERENCES public.municipalities (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.operational_units
    ADD CONSTRAINT fk_operational_units_company_id FOREIGN KEY (company_id) REFERENCES public.companies (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.physical_structures
    ADD CONSTRAINT fk_physical_structures_company_id FOREIGN KEY (company_id) REFERENCES public.companies (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.physical_structures
    ADD CONSTRAINT fk_physical_structures_district_id FOREIGN KEY (district_id) REFERENCES public.districts (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.production_factors
    ADD CONSTRAINT fk_production_factors_production_factor_type_id FOREIGN KEY (production_factor_type_id) REFERENCES public.production_factor_types (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.provinces
    ADD CONSTRAINT fk_provinces_region_id FOREIGN KEY (region_id) REFERENCES public.regions (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.resolutions
    ADD CONSTRAINT fk_resolutions_company_id FOREIGN KEY (company_id) REFERENCES public.companies (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.resolutions
    ADD CONSTRAINT fk_resolutions_parent_resolution_id FOREIGN KEY (parent_resolution_id) REFERENCES public.resolutions (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.resolutions
    ADD CONSTRAINT fk_resolutions_resolution_type_id FOREIGN KEY (resolution_type_id) REFERENCES public.resolution_types (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.specialties
    ADD CONSTRAINT fk_specialties_grouping_specialty_id FOREIGN KEY (grouping_specialty_id) REFERENCES public.grouping_specialties (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.specialties
    ADD CONSTRAINT fk_specialties_parent_specialty_id FOREIGN KEY (parent_specialty_id) REFERENCES public.specialties (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udo_production_factors
    ADD CONSTRAINT fk_udo_production_factors_production_factor_id FOREIGN KEY (production_factor_id) REFERENCES public.production_factors (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udo_production_factors
    ADD CONSTRAINT fk_udo_production_factors_udo_id FOREIGN KEY (udo_id) REFERENCES public.udos (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udo_specialties
    ADD CONSTRAINT fk_udo_specialties_specialty_id FOREIGN KEY (specialty_id) REFERENCES public.specialties (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udo_specialties
    ADD CONSTRAINT fk_udo_specialties_udo_id FOREIGN KEY (udo_id) REFERENCES public.udos (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udo_type_production_factor_types
    ADD CONSTRAINT fk_udo_type_production_factor_types_production_factor_type_id FOREIGN KEY (production_factor_type_id) REFERENCES public.production_factor_types (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udo_type_production_factor_types
    ADD CONSTRAINT fk_udo_type_production_factor_types_udo_type_id FOREIGN KEY (udo_type_id) REFERENCES public.udo_types (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udo_types
    ADD CONSTRAINT fk_udo_types_udo_type_classification_id FOREIGN KEY (udo_type_classification_id) REFERENCES public.udo_type_classifications (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udos
    ADD CONSTRAINT fk_udos_udo_type_id FOREIGN KEY (udo_type_id) REFERENCES public.udo_types (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udos
    ADD CONSTRAINT fk_udos_operational_office_id FOREIGN KEY (operational_office_id) REFERENCES public.operational_offices (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udos
    ADD CONSTRAINT fk_udos_building_id FOREIGN KEY (building_id) REFERENCES public.buildings (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udos
    ADD CONSTRAINT fk_udos_company_id FOREIGN KEY (company_id) REFERENCES public.companies (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udos
    ADD CONSTRAINT fk_udos_operational_unit_id FOREIGN KEY (operational_unit_id) REFERENCES public.operational_units (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.udos_history
    ADD CONSTRAINT fk_udos_history_udo_id FOREIGN KEY (udo_id) REFERENCES public.udos (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.user_companies
    ADD CONSTRAINT fk_user_companies_company_id FOREIGN KEY (company_id) REFERENCES public.companies (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.user_companies
    ADD CONSTRAINT fk_user_companies_user_id FOREIGN KEY (user_id) REFERENCES public.users (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.users
    ADD CONSTRAINT fk_users_operational_unit_id FOREIGN KEY (operational_unit_id) REFERENCES public.operational_units (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.user_company_permissions
    ADD CONSTRAINT fk_user_company_permissions_company_id FOREIGN KEY (company_id) REFERENCES public.companies (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.user_company_permissions
    ADD CONSTRAINT fk_user_company_permissions_permission_id FOREIGN KEY (permission_id) REFERENCES public.permissions (id) ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE public.user_company_permissions
    ADD CONSTRAINT fk_user_company_permissions_user_id FOREIGN KEY (user_id) REFERENCES public.users (id) ON UPDATE NO ACTION ON DELETE NO ACTION;
