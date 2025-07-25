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