-- Sequences
CREATE SEQUENCE sq_procedures_progressive_code AS integer;

-- Tables
CREATE TABLE attachment_types
(
    id           UUID                     NOT NULL DEFAULT gen_random_uuid(),
    name         TEXT                     NOT NULL,
    is_mandatory BOOLEAN                  NOT NULL DEFAULT FALSE,
    extra        JSONB,
    disabled_at  TIMESTAMP WITH TIME ZONE,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_attachment_types PRIMARY KEY (id)
);

CREATE TABLE procedure_attachments
(
    id                              UUID                     NOT NULL DEFAULT gen_random_uuid(),
    procedure_id                    UUID                     NOT NULL,
    file_id                         UUID                     NOT NULL,
    file_name                       TEXT                     NOT NULL,
    object                          TEXT,
    attachment_type_id              UUID                     NOT NULL,
    attachment_user_id              UUID,
    attachment_user_last_first_name TEXT,
    attachment_created_at           TIMESTAMP WITH TIME ZONE,
    extra                           JSONB,
    created_at                      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    disabled_at                     TIMESTAMP WITH TIME ZONE,
    CONSTRAINT pk_procedure_attachments PRIMARY KEY (id)
);

CREATE TABLE procedure_entities
(
    id                           UUID                     NOT NULL DEFAULT gen_random_uuid(),
    procedure_id                 UUID                     NOT NULL,
    object_type                  TEXT                     NOT NULL,
    object_reference_type        TEXT                     NOT NULL,
    object_reference_id          UUID                     NOT NULL,
    object_data                  JSONB                    NOT NULL,
    district_id                  UUID,
    udo_type_id                  UUID,
    udo_type_classification_id   UUID,
    is_udo_type_mental_health    BOOLEAN,
    operational_unit_id          UUID,
    organigram_node_id           UUID,
    operational_office_id        UUID,
    building_id                  UUID,
    physical_structure_id        UUID,
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

CREATE TABLE procedure_notes
(
    id           UUID                     NOT NULL DEFAULT gen_random_uuid(),
    procedure_id UUID                     NOT NULL,
    title        TEXT                     NOT NULL,
    description  TEXT,
    extra        JSONB,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_procedure_notes PRIMARY KEY (id)
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
ALTER TABLE procedure_attachments
    ADD CONSTRAINT fk_procedure_attachments_attachment_type_id FOREIGN KEY (attachment_type_id)
        REFERENCES attachment_types (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

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

ALTER TABLE procedure_notes
    ADD CONSTRAINT fk_procedure_note_procedures_id FOREIGN KEY (procedure_id)
        REFERENCES procedures (id)
        ON UPDATE NO ACTION
        ON DELETE CASCADE;

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

-- Seed
INSERT INTO attachment_types (id, name, is_mandatory)
VALUES ('B66C1E6C-B729-1B6B-E053-0100007F7D21', 'Planimetria Locali', True),
       ('B66C1E6C-B72A-1B6B-E053-0100007F7D21', 'Attestato Abitabilità Locali', True),
       ('B66C1E6C-B72E-1B6B-E053-0100007F7D21', 'Dichiarazione Accettazione Incarico Direttore Sanitario', True),
       ('B66C1E6C-B72F-1B6B-E053-0100007F7D21', 'Elenco Prestazioni Sanitarie con Indicazione Orari Apertura', True),
       ('B66C1E6C-B730-1B6B-E053-0100007F7D21', 'Elenco Personale con Relative Qualifiche e Titoli Professionali',
        True),
       ('B66C1E6C-B731-1B6B-E053-0100007F7D21', 'Elenco Attrezzature e Relativi Piani Manutenzione', True),
       ('B66C1E6C-B732-1B6B-E053-0100007F7D21',
        'Regolamento Sanitario Interno (Modalità Ammissione e Norme Funzionamento Servizi)', True),
       ('B66C1E6C-B733-1B6B-E053-0100007F7D21', 'Dati Relativi Imposta di Bollo', True),
       ('B66C1E6C-B734-1B6B-E053-0100007F7D21', 'Prestazioni e Attività Effettuate', True),
       ('B66C1E6C-B735-1B6B-E053-0100007F7D21', 'Prestazioni e Attività Previste', False),
       ('B66C1E6C-B736-1B6B-E053-0100007F7D21',
        'Documentazione Possesso Titoli Professionali(Professionista e Personale)', True),
       ('B66C1E6C-B737-1B6B-E053-0100007F7D21', 'Documentazione Formazione e Aggiornamento', True),
       ('B66C1E6C-B738-1B6B-E053-0100007F7D21', 'Risorse Umane - Dati Personale Incaricato', False),
       ('B66C1E6C-B739-1B6B-E053-0100007F7D21', 'Documentazione Politica / Mission / Vision Struttura Sanitaria',
        False),
       ('B66C1E6C-B73A-1B6B-E053-0100007F7D21', 'Documentazione Obiettivi Unità Operativo-Funzionale', False),
       ('B66C1E6C-B73B-1B6B-E053-0100007F7D21', 'Elenco Procedure Unità Operativo-Funzionale', False),
       ('B66C1E6C-B73C-1B6B-E053-0100007F7D21', 'Elenco Attrezzature in Dotazione', True),
       ('B66C1E6C-B73D-1B6B-E053-0100007F7D21', 'Piano Manutenzione Attrezzature', True),
       ('B66C1E6C-B73E-1B6B-E053-0100007F7D21', 'Eventuali Funzioni/Specializzazioni/Situazioni Peculiari', False),
       ('B66C1E6C-B73F-1B6B-E053-0100007F7D21', 'Eventuali Riconoscimenti', False),
       ('B66C1E6C-B741-1B6B-E053-0100007F7D21',
        'Elenco Personale con Qualifiche ed Estremi Iscrizione Albo/Collegio Professionale', True),
       ('B66C1E6C-B742-1B6B-E053-0100007F7D21', 'Carta dei Servizi (Modalità Ammissione e Norme Funzionamento Servizi)',
        True),
       ('BBE9C4B2-0440-2B44-E053-0100007FF314', 'Documentazione Prevenzione Incendi', True),
       ('BBE9C4B2-0441-2B44-E053-0100007FF314',
        'Documento Conformità Impianto Elettrico, Impianto Distribuzione Gas Medicali, Impianto CLimatizzazione/Trattamento Aria Immessa',
        True),
       ('BBE9C4B2-0442-2B44-E053-0100007FF314', 'Documento Identità Direttore Sanitario', True),
       ('BBE9C4B2-0443-2B44-E053-0100007FF314', 'Titolo di Studio Direttore Sanitario', True),
       ('5a4c8f4c-c288-405a-974d-595692053a13', 'Planimetrie', False),
       ('641dce9e-8a03-4fff-b3a3-b41e06293dfb', 'Piano Di Adeguamento', False),
       ('7c0b5069-cd33-42b5-82b5-c6143e1f6881', 'Oneri', False),
       ('85d7b6d8-d14c-4269-9a46-45818e2eadae', 'Relazione Attivita', False),
       ('f7580519-7c18-41bf-9dcc-463025ba7a86', 'Dichiarazione Di Incompatibilita', False),
       ('e29e8245-825c-4526-8a1c-5f32b04bdeec', 'Certificato Casellario', False),
       ('7fd601be-e40e-4fc1-89dc-77337b0ee294', 'Autorizzazione Alla Realizzazione', False),
       ('57f935c3-7d18-4935-ac86-f946fdba11b7', 'Istanza', False),
       ('79a84537-938c-4df8-86cf-f12b366eb3b3', 'Lettera Di Trasmissione', False),
       ('b3fc7593-4543-4a7f-971c-3d17643e5966', 'Relazione Conclusiva', False),
       ('7a33765c-b814-4bb7-b2ec-ec1b006a0cc8', 'Altro', False);
