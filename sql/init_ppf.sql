\c area_ppf_db

CREATE TABLE prescriptions
(
    id                   UUID                     NOT NULL DEFAULT gen_random_uuid(),
    description          TEXT                     NOT NULL,
    -- ENUM VALUE: IN_BOZZA, AUTORIZZATO, CHIUSA
    state                TEXT                     NOT NULL,
    -- ENUM VALUE: COLLETTIVA_APERTA,INDIVIDUALE_APERTA,INDIVIDUALE_CHIUSA,COLLETTIVA_MISTA
    criteria             TEXT                     NOT NULL,
    clinical_indications TEXT,
    annotation           TEXT,
    version              numeric                  NOT NULL,
    resolution_id        UUID,
    user_creator_id      UUID,
    user_last_mod_id     UUID,
    extra                JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at          TIMESTAMP WITH TIME ZONE,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_prescriptions PRIMARY KEY (id)
);

CREATE TABLE prescription_pharmacy_data
(
    prescription_id  UUID NOT NULL,
    pharmacy_data_id UUID NOT NULL,
    CONSTRAINT pk_prescription_pharmacy_data PRIMARY KEY (prescription_id, pharmacy_data_id)
);

CREATE TABLE prescription_specializations
(
    prescription_id   UUID NOT NULL,
    specialization_id TEXT NOT NULL,
    CONSTRAINT pk_prescription_specializations PRIMARY KEY (prescription_id, specialization_id)
);

CREATE TABLE prescription_specialties
(
    prescription_id UUID NOT NULL,
    specialty_id    UUID NOT NULL,
    CONSTRAINT pk_prescription_specialties PRIMARY KEY (prescription_id, specialty_id)
);

CREATE TABLE prescription_organigram_nodes
(
    prescription_id UUID NOT NULL,
    node_id         UUID NOT NULL,
    CONSTRAINT pk_prescription_organigram_nodes PRIMARY KEY (prescription_id, node_id)
);

CREATE TABLE prescription_hr_pers
(
    prescription_id UUID NOT NULL,
    hr_pers_id      TEXT NOT NULL,
    group_type      TEXT NOT NULL,
    CONSTRAINT pk_prescription_hr_pers PRIMARY KEY (prescription_id, hr_pers_id, group_type)
);

CREATE TABLE prescription_histories
(
    id               UUID                     NOT NULL DEFAULT gen_random_uuid(),
    event            TEXT                     NOT NULL,
    version          INTEGER                  NOT NULL,
    prescription_id  UUID                     NOT NULL,
    user_creator_id  UUID,
    user_last_mod_id UUID,
    extra            JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at      TIMESTAMP WITH TIME ZONE,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_prescription_histories PRIMARY KEY (id)
);

CREATE TABLE integration_requests
(
    id               UUID                     NOT NULL DEFAULT gen_random_uuid(),
    is_autorized     BOOLEAN,
    is_rejected      BOOLEAN,
    annotations      TEXT,
    prescription_id  UUID                     NOT NULL,
    user_creator_id  UUID,
    user_last_mod_id UUID,
    extra            JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at      TIMESTAMP WITH TIME ZONE,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_integration_requests PRIMARY KEY (id)
);

CREATE TABLE integration_request_hr_pers
(
    integration_request_id UUID NOT NULL,
    hr_pers_id             TEXT NOT NULL,
    CONSTRAINT pk_integration_request_hr_pers PRIMARY KEY (integration_request_id, hr_pers_id)
);

CREATE TABLE prescr_hr_org_node_inbox
(
    id                       UUID                     NOT NULL DEFAULT gen_random_uuid(),
    prescription_id          UUID                     NOT NULL,
    organigram_node_id       UUID                     NOT NULL,
    hr_pers_id               TEXT                     NOT NULL,
    is_authorized_company    BOOLEAN,
    is_authorized_regionally BOOLEAN,
    is_ignored               BOOLEAN,
    extra                    JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at              TIMESTAMP WITH TIME ZONE,
    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_prescr_hr_org_node_inbox PRIMARY KEY (id)
);

CREATE TABLE prescr_hr_specialization_inbox
(
    id                       UUID                     NOT NULL DEFAULT gen_random_uuid(),
    prescription_id          UUID                     NOT NULL,
    hr_pers_id               TEXT                     NOT NULL,
    is_authorized_company    BOOLEAN,
    is_authorized_regionally BOOLEAN,
    is_ignored               BOOLEAN,
    extra                    JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    disabled_at              TIMESTAMP WITH TIME ZONE,
    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT pk_prescr_hr_specialization_inbox PRIMARY KEY (id)
);

CREATE TABLE specialization_hr_inbox
(
    prescr_hr_specialization_inbox_id UUID NOT NULL,
    specialization_id                 TEXT NOT NULL,
    CONSTRAINT pk_specialization_hr_inbox PRIMARY KEY (prescr_hr_specialization_inbox_id, specialization_id)
);


-- Foreign key constraints
ALTER TABLE prescription_hr_pers
    ADD CONSTRAINT fk_prescription_hr_pers_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE prescription_pharmacy_data
    ADD CONSTRAINT fk_prescription_pharmacy_data_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE prescription_specializations
    ADD CONSTRAINT fk_prescription_specializations_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE prescription_specialties
    ADD CONSTRAINT fk_prescription_specialties_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE prescription_organigram_nodes
    ADD CONSTRAINT fk_prescription_organigram_nodes_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE prescription_histories
    ADD CONSTRAINT fk_prescription_histories_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE integration_requests
    ADD CONSTRAINT fk_integration_requests_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE integration_request_hr_pers
    ADD CONSTRAINT fk_integration_request_hr_pers_integration_request_id FOREIGN KEY (integration_request_id)
        REFERENCES integration_requests (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE prescr_hr_org_node_inbox
    ADD CONSTRAINT fk_prescr_hr_org_node_inbox_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE prescr_hr_specialization_inbox
    ADD CONSTRAINT fk_prescr_hr_specialization_inbox_prescription_id FOREIGN KEY (prescription_id)
        REFERENCES prescriptions (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

ALTER TABLE specialization_hr_inbox
    ADD CONSTRAINT fk_specialization_hr_inbox_prescription_id FOREIGN KEY (prescr_hr_specialization_inbox_id)
        REFERENCES prescr_hr_specialization_inbox (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;