DROP TABLE IF EXISTS staging.eharsid_documentid_entityid CASCADE;

CREATE TABLE staging.eharsid_documentid_entityid as
SELECT
    a."EHARSID",
    a."DOCUMENTID",
    b.entity_id
FROM
    raw_cdph.document as a
LEFT JOIN
     patients_cdph.main as b
ON
    a."EHARSID" = b.patient_id;

CREATE INDEX ON staging.eharsid_documentid_entityid(entity_id);
CREATE INDEX ON staging.eharsid_documentid_entityid("EHARSID");
