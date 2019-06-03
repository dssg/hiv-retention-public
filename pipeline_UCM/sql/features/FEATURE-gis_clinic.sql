DROP TABLE IF EXISTS features_cs.gis_toclinic CASCADE;

CREATE TABLE features_cs.gis_toclinic AS
WITH first_appt AS (
SELECT DISTINCT ON(entity_id)
       entity_id,
       date_col
FROM
    features_cs.appt
ORDER BY
      entity_id, date_col
)

SELECT
    patients_ucm.main.entity_id,
    date_col as knowledge_date_col,
    total_travel_time_public,
    crime_rate,
    total_length_miles,
    total_time_car
FROM
    raw.gis_clinic
JOIN
    patients_ucm.main
ON
    raw.gis_clinic.mrn = patients_ucm.main.patient_id
JOIN
    first_appt
ON
    patients_ucm.main.entity_id = first_appt.entity_id
WHERE
    total_travel_time_public IS NOT NULL
    AND crime_rate IS NOT NULL
    AND total_length_miles IS NOT NULL
    AND total_time_car IS NOT NULL;

CREATE INDEX ON features_cs.gis_toclinic(entity_id);
CREATE INDEX ON features_cs.gis_toclinic(knowledge_date_col);
CREATE INDEX ON features_cs.gis_toclinic(entity_id,knowledge_date_col);
