DROP TABLE IF EXISTS hiv1_labs CASCADE;
CREATE TEMP TABLE hiv1_labs AS
     SELECT
        entity_id,
        "RESULT_RPT_DT"::date as lab_date
     FROM
        raw_cdph.lab_with_dates
     JOIN
        raw_cdph.document USING ("DOCUMENTID")
     JOIN
        patients_cdph.main
     ON
        "EHARSID" = patient_id
     WHERE
        lab_test_cd LIKE 'EC-%' --all HIV-1 tests
        AND lab_test_cd IS NOT NULL
        AND "RESULT_RPT_DT" IS NOT NULL
        AND "RESULT_RPT_DT" > '01-01-2040'::date;
CREATE INDEX ON hiv1_labs(entity_id);
CREATE INDEX ON hiv1_labs(lab_date);


DROP TABLE IF EXISTS all_days_labs CASCADE;
CREATE TEMP TABLE all_days_labs AS (
      SELECT
        entity_id,
        generate_series(first_lab,last_lab,'1day') as days
      FROM
        (
            SELECT
                entity_id,
                min(lab_date) as first_lab,
                '03-03-2047'::date as last_lab
             FROM
                hiv1_labs
                GROUP BY entity_id) as a
       ORDER BY
             entity_id,
             days
);
CREATE INDEX ON all_days_labs(entity_id);
CREATE INDEX ON all_days_labs(days);


DROP TABLE IF EXISTS events_cdph_labs CASCADE;
CREATE TABLE IF NOT EXISTS events_cdph_labs AS
SELECT
    x.entity_id,
    x.days AS outcome_date,
    CASE
        WHEN max(y.lab_date) - min(y.lab_date) >= 90 THEN false
        ELSE true
    END AS outcome
FROM
    all_days_labs AS x
JOIN
    hiv1_labs y USING (entity_id)
WHERE
    y.lab_date BETWEEN x.days AND x.days + '12 months'::interval - '1day'::interval
GROUP BY
      x.entity_id,
      x.days;

CREATE INDEX IF NOT EXISTS ON events_cdph_labs(entity_id);
CREATE INDEX IF NOT EXISTS ON events_cdph_labs(outcome_date);

--CREATE STATES TABLE QUARTERLY
DROP TABLE IF EXISTS states_cdph_quarter CASCADE;
CREATE TABLE states_cdph_quarter AS
WITH
prediction_dates AS(
      SELECT generate_series as d
      FROM generate_series('01-01-2040'::date, '06-01-2047'::date,'3months')

)
SELECT
    DISTINCT entity_id,
    outcome_date AS start_time,
    (outcome_date + interval '1day')::date AS end_time,
    'active' AS state
FROM
    events_cdph_labs
JOIN
    prediction_dates
ON
    outcome_date = d;
