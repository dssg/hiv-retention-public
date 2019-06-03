CREATE SCHEMA IF NOT EXISTS cleaned_cdph;




DROP TABLE IF EXISTS cleaned_cdph.baseline CASCADE;

CREATE TABLE cleaned_cdph.baseline AS
  WITH diagnosis as (
      SELECT "EHARSID"                                    AS eharsid,
             "DOCUMENTID"                                 AS documentid,
             birth_sex,
             race1,
             trans_categ,
             cur_city_fips,
             rsd_city_fips,
             cur_city_name,
             cur_state_cd,
             '1960-01-01' :: date + "DOB" :: int          AS birth_date,
             '1960-01-01' :: date + "HIV_DX_DT" :: int    AS hiv_dx_date,
             '1960-01-01' :: date + "PHYS_HIV_DT" :: int  AS phys_hiv_date,
             '1960-01-01' :: date + "DEATH_REP_DT" :: int AS death_date
      FROM raw_cdph.person
      WHERE cur_state_cd = 'IL' and (cur_city_name = 'CHICAGO' or cur_city_fips = '14000')),

      labs as (
      --Grab Lab Tests
        SELECT lab_test_cd,
               result,
               result_units,
               comments,
               "RESULT_RPT_DT" AS result_rpt_date,
               "DOCUMENTID"    AS documentid
        FROM raw_cdph.lab_with_dates
        WHERE lab_test_cd like 'EC-%'
          AND lab_test_cd is not NULL
          AND "RESULT_RPT_DT" is not NULL
    ),

      prestarttime as (
        SELECT a.eharsid,
               a.documentid,
               a.birth_sex,
               a.race1 as race,
               a.trans_categ as transmission_category,
               a.cur_city_fips,
               a.rsd_city_fips,
               a.birth_date,
               a.hiv_dx_date,
               a.phys_hiv_date,
               a.death_date,
               b.lab_test_cd,
               b.result,
               b.result_units,
               b.comments,
               b.result_rpt_date,
               min(result_rpt_date) over (PARTITION BY eharsid) AS first_lab

        FROM diagnosis a
               LEFT JOIN labs b USING (documentid)
    )

  select distinct on (eharsid)
                     eharsid,
                     documentid,
                     birth_sex,
                     race,
                     transmission_category,
                     cur_city_fips,
                     rsd_city_fips,
                     birth_date,
                     hiv_dx_date,
                     phys_hiv_date,
                     death_date,
                     lab_test_cd,
                     result,
                     result_units,
                     comments,
                     result_rpt_date,
                     first_lab,
                     case
                       when hiv_dx_date is not NULL THEN hiv_dx_date
                       when hiv_dx_date is NULL and first_lab is not NULL THEN first_lab
                       else birth_date end as start_time,
                     case
                       when death_date is null then '2048-12-31' :: date
                       else death_date end as end_time

  from prestarttime;