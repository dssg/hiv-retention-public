DROP SCHEMA IF EXISTS patients_cdph CASCADE;
CREATE SCHEMA patients_cdph;

DROP SCHEMA IF EXISTS events_cdph CASCADE;
CREATE SCHEMA events_cdph;

DROP SCHEMA IF EXISTS lookup_cdph CASCADE;
CREATE SCHEMA lookup_cdph;

DROP TYPE IF EXISTS gender_enum;
DROP TYPE IF EXISTS name_type_enum;

CREATE TYPE gender_enum AS ENUM('male', 'female', 'transgender male', 'transgender female', 'other');
CREATE TYPE name_type_enum AS ENUM ('birth', 'marital', 'maiden', 'changed'); -- TODO: check these types
