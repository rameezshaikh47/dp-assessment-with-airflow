-- Staging layer, raw data without any transformations will be stored in this schema
CREATE SCHEMA IF NOT EXISTS staging;

--
CREATE SCHEMA IF NOT EXISTS processed;

--
CREATE SCHEMA IF NOT EXISTS golden;

--
CREATE SCHEMA IF NOT EXISTS dwh_audit;


-- Staging Table
CREATE TABLE IF NOT EXISTS staging.temperature_readings_stg (
    id TEXT,
    room_id TEXT,
    noted_date TEXT,
    temp TEXT,
    out_or_in TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file_name TEXT
);

-- processed table
CREATE TABLE IF NOT EXISTS processed.temperature_readings (
    id TEXT,
    room_id TEXT,
    noted_date TEXT,
    temp TEXT,
    out_or_in TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    validation_message TEXT,
    source_file_name TEXT,
    load_status TEXT DEFAULT 'Processing',
    current_run BOOLEAN DEFAULT True
);

-- golden table
CREATE TABLE IF NOT EXISTS golden.temperature_readings (
    id TEXT PRIMARY KEY,
    room_id VARCHAR(255) NOT NULL,
    noted_date TIMESTAMP NOT NULL,
    temp NUMERIC(5, 2) NOT NULL,
    out_or_in VARCHAR(10) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS dwh_audit.rejected_files_log (
    file_name TEXT,
    reason TEXT,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE OR REPLACE FUNCTION processed.validate_timestamp(input TEXT) RETURNS TIMESTAMP AS $$
BEGIN
    BEGIN
        RETURN to_timestamp(input, 'DD-MM-YYYY HH24:MI');
    EXCEPTION WHEN others THEN
        RETURN NULL;
    END;
END;
$$ LANGUAGE plpgsql;
