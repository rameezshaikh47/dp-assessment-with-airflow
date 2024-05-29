BEGIN;
INSERT INTO {{params.SCHEMA_PROCESSED}}.{{params.PROCESSED_TABLE}}
    (id, room_id, noted_date, temp, out_or_in, load_timestamp, validation_message, source_file_name, load_status)
    SELECT
        id
        , room_id
        , noted_date
        , temp
        , out_or_in
        , current_timestamp
        , CASE
            WHEN id IS NULL THEN 'id is NULL'
            WHEN room_id IS NULL THEN 'room_id is NULL'
            WHEN noted_date IS NULL THEN 'noted_date in NULL'
            WHEN temp IS NULL THEN 'temp is NULL'
            WHEN out_or_in IS NULL THEN 'out_or_in is NULL'
            WHEN processed.validate_timestamp(noted_date) IS NULL THEN 'Invalid noted_date format'
            WHEN temp::NUMERIC(5, 2) IS NULL THEN 'Invalid temp format'
            WHEN out_or_in NOT IN ('In', 'Out') THEN 'Invalid out_or_in value'
            WHEN processed.validate_timestamp(noted_date) > CURRENT_TIMESTAMP THEN 'Future timestamp'
        ELSE 'Valid record'
        END AS validation_message
        , source_file_name
        , CASE
            -- WHEN id IN (SELECT id FROM {SCHEMA_GOLDEN}.{GOLDEN_TABLE}) THEN 'Reprocessed'
            WHEN id IS NULL OR room_id IS NULL OR processed.validate_timestamp(noted_date) IS NULL
                OR temp::NUMERIC(5, 2) IS NULL OR out_or_in NOT IN ('In', 'Out')
                OR temp IS NULL OR noted_date IS NULL OR out_or_in IS NULL
                OR processed.validate_timestamp(noted_date) > CURRENT_TIMESTAMP THEN 'Error'
        ELSE 'Processing'
        END AS load_status
    FROM {{params.SCHEMA_STAGING}}.{{params.STAGING_TABLE}};
-- Identify reprocessed records
UPDATE {{params.SCHEMA_PROCESSED}}.{{params.PROCESSED_TABLE}}
SET load_status='Reprocessed'
WHERE id IN (SELECT id FROM {{params.SCHEMA_GOLDEN}}.{{params.GOLDEN_TABLE}})
AND load_status='Processing';

-- Check for Duplicate ID's
UPDATE {{params.SCHEMA_PROCESSED}}.{{params.PROCESSED_TABLE}}
SET load_status='Duplicate'
WHERE id in (SELECT id
             FROM {{params.SCHEMA_STAGING}}.{{params.STAGING_TABLE}}
             GROUP BY id
             HAVING COUNT(1) > 1)
AND load_status='Processing';
END;