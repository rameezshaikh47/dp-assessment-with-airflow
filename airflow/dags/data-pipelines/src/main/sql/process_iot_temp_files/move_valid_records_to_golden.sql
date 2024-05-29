BEGIN;
-- Insert New Records
INSERT INTO {{params.SCHEMA_GOLDEN}}.{{params.GOLDEN_TABLE}} (id, room_id, noted_date, temp, out_or_in, load_timestamp)
SELECT
    id ,
    room_id,
    TO_TIMESTAMP(noted_date, 'DD-MM-YYYY HH24:MI'),
    temp::NUMERIC(5, 2),
    out_or_in,
    current_timestamp
FROM {{params.SCHEMA_PROCESSED}}.{{params.PROCESSED_TABLE}}
WHERE load_status = 'Processing';

-- Update exiting Records
UPDATE {{params.SCHEMA_GOLDEN}}.{{params.GOLDEN_TABLE}} g
SET
    room_id = p.room_id,
    noted_date = TO_TIMESTAMP(p.noted_date, 'DD-MM-YYYY HH24:MI'),
    temp = p.temp::NUMERIC(5, 2),
    out_or_in = p.out_or_in,
    load_timestamp = current_timestamp
FROM {{params.SCHEMA_PROCESSED}}.{{params.PROCESSED_TABLE}} p
WHERE g.id = p.id AND p.load_status = 'Reprocessed';

-- Mark records as processed in Processing layer
UPDATE {{params.SCHEMA_PROCESSED}}.{{params.PROCESSED_TABLE}}
SET load_status = 'Processed'
WHERE load_status in ('Processing', 'Reprocessed');

COMMIT;