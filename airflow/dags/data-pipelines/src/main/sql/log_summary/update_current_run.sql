UPDATE {{SCHEMA_PROCESSED}}.{{PROCESSED_TABLE}}
SET current_run=False
WHERE current_run=True