SELECT load_status, count(1)
FROM {{SCHEMA_PROCESSED}}.{{PROCESSED_TABLE}}
WHERE current_run=True
GROUP BY load_status;