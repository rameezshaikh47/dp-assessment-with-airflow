SELECT validation_message, count(1)
FROM {{SCHEMA_PROCESSED}}.{{PROCESSED_TABLE}}
WHERE current_run=True
GROUP BY validation_message