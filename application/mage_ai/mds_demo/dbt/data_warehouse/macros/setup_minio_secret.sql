{% macro setup_minio_secret() %}

DO $$
DECLARE
    v_has_secret BOOLEAN := FALSE;
BEGIN
    -- Only create the secret if no S3 secret exists yet.
    -- create_simple_secret() stores persistently in PostgreSQL and is visible
    -- across all connections, so this only needs to run once per database lifetime.
    SELECT EXISTS (
        SELECT 1
        FROM duckdb.query('SELECT type FROM duckdb_secrets()') AS r
        WHERE r['type']::TEXT = 's3'
    ) INTO v_has_secret;

    IF NOT v_has_secret THEN
        PERFORM duckdb.create_simple_secret(
            's3',
            '{{ env_var("MINIO_ROOT_USER", "admin") }}',
            '{{ env_var("MINIO_ROOT_PASSWORD", "admin123") }}',
            '',                                           -- session_token
            'us-east-1',                                 -- region
            'path',                                      -- url_style
            '',                                          -- provider
            '{{ env_var("MINIO_HOST", "minio") }}:9000', -- endpoint
            '',                                          -- scope
            '',                                          -- validation
            'false'                                      -- use_ssl
        );
    END IF;
END;
$$

{% endmacro %}
