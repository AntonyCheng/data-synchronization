SET timezone = 'UTC';

CREATE SCHEMA IF NOT EXISTS poc_meta;

CREATE TABLE IF NOT EXISTS poc_meta.environment_marker (
    environment_name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO poc_meta.environment_marker(environment_name)
VALUES ('data-sync-poc')
ON CONFLICT (environment_name) DO NOTHING;
