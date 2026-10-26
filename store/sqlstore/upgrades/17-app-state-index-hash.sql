-- v17 (compatible with v8+): Remember the logical index of app state mutation MACs
-- IF NOT EXISTS (postgres): many workers share one database and upgrade concurrently on
-- rollout, and large deployments pre-build the index with CREATE INDEX CONCURRENTLY so
-- this upgrade doesn't block app state writes while it builds.
-- only: postgres
ALTER TABLE whatsmeow_app_state_mutation_macs ADD COLUMN IF NOT EXISTS index_hash bytea;
-- only: sqlite
ALTER TABLE whatsmeow_app_state_mutation_macs ADD COLUMN index_hash bytea;
CREATE INDEX IF NOT EXISTS whatsmeow_app_state_mutation_macs_index_hash_idx ON whatsmeow_app_state_mutation_macs (jid, name, index_hash);
