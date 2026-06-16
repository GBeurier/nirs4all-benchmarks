-- nirs4all-benchmarks (the Arena) — arena.sqlite schema.
--
-- Dimensions + facts, all keyed by the content hashes of identity/hashing.py
-- (DATA_MANAGEMENT.md §2). Dedup tables collapse by key on insert because every
-- identity is a content hash (DESIGN.md §7.4). Every dimension carries the engine
-- fingerprint it was derived from, so identity provenance is auditable and drift
-- detectable (DATA_MANAGEMENT.md §3). NO artifacts are ever stored.
--
-- The store stamps PRAGMA user_version with ARENA_SCHEMA_VERSION.

PRAGMA foreign_keys = ON;

-- ─────────────────────────────────────────────────────────── meta ──
CREATE TABLE IF NOT EXISTS arena_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collections (
    collection_id TEXT PRIMARY KEY,
    kind          TEXT NOT NULL CHECK (kind IN ('benchmark_release', 'user_run_collection')),
    name          TEXT NOT NULL,
    description   TEXT,
    created_at    TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS citations (
    citation_id TEXT PRIMARY KEY,
    kind        TEXT,            -- dataset | operator | library | method | view
    title       TEXT,
    doi         TEXT,
    url         TEXT,
    license     TEXT,
    bibtex      TEXT
);

-- ────────────────────────────────────────────────────── dimensions ──

-- The always-present data identity (io schema_fingerprint when available).
CREATE TABLE IF NOT EXISTS dataset_fingerprints (
    dataset_fingerprint  TEXT PRIMARY KEY,
    privacy_level        TEXT NOT NULL DEFAULT 'public',
    n_samples            INTEGER,
    n_features           INTEGER,
    task_type            TEXT,
    schema_fingerprint   TEXT,
    relation_fingerprint TEXT,
    plan_fingerprint     TEXT,
    x_stats_json         TEXT NOT NULL DEFAULT '{}',
    y_stats_json         TEXT NOT NULL DEFAULT '{}',
    group_stats_json     TEXT NOT NULL DEFAULT '{}',
    dataset_card_hash    TEXT REFERENCES dataset_cards (dataset_card_hash),
    producer_version     TEXT,
    created_at           TEXT NOT NULL
);

-- Optional rich identity card for known (catalog) datasets.
CREATE TABLE IF NOT EXISTS dataset_cards (
    dataset_card_hash     TEXT PRIMARY KEY,
    dataset_id            TEXT,
    dataset_version       TEXT,
    source_registry       TEXT,
    dataset_fingerprint   TEXT,
    visibility            TEXT NOT NULL DEFAULT 'public',
    name                  TEXT,
    domain                TEXT,
    modality              TEXT,
    signal_type           TEXT,
    axis_unit             TEXT,
    axis_min              REAL,
    axis_max              REAL,
    axis_resolution       REAL,
    n_samples             INTEGER,
    n_features            INTEGER,
    license               TEXT,
    content_hash          TEXT,
    descriptor_hash       TEXT,
    access_policy_json    TEXT NOT NULL DEFAULT '{}',
    identity_stats_json   TEXT NOT NULL DEFAULT '{}',
    nirs_stats_json       TEXT NOT NULL DEFAULT '{}',
    grouping_metadata_json TEXT NOT NULL DEFAULT '{}',
    citation_id           TEXT REFERENCES citations (citation_id),
    card_json             TEXT NOT NULL DEFAULT '{}',
    created_at            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_specs (
    task_hash           TEXT PRIMARY KEY,
    dataset_fingerprint TEXT REFERENCES dataset_fingerprints (dataset_fingerprint),
    task_type           TEXT NOT NULL DEFAULT 'regression',
    target_name         TEXT,
    target_unit         TEXT,
    target_hash         TEXT,
    encoding_json       TEXT NOT NULL DEFAULT '{}',
    target_stats_json   TEXT NOT NULL DEFAULT '{}',
    created_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_variants (
    dataset_variant_hash TEXT PRIMARY KEY,
    dataset_fingerprint  TEXT REFERENCES dataset_fingerprints (dataset_fingerprint),
    task_hash            TEXT REFERENCES task_specs (task_hash),
    variant_spec_json    TEXT NOT NULL DEFAULT '{}',
    sample_manifest_hash TEXT,
    size_label           TEXT,            -- materialized for dataviz (50 | 100 | all)
    aggregation          TEXT,            -- materialized for dataviz (none | sample_mean | group_mean)
    created_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_dags (
    pipeline_dag_hash       TEXT PRIMARY KEY,
    dag_schema_version      TEXT NOT NULL DEFAULT 'arena.graph/1',
    graph_json              TEXT NOT NULL,          -- canonical GraphSpec (source of truth)
    entry_nodes_json        TEXT NOT NULL DEFAULT '[]',
    terminal_nodes_json     TEXT NOT NULL DEFAULT '[]',
    n_nodes                 INTEGER NOT NULL DEFAULT 0,
    is_linear               INTEGER NOT NULL DEFAULT 0,
    main_model              TEXT,                   -- materialized for dataviz
    human_label             TEXT,
    engine_graph_fingerprint TEXT,                  -- recorded secondary id
    nirs4all_identity_hash  TEXT,                   -- recorded secondary id (get_hash)
    created_at              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS operator_specs (
    operator_spec_hash TEXT PRIMARY KEY,
    library            TEXT,
    version            TEXT,
    entrypoint         TEXT,            -- dotted import path
    role               TEXT,
    family             TEXT,
    citation_id        TEXT REFERENCES citations (citation_id),
    license            TEXT,
    created_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parameter_values (
    param_value_hash TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    value_json       TEXT NOT NULL,
    kind             TEXT,            -- numeric | categorical | bool | sequence | object
    numeric_value    REAL,            -- set when kind == numeric (powers effect plots)
    is_numeric       INTEGER NOT NULL DEFAULT 0,
    is_ordinal       INTEGER NOT NULL DEFAULT 0,
    is_sweepable     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pipeline_nodes (
    pipeline_dag_hash  TEXT NOT NULL REFERENCES pipeline_dags (pipeline_dag_hash),
    node_id            TEXT NOT NULL,          -- normalized id (g0, g1, …)
    node_signature     TEXT NOT NULL,          -- Merkle signature
    role               TEXT NOT NULL DEFAULT 'transform',
    operator           TEXT,                   -- dotted import path
    operator_version   TEXT,
    operator_spec_hash TEXT REFERENCES operator_specs (operator_spec_hash),
    params_hash        TEXT,
    params_json        TEXT NOT NULL DEFAULT '{}',
    branch_path_json   TEXT NOT NULL DEFAULT '[]',
    source_id          TEXT,
    model_family       TEXT,
    fit_scope          TEXT,
    PRIMARY KEY (pipeline_dag_hash, node_id)
);

CREATE TABLE IF NOT EXISTS pipeline_node_params (
    pipeline_dag_hash TEXT NOT NULL,
    node_id           TEXT NOT NULL,
    param_name        TEXT NOT NULL,
    param_value_hash  TEXT NOT NULL REFERENCES parameter_values (param_value_hash),
    PRIMARY KEY (pipeline_dag_hash, node_id, param_name),
    FOREIGN KEY (pipeline_dag_hash, node_id) REFERENCES pipeline_nodes (pipeline_dag_hash, node_id)
);

CREATE TABLE IF NOT EXISTS pipeline_edges (
    pipeline_dag_hash TEXT NOT NULL REFERENCES pipeline_dags (pipeline_dag_hash),
    src               TEXT NOT NULL,
    src_port          TEXT NOT NULL DEFAULT 'out',
    dst               TEXT NOT NULL,
    dst_port          TEXT NOT NULL DEFAULT 'in',
    PRIMARY KEY (pipeline_dag_hash, src, src_port, dst, dst_port)
);

CREATE TABLE IF NOT EXISTS split_specs (
    split_spec_hash      TEXT PRIMARY KEY,
    method               TEXT NOT NULL DEFAULT 'none',
    params_json          TEXT NOT NULL DEFAULT '{}',
    group_policy         TEXT,
    stratification_policy TEXT
);

CREATE TABLE IF NOT EXISTS split_instances (
    split_instance_hash  TEXT PRIMARY KEY,
    split_spec_hash      TEXT REFERENCES split_specs (split_spec_hash),
    rng_context_hash     TEXT,
    partition_summary_json TEXT NOT NULL DEFAULT '{}',
    split_indices_hash   TEXT
);

CREATE TABLE IF NOT EXISTS cv_specs (
    cv_spec_hash      TEXT PRIMARY KEY,
    method            TEXT NOT NULL DEFAULT 'none',
    n_folds           INTEGER,
    n_repeats         INTEGER,
    nested            INTEGER NOT NULL DEFAULT 0,
    within_train_only INTEGER NOT NULL DEFAULT 1,
    params_json       TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS cv_instances (
    cv_instance_hash          TEXT PRIMARY KEY,
    cv_spec_hash              TEXT REFERENCES cv_specs (cv_spec_hash),
    rng_context_hash          TEXT,
    fold_summary_json         TEXT NOT NULL DEFAULT '{}',
    engine_fold_set_fingerprint TEXT
);

CREATE TABLE IF NOT EXISTS rng_contexts (
    rng_context_hash      TEXT PRIMARY KEY,
    root_seed             INTEGER,
    derivation            TEXT,
    framework_seeds_json  TEXT NOT NULL DEFAULT '{}',
    determinism_flags_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS refit_strategies (
    refit_strategy_hash TEXT PRIMARY KEY,
    strategy            TEXT NOT NULL DEFAULT 'none',
    selection_scope     TEXT,
    train_scope         TEXT,
    params_json         TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS score_computation_specs (
    score_computation_hash TEXT PRIMARY KEY,
    score_version          TEXT NOT NULL DEFAULT '1.0',
    metric_implementation  TEXT,
    score_level            TEXT,           -- fold | cv | refit | test | view
    aggregation_policy     TEXT,
    filters_json           TEXT NOT NULL DEFAULT '{}',
    direction              TEXT,
    standardization        TEXT,
    created_at             TEXT NOT NULL
);

-- ──────────────────────────────────────────────────────────── facts ──

CREATE TABLE IF NOT EXISTS run_conditions (
    run_condition_hash   TEXT PRIMARY KEY,
    collection_id        TEXT REFERENCES collections (collection_id),
    dataset_variant_hash TEXT REFERENCES dataset_variants (dataset_variant_hash),
    split_instance_hash  TEXT REFERENCES split_instances (split_instance_hash),
    cv_instance_hash     TEXT REFERENCES cv_instances (cv_instance_hash),
    rng_context_hash     TEXT REFERENCES rng_contexts (rng_context_hash),
    pipeline_dag_hash    TEXT REFERENCES pipeline_dags (pipeline_dag_hash),
    refit_strategy_hash  TEXT REFERENCES refit_strategies (refit_strategy_hash),
    task_hash            TEXT,               -- denormalized for fast filtering
    dataset_fingerprint  TEXT,               -- denormalized for fast filtering
    created_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_batches (
    ingestion_batch_id TEXT PRIMARY KEY,
    source             TEXT,
    input_export_hash  TEXT NOT NULL,
    arena_schema_version INTEGER NOT NULL,
    target_collection  TEXT,
    status             TEXT NOT NULL DEFAULT 'staging',  -- staging|committed|rejected|quarantined
    n_dimensions       INTEGER NOT NULL DEFAULT 0,
    n_facts            INTEGER NOT NULL DEFAULT 0,
    clean_report_json  TEXT NOT NULL DEFAULT '{}',
    errors_json        TEXT NOT NULL DEFAULT '[]',
    created_at         TEXT NOT NULL,
    -- Idempotency is global across collections (see ingestion/ingest.py §2).
    UNIQUE (input_export_hash, arena_schema_version)
);

CREATE TABLE IF NOT EXISTS executions (
    execution_hash      TEXT PRIMARY KEY,
    execution_id        TEXT,                -- producer uuid (recorded, never a join key)
    run_condition_hash  TEXT REFERENCES run_conditions (run_condition_hash),
    ingestion_batch_id  TEXT REFERENCES ingestion_batches (ingestion_batch_id),
    arena_export_hash   TEXT,
    producer_capsule    TEXT,
    nirs4all_version    TEXT,
    dag_ml_version      TEXT,
    dag_ml_data_version TEXT,
    io_version          TEXT,
    os                  TEXT,
    hardware            TEXT,
    time_ms             REAL,
    peak_mem_mb         REAL,
    status              TEXT NOT NULL DEFAULT 'ok',
    failure_code        TEXT,
    failure_message     TEXT,
    oof_enforced        INTEGER NOT NULL DEFAULT 0,
    unsafe_flags_json   TEXT NOT NULL DEFAULT '[]',
    validity_status     TEXT NOT NULL DEFAULT 'valid',   -- valid|quarantined|invalidated
    created_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS score_sets (
    score_set_id           TEXT PRIMARY KEY,
    execution_hash         TEXT REFERENCES executions (execution_hash),
    score_computation_hash TEXT REFERENCES score_computation_specs (score_computation_hash),
    scope                  TEXT NOT NULL DEFAULT 'cv',
    supersedes_score_set_id TEXT REFERENCES score_sets (score_set_id),
    validity_status        TEXT NOT NULL DEFAULT 'valid',
    created_at             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metric_observations (
    metric_observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    score_set_id      TEXT NOT NULL REFERENCES score_sets (score_set_id),
    metric_name       TEXT NOT NULL,
    metric_value      REAL,
    metric_unit       TEXT,
    direction         TEXT,
    fold_id           TEXT,
    partition         TEXT,
    aggregation_level TEXT,
    n_samples         INTEGER,
    coverage          REAL
);

CREATE TABLE IF NOT EXISTS residual_sets (
    residual_set_id  TEXT PRIMARY KEY,
    execution_hash   TEXT REFERENCES executions (execution_hash),
    score_set_id     TEXT REFERENCES score_sets (score_set_id),
    key              TEXT NOT NULL DEFAULT 'sample_id',   -- sample_id | positional (degraded)
    partition_set_json TEXT NOT NULL DEFAULT '[]',
    parquet_path     TEXT,
    n_rows           INTEGER NOT NULL DEFAULT 0,
    pseudonymized    INTEGER NOT NULL DEFAULT 1,
    publishable_json TEXT NOT NULL DEFAULT '{}',
    validity_status  TEXT NOT NULL DEFAULT 'valid',
    created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS view_definitions (
    view_definition_hash TEXT PRIMARY KEY,
    view_id              TEXT,
    kind                 TEXT,
    filters_json         TEXT NOT NULL DEFAULT '{}',
    score_level          TEXT,
    aggregation_json     TEXT NOT NULL DEFAULT '{}',
    definition_json      TEXT NOT NULL DEFAULT '{}',
    materialized_at      TEXT,
    created_at           TEXT NOT NULL
);

-- ──────────────────────────────────────────────────────── indexes ──
CREATE INDEX IF NOT EXISTS ix_run_conditions_pipeline ON run_conditions (pipeline_dag_hash);
CREATE INDEX IF NOT EXISTS ix_run_conditions_dataset  ON run_conditions (dataset_fingerprint);
CREATE INDEX IF NOT EXISTS ix_run_conditions_variant  ON run_conditions (dataset_variant_hash);
CREATE INDEX IF NOT EXISTS ix_run_conditions_task     ON run_conditions (task_hash);
CREATE INDEX IF NOT EXISTS ix_executions_condition    ON executions (run_condition_hash);
CREATE INDEX IF NOT EXISTS ix_executions_validity     ON executions (validity_status);
CREATE INDEX IF NOT EXISTS ix_score_sets_execution    ON score_sets (execution_hash);
CREATE INDEX IF NOT EXISTS ix_metric_obs_set          ON metric_observations (score_set_id);
CREATE INDEX IF NOT EXISTS ix_metric_obs_name         ON metric_observations (metric_name);
CREATE INDEX IF NOT EXISTS ix_nodes_operator          ON pipeline_nodes (operator);
CREATE INDEX IF NOT EXISTS ix_nodes_role              ON pipeline_nodes (role);
CREATE INDEX IF NOT EXISTS ix_node_params_name        ON pipeline_node_params (param_name);
CREATE INDEX IF NOT EXISTS ix_param_values_name       ON parameter_values (name);
CREATE INDEX IF NOT EXISTS ix_residual_sets_exec      ON residual_sets (execution_hash);

-- ──────────────────────────────────────────────────────────── views ──
-- One row per (execution, metric observation) joined to the full condition —
-- the backbone the serving layer queries (DATA_MANAGEMENT.md §6).
CREATE VIEW IF NOT EXISTS v_run_metrics AS
SELECT
    e.execution_hash,
    e.run_condition_hash,
    e.validity_status        AS execution_validity,
    e.status                 AS execution_status,
    e.producer_capsule,
    e.time_ms,
    rc.collection_id,
    rc.pipeline_dag_hash,
    rc.dataset_fingerprint,
    rc.dataset_variant_hash,
    rc.task_hash,
    rc.split_instance_hash,
    rc.cv_instance_hash,
    rc.rng_context_hash,
    rc.refit_strategy_hash,
    pd.main_model,
    pd.human_label           AS pipeline_label,
    ss.score_set_id,
    ss.scope                 AS score_scope,
    ss.validity_status       AS score_validity,
    ss.score_computation_hash,
    mo.metric_name,
    mo.metric_value,
    mo.direction,
    mo.fold_id,
    mo.partition,
    mo.aggregation_level,
    mo.n_samples
FROM executions e
JOIN run_conditions rc ON rc.run_condition_hash = e.run_condition_hash
LEFT JOIN pipeline_dags pd ON pd.pipeline_dag_hash = rc.pipeline_dag_hash
JOIN score_sets ss ON ss.execution_hash = e.execution_hash
JOIN metric_observations mo ON mo.score_set_id = ss.score_set_id;
