-- ===================== DIMENSION TABLES =====================

CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    full_timestamp VARCHAR NOT NULL UNIQUE,
    minute VARCHAR NOT NULL,
    hour VARCHAR NOT NULL,
    day VARCHAR NOT NULL,
    week VARCHAR NOT NULL,
    month VARCHAR NOT NULL,
    year VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_artifact (
    artifact_id VARCHAR PRIMARY KEY,
    artifact_type VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_process (
    process_id VARCHAR PRIMARY KEY,
    status VARCHAR,
    outcome VARCHAR,
    process_type VARCHAR,
    stakeholder_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_stage (
    stage_id SERIAL PRIMARY KEY,
    process_type VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    parent_id INTEGER REFERENCES dim_stage(stage_id),
    is_leaf BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_deviation (
    deviation_id SERIAL PRIMARY KEY,
    type_name VARCHAR NOT NULL UNIQUE
);

-- ===================== BRIDGE TABLES =====================

CREATE TABLE IF NOT EXISTS bridge_stage_deviation (
    deviation_combination_id VARCHAR NOT NULL,
    deviation_id INTEGER NOT NULL REFERENCES dim_deviation(deviation_id),
    weight FLOAT,
    iteration_index INTEGER,
    PRIMARY KEY (deviation_combination_id, deviation_id)
);

CREATE TABLE IF NOT EXISTS bridge_artifact_stage (
    artifact_combination_id VARCHAR NOT NULL,
    artifact_id VARCHAR NOT NULL REFERENCES dim_artifact(artifact_id),
    weight FLOAT,
    PRIMARY KEY (artifact_combination_id, artifact_id)
);

-- ===================== FACT TABLES =====================

CREATE TABLE IF NOT EXISTS fact_stage_execution (
    stage_id INTEGER NOT NULL REFERENCES dim_stage(stage_id),
    process_id VARCHAR NOT NULL REFERENCES dim_process(process_id),
    time_open_id INTEGER NOT NULL REFERENCES dim_time(time_id),
    time_close_id INTEGER REFERENCES dim_time(time_id),
    artifact_combination_id VARCHAR,
    deviation_combination_id VARCHAR,
    execution_time INTEGER,
    waiting_time INTEGER,
    PRIMARY KEY (stage_id, process_id, time_open_id)
);

-- FACT_ARTIFACT_ENGAGEMENT: composite PK (artifact_id, process_id)
CREATE TABLE IF NOT EXISTS fact_artifact_engagement (
    artifact_id VARCHAR NOT NULL REFERENCES dim_artifact(artifact_id),
    process_id VARCHAR NOT NULL REFERENCES dim_process(process_id),
    time_attached_id INTEGER REFERENCES dim_time(time_id),
    time_detached_id INTEGER REFERENCES dim_time(time_id),
    engagement_duration FLOAT,
    idle_since_last_engagement FLOAT,
    PRIMARY KEY (artifact_id, process_id)
);

-- ===================== ETL SUPPORT TABLES =====================

CREATE TABLE IF NOT EXISTS stage_event_dedup_tracker (
    process_name VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    last_compliance VARCHAR,
    last_state VARCHAR,
    last_timestamp FLOAT,
    PRIMARY KEY (process_name, stage_name)
);

CREATE TABLE IF NOT EXISTS egsm_hierarchy_cache (
    process_type VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    parent_stage_name VARCHAR,
    is_leaf BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (process_type, stage_name)
);

CREATE TABLE IF NOT EXISTS deviation_process_tracker (
    process_id VARCHAR NOT NULL,
    perspective VARCHAR NOT NULL,
    last_deviations_hash VARCHAR,
    raw_deviations TEXT,          -- store full JSON array
    PRIMARY KEY (process_id, perspective)
);