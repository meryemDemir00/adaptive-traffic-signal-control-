CREATE DATABASE IF NOT EXISTS traffic;

-- Ham olcumler (traffic.raw)
CREATE TABLE IF NOT EXISTS traffic.raw_samples
(
    run_id            String,
    scenario          LowCardinality(String),
    mode              LowCardinality(String),
    seed              UInt32,
    sim_time          Float64,
    event_time_ms     Int64,
    produced_at_ms    Int64,
    tls_id            LowCardinality(String),
    total_halting     UInt32,
    queue_main        UInt32,
    queue_side        UInt32,
    avg_waiting_time  Float64,
    avg_speed         Float64,
    avg_occupancy     Float64,
    vehicle_count     UInt32,
    ts                DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(event_time_ms)
)
ENGINE = MergeTree
ORDER BY (run_id, tls_id, event_time_ms);

-- Spark pencere sonuclari (traffic.metrics)
CREATE TABLE IF NOT EXISTS traffic.metrics
(
    run_id               String,
    tls_id               LowCardinality(String),
    window_start_ms      Int64,
    window_end_ms        Int64,
    sim_time_end         Float64,
    last_produced_at_ms  Int64,
    spark_out_at_ms      Int64,
    avg_queue            Float64,
    avg_queue_main       Float64,
    avg_queue_side       Float64,
    avg_wait             Float64,
    avg_speed            Float64,
    avg_occupancy        Float64,
    ts                   DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(window_end_ms)
)
ENGINE = MergeTree
ORDER BY (run_id, tls_id, window_start_ms);

-- Kontrol kararlari ve uygulanma bilgisi (traffic.control + producer)
CREATE TABLE IF NOT EXISTS traffic.control_events
(
    run_id                 String,
    tls_id                 LowCardinality(String),
    program_id             LowCardinality(String),
    decided_at_ms          Int64,
    source_produced_at_ms  Int64,
    sim_time               Float64,
    reason                 LowCardinality(String),
    applied_at_ms          Int64,
    applied_sim_time       Float64,
    ts                     DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(decided_at_ms)
)
ENGINE = MergeTree
ORDER BY (run_id, tls_id, decided_at_ms);
CREATE USER IF NOT EXISTS traffic_user IDENTIFIED WITH plaintext_password BY 'traffic_pass';
GRANT ALL ON traffic.* TO traffic_user;
