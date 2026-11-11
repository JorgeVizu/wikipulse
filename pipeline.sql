SET 'execution.target' = 'remote';
SET 'rest.address' = 'jobmanager';
SET 'rest.port' = '8081';
SET 'parallelism.default' = '3';
SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout' = '10 s';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'WikiPulse';

CREATE TABLE edits (
    edit_id STRING,
    title STRING,
    event_ms BIGINT,
    ingested_ms BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(event_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'wiki-edits',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'wikipulse-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE ranking (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    rank_no BIGINT,
    title STRING,
    edit_count BIGINT,
    avg_ingest_ms DOUBLE,
    PRIMARY KEY (window_end, rank_no) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'wiki-ranking',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO ranking
SELECT window_start, window_end, rank_no, title, edit_count, avg_ingest_ms
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY window_start, window_end ORDER BY edit_count DESC, title ASC
    ) AS rank_no
    FROM (
        SELECT window_start, window_end, title,
               COUNT(DISTINCT edit_id) AS edit_count,
               AVG(CAST(ingested_ms - event_ms AS DOUBLE)) AS avg_ingest_ms
        FROM TABLE(HOP(TABLE edits, DESCRIPTOR(event_time), INTERVAL '30' SECOND, INTERVAL '5' MINUTE))
        GROUP BY window_start, window_end, title
    )
)
WHERE rank_no <= 10;



