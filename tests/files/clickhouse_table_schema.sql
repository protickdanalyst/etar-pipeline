CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.user_interactions
(
    event_id           UUID,
    user_id            UUID,
    session_id         UUID,
    event_type         Enum8('VIEW_PRODUCT' = 1, 'ADD_TO_CART' = 2, 'CHECKOUT' = 3, 'PAYMENT' = 4, 'SEARCH' = 5),
    event_timestamp    DateTime64(3, 'UTC'),        -- millisecond precision, stored in UTC
    request_latency_ms UInt32,
    status             Enum8('SUCCESS' = 1, 'ERROR' = 2),
    error_code         Nullable(Int32),             -- Int32 (signed) matches Avro ["null","int"]; was UInt32
    product_id         Nullable(UInt32),

    event_minute       DateTime MATERIALIZED toStartOfMinute(event_timestamp)
)

ENGINE = MergeTree()

-- Partition by calendar day, not by minute.
-- Per-minute partitioning produces ~1,440 parts/day (~525k/year), which
-- breaches ClickHouse's ~1,000 active-parts limit within days and triggers
-- TOO_MANY_PARTS errors that stall all ingestion.
PARTITION BY toYYYYMMDD(event_timestamp)

-- Primary key covers the two most common query patterns:
-- 1. GROUP BY event_type (analytics rollups)
-- 2. WHERE user_id = ? (per-user session lookups)
-- event_timestamp last so range predicates can prune within each user segment.
ORDER BY (event_type, user_id, event_timestamp)

-- Automatically drop data older than 90 days at the partition level.
TTL toDate(event_timestamp) + INTERVAL 90 DAY;
