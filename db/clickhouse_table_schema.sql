CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.user_interactions
(
    event_id UUID,
    user_id UUID,
    session_id UUID,
    event_type Enum8('VIEW_PRODUCT' = 1, 'ADD_TO_CART' = 2, 'CHECKOUT' = 3, 'PAYMENT' = 4, 'SEARCH' = 5),  -- 8bits for 5 items
    event_timestamp DateTime64(3, 'UTC'), -- 3: 10^-3: Millisecond precision, UTC timezone. This is stored as datetime in db.
    request_latency_ms UInt32,
    status Enum8('SUCCESS' = 1, 'ERROR' = 2),
    error_code Nullable(UInt32),
    product_id Nullable(UInt32),
    
    event_minute DateTime MATERIALIZED toStartOfMinute(event_timestamp)
)

ENGINE = MergeTree()
PARTITION BY event_minute
ORDER BY (event_minute, event_type);