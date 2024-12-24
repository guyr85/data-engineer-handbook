-- Create processed_events_sessionized table
CREATE TABLE IF NOT EXISTS processed_events_sessionized (
    session_hour TIMESTAMP(3),
    session_end TIMESTAMP(3),
    ip VARCHAR,
    geodata VARCHAR,
    host VARCHAR,
    num_hits BIGINT
);