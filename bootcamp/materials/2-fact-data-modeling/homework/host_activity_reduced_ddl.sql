-- Table to store host_activity_reduced data
-- The key for this table is month_start, date
CREATE TABLE host_activity_reduced
(
    -- Represents the start of the month data is related to
    month_start DATE,
    host TEXT,
    --  Array of bigint to represents the number of activities that the host is experiencing per day
    hit_array BIGINT[],
    --  Array of bigint to represents the number of unique users that the host is experiencing per day
    unique_visitors BIGINT[],
    PRIMARY KEY (month_start, host)
);