-- Table to store hosts_cumulated data
-- The key for this table is host, date
CREATE TABLE hosts_cumulated (
    host TEXT,
    -- host_activity_datelist is an array of dates which logs to see which dates each host is experiencing any activity
    host_activity_datelist DATE[],
    -- the date column is the date that the data is related to
    date DATE,
    PRIMARY KEY (host, date)
);
