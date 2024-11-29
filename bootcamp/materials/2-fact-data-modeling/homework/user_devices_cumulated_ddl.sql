-- Table to store user_devices_cumulated data
-- The key for this table is user_id, browser_type, date
CREATE TABLE user_devices_cumulated (
    -- user_id is numeric data type since the source data store the user_id as numeric
    user_id NUMERIC,
    browser_type TEXT,
    -- device_activity_datelist is an array of dates for the activity of a user and browser_type
    device_activity_datelist DATE[],
    -- the date column is the date that the data is related to
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);
