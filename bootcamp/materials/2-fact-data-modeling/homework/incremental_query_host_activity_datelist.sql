WITH yesterday AS (
    -- Get the data from the previous day hosts_cumulated table
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2022-12-31')
),
today AS (
    -- Get the data from the current day events table
	SELECT host,
		   CAST(event_time AS date) AS today_date,
		   COUNT(1) AS num_events
	FROM events
	-- DATE('2023-01-01') is an hard coded value since we are producing incremental query
	-- In production it will be parameterized or handled dynamically
	WHERE CAST(event_time AS date) = DATE('2023-01-01')
          AND host IS NOT NULL
    GROUP BY host, CAST(event_time AS date)
    )
INSERT INTO hosts_cumulated
SELECT
       -- Generate a host column that contains the host from the yesterday table or the today table
       COALESCE(t.host, y.host),
       -- Generate a host_activity_datelist date[] column that contains the dates the host was active
       COALESCE(y.host_activity_datelist,
           ARRAY[]::DATE[])
            || CASE WHEN
                t.host IS NOT NULL
                THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[]
                END AS host_activity_datelist,
       CAST(COALESCE(t.today_date, y.date + Interval '1 day') AS date) AS date
FROM yesterday y
FULL OUTER JOIN today t
	ON t.host = y.host;
