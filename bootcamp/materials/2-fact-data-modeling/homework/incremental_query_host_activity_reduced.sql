WITH yesterday AS (
    -- Get the latest data for the processed month
    SELECT *
    FROM host_activity_reduced
    WHERE month_start = '2023-01-01'
),
today AS (
	-- Get the data for the current day
	SELECT host,
	       CAST(event_time AS date) AS today_date,
           COUNT(1) AS num_hits,
		   COUNT(DISTINCT user_id) AS unique_visitors
    FROM events
    WHERE CAST(event_time AS date) = DATE('2023-01-01')
    AND user_id IS NOT NULL
    GROUP BY host, CAST(event_time AS date)
     )
INSERT INTO host_activity_reduced
-- Combine the data from yesterday and today and merge it into the host_activity_reduced table
SELECT
    COALESCE(y.month_start, CAST(DATE_TRUNC('month', t.today_date) AS date)) AS month_start,
	COALESCE(y.host, t.host) AS host,
    -- Since each index represents the day of the month, we need to fill the array with 0s for the days that have no data
	COALESCE(y.hit_array,
			 array_fill(0::BIGINT, ARRAY[COALESCE(today_date - CAST(DATE_TRUNC('month', today_date) AS date), 0)]))
			 || ARRAY[COALESCE(t.num_hits, 0)] AS hit_array,
	COALESCE(y.unique_visitors,
			 array_fill(0::BIGINT, ARRAY[COALESCE(today_date - CAST(DATE_TRUNC('month', today_date) AS date), 0)]))
			 || ARRAY[COALESCE(t.unique_visitors, 0)] AS unique_visitors
FROM yesterday y
FULL OUTER JOIN today t
	ON y.host = t.host
-- Merge the data into the host_activity_reduced table
-- For existing key, update the hit_array and unique_visitors otherwise insert the new data
ON CONFLICT (month_start, host)
DO
	UPDATE SET hit_array = EXCLUDED.hit_array,
			   unique_visitors = EXCLUDED.unique_visitors