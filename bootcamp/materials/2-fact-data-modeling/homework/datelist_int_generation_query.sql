WITH starter AS (
    -- Selecting the user_id, browser_type and checking which days the user was active
    -- based on the device_activity_datelist column and the valid_date
    SELECT udc.device_activity_datelist @> ARRAY [DATE(d.valid_date)] AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
           udc.user_id,
		   udc.browser_type
    FROM user_devices_cumulated udc
             CROSS JOIN
         (SELECT generate_series('2022-12-31', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-01-31')
),
bits AS (
    -- Selecting the user_id, browser_type, and creating the datelist_int column based on bit(32) calculations
    -- The datelist_int column is a bit(32) column that represents the days the user was active
	SELECT user_id,
	       browser_type,
	       SUM(CASE
			   WHEN is_active THEN POW(2, 32 - days_since)
			   ELSE 0 END)::bigint::bit(32) AS datelist_int,
               DATE('2023-02-01') as date
    FROM starter
    GROUP BY user_id, browser_type
)

SELECT *
FROM bits