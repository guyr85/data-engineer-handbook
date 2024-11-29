INSERT INTO user_devices_cumulated
WITH clean_events AS (
	-- Cleaning the events and grouping the browser_type that looks the same
	SELECT e.user_id,
		   TRIM(LOWER(d.browser_type)) AS browser_type,
		   CAST(e.event_time AS date) AS event_date
	FROM public.events AS e
	LEFT JOIN public.devices AS d
		ON e.device_id = d.device_id
	-- Filter Nulls values that are not relevant to the dataset
	WHERE e.user_id IS NOT NULL AND e.device_id IS NOT NULL AND e.event_time IS NOT NULL
	GROUP BY 1,2,3
),
user_browser_first_date AS(
    -- Getting the first date of the user and browser_type
	SELECT user_id,
		   browser_type,
		   MIN(event_date) AS first_date
	FROM clean_events
	GROUP BY 1,2
),
dates_series AS (
	-- Generating a series of dates from 2023-01-01 to 2023-01-31
	SELECT CAST(generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS date) AS date
),
users_browsers_dates AS (
	-- Joining the first date of the user and browser_type with the dates_series
    SELECT *
    FROM user_browser_first_date AS ubfd
    JOIN dates_series ds
        ON ubfd.first_date <= ds.date
),
windowed AS (
    -- Window function to get the active dates of the user and browser_type
	SELECT 
        ubd.user_id,
		ubd.browser_type,
        ubd.date,
        ARRAY_REMOVE(
			ARRAY_AGG(
                CASE
                    WHEN e.event_date IS NOT NULL
                    THEN e.event_date
					ELSE CAST('1900-01-01' AS DATE)
                END
			) OVER (PARTITION BY ubd.user_id, ubd.browser_type ORDER BY ubd.date ),
			 '1900-01-01'::DATE)  AS device_activity_datelist
    FROM users_browsers_dates AS ubd
    LEFT JOIN clean_events AS e
        ON ubd.user_id = e.user_id
		AND ubd.browser_type = e.browser_type
        AND ubd.date = e.event_date
)

-- Selecting the user_id, browser_type, device_activity_datelist (in reversed order) and date
-- I reversed the order of the device_activity_datelist to match the expected input for the future datelist_int generation
SELECT
	user_id,
	browser_type,
    ARRAY(
        SELECT d
        FROM unnest(device_activity_datelist) WITH ORDINALITY t(d, ordinality)
        ORDER BY ordinality DESC
    ) AS device_activity_datelist,
	date
FROM windowed