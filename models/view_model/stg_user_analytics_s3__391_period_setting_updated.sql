WITH stg_user_analytics_s3__raw_event AS (
    
    SELECT *
    FROM
        {{ source('stg_user_analytics_s3', 'stg_user_analytics_s3__raw_event') }}
    WHERE
        event_id = 391
),

payload_extracted AS (
    SELECT *,
        payload."lockingPeriod"::VARCHAR AS payload_locking_period,
        payload."lockingPeriodOption"::INT AS payload_locking_period_option
    FROM
        stg_user_analytics_s3__raw_event
),

payload_extracted_2 AS (
    SELECT *,
        -- payload_locking_period,
        CASE
            WHEN payload_locking_period ~ '[\\d]{2}-[\\d]{2}-[\\d]{4} to [\\d]{2}-[\\d]{2}-[\\d]{4}'
                THEN TO_DATE(SUBSTRING(payload_locking_period, 1, 10), 'DD-MM-YYYY')
        END AS payload_locking_period_start,
        CASE
            WHEN payload_locking_period ~ '[\\d]{2}-[\\d]{2}-[\\d]{4} to [\\d]{2}-[\\d]{2}-[\\d]{4}'
                THEN TO_DATE(SUBSTRING(payload_locking_period, 15, 10), 'DD-MM-YYYY')
        END AS payload_locking_period_end
        -- payload_locking_period_option
    FROM
        payload_extracted
)

SELECT *
FROM payload_extracted_2