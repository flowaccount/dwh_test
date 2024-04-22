WITH stg_user_analytics_s3__raw_event AS (
    SELECT *
    FROM {{ source('stg_user_analytics_s3', 'stg_user_analytics_s3__raw_event') }}
    WHERE
        event_id = 393
),

payload_extracted AS (
    SELECT *,
        payload."type"::VARCHAR AS payload_type,
        payload."ClosingPeriod"::INT AS payload_closing_period
    FROM
        stg_user_analytics_s3__raw_event
),

payload_extracted_2 AS (
    SELECT *,
        payload_type,
        payload_closing_period,
        CASE
            WHEN payload_closing_period ~ '[\\d]{2}-[\\d]{2}-[\\d]{4} to [\\d]{2}-[\\d]{2}-[\\d]{4}'
                THEN TO_DATE(SUBSTRING(payload_closing_period, 1, 10), 'DD-MM-YYYY')
        END AS payload_closing_period_start,
        CASE
            WHEN payload_closing_period ~ '[\\d]{2}-[\\d]{2}-[\\d]{4} to [\\d]{2}-[\\d]{2}-[\\d]{4}'
                THEN TO_DATE(SUBSTRING(payload_closing_period, 15, 10), 'DD-MM-YYYY')
        END AS payload_closing_period_end
    FROM
        payload_extracted
)

SELECT *
FROM payload_extracted_2