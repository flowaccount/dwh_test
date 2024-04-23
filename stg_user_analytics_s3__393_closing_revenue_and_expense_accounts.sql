WITH stg_user_analytics_s3__raw_event AS (
    SELECT
        *
    FROM
        {{ source('user_analytics_s3', 'stg_user_analytics_s3__raw_event') }}
    WHERE
        event_id = 393
),

payload_extracted AS (
    SELECT
        *,
        payload."type"::INT AS payload_type,
        payload."ClosingPeriod"::VARCHAR AS payload_closing_period
    FROM
        stg_user_analytics_s3__raw_event
)

SELECT *
FROM payload_extracted