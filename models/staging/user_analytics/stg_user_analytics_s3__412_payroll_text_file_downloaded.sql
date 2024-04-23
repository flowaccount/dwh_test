WITH stg_user_analytics_s3__raw_event AS (
    SELECT *
    FROM
        {{ source('stg_user_analytics_s3', 'stg_user_analytics_s3__raw_event') }}
    WHERE
        event_id = 412
),

payload_extracted AS (
    SELECT *,
        payload."searchType"::INT AS payload_search_type,
        payload."startDate"::TIMESTAMP AS payload_start_date,
        payload."endDate"::TIMESTAMP AS payload_end_date,
        payload."textFile"::INT AS payload_text_file,
        payload."fileType"::VARCHAR(10) AS payload_file_type
    FROM
        stg_user_analytics_s3__raw_event
)

SELECT *
FROM payload_extracted
