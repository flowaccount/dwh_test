WITH renamed AS (
    SELECT
        companyid AS company_id,
        userid AS user_id,
        platform AS platform,
        createddate AS event_time,
        documenttypeenum AS document_type_enum,
        documenttypestring AS document_type_string,
        documentid AS document_id,
        isvat AS is_vat,
        includesvat AS includes_vat,
        totalvalue AS total_value,
        source AS source,
        s3key AS s3_key,
        useragent AS user_agent,
        documentaction AS document_action
    FROM
        {{ source(
            'fa_event_test',
            'engagement'
        ) }}
    WHERE createddate like '2023-01-27%'
),

cleaned AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'document_id',
            'event_time'
        ]) }} AS event_id,
        company_id::BIGINT,
        user_id::BIGINT,
        CASE
            WHEN plat.enum IS NOT NULL THEN plat.application
            WHEN e.platform IS NOT NULL THEN 'Invalid Application'
            ELSE 'N/A'
        END AS "application",
        CASE
            WHEN plat.enum IS NOT NULL THEN plat.platform
            WHEN e.platform IS NOT NULL THEN 'Invalid Platform'
            ELSE 'N/A'
        END AS platform,
        event_time,
        CASE
            WHEN doc.enum IS NOT NULL THEN doc.document_type
            WHEN e.document_type_enum IS NOT NULL THEN 'Invalid Document Type'
            ELSE 'N/A'
        END AS document_type,
        document_id :: BIGINT,
        COALESCE(is_vat, FALSE) AS is_vat,
        COALESCE(includes_vat, FALSE) AS includes_vat,
        COALESCE(total_value, 0) :: DECIMAL(23, 8) AS total_value,
        CASE
            WHEN source IS NULL THEN 'N/A'
            WHEN source IN (
                'user-analytics',
                'audit-stamp'
            ) THEN source
            ELSE 'Invalid Source'
        END AS source,
        COALESCE(s3_key, 'N/A') AS s3_key,
        COALESCE(user_agent, 'N/A') AS user_agent,
        COALESCE(document_action, 'N/A') AS document_action
    FROM
        renamed AS e
    LEFT JOIN
        {{ ref('platform_types') }} AS plat
        ON e.platform = plat.enum
    LEFT JOIN
        {{ ref('document_types') }} AS doc
        ON e.document_type_enum = doc.enum
            OR LOWER(e.document_type_string) = LOWER(doc.constant_ts)
)

SELECT *
FROM cleaned
{% if is_incremental() %}
WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}