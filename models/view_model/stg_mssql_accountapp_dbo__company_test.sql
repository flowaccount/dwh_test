WITH renamed AS (
    SELECT id AS id,
        createdon AS created_on,
        modifiedon AS modified_on,
        isactive AS is_active,
        isadvance AS is_advance,
        signupchannel AS signup_channel,
        supportcode AS support_code,
        useragent AS user_agent,
        isvat AS is_vat
    FROM {{ source('mssql_accountapp_dbo', 'company') }}
    WHERE createdon >= '2024-01-01'
),
cleaned AS (
    SELECT
        id::BIGINT,
        TO_TIMESTAMP(created_on, 'YYYY-MM-DD"T"HH24:MI:SS.US') AS created_on,
        TO_TIMESTAMP(modified_on, 'YYYY-MM-DD"T"HH24:MI:SS.US') AS modified_on,
        CASE WHEN is_active = 1 THEN TRUE
             ELSE FALSE END AS is_active,
        CASE WHEN is_advance = 1 THEN TRUE
             ELSE FALSE END AS is_advance,
        support_code::VARCHAR(10),
        user_agent,
        CASE WHEN is_vat = 1 THEN TRUE
             ELSE FALSE END AS is_vat
    FROM renamed
)

SELECT *
FROM cleaned