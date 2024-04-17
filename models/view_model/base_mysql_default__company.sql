WITH renamed AS (
    SELECT
        id AS id,
        companyid AS company_id,
        createdon AS created_on,
        modifiedon AS modified_on,
        isadvance AS is_advance,
        isautorenew AS is_auto_renew
    FROM
        {{ source('mysql_default', 'company') }}
    WHERE createdon >= '2024-01-01'
),
cleaned AS (
    SELECT
    id :: INT AS id,
    company_id :: INT AS company_id,
    TO_TIMESTAMP(
        created_on,
        'YYYY-MM-DD"T"HH24:MI:SS.US'
    ) AS created_on,
    TO_TIMESTAMP(
        modified_on,
        'YYYY-MM-DD"T"HH24:MI:SS.US'
    ) AS modified_on,
    COALESCE(is_advance, TRUE) AS is_advance,
    COALESCE(is_auto_renew, FALSE) AS is_auto_renew
    FROM renamed
)

SELECT *
FROM cleaned