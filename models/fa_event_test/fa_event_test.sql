with
    renamed as (
        select
            companyid as company_id,
            userid as user_id,
            platform as platform,
            createddate as event_time,
            documenttypeenum as document_type_enum,
            documenttypestring as document_type_string,
            documentid as document_id,
            isvat as is_vat,
            includesvat as includes_vat,
            totalvalue as total_value,
            source as source,
            s3key as s3_key,
            useragent as user_agent,
            documentaction as document_action
        from {{ source("fa_event_test", "engagement") }}
        where createddate like '2023-01-27%'
    ),

    cleaned as (
        select
            {{ dbt_utils.generate_surrogate_key(["document_id", "event_time"]) }}
            as event_id,
            company_id::bigint,
            user_id::bigint,
            case
                when plat.enum is not null
                then plat.application
                when e.platform is not null
                then 'Invalid Application'
                else 'N/A'
            end as "application",
            case
                when plat.enum is not null
                then plat.platform
                when e.platform is not null
                then 'Invalid Platform'
                else 'N/A'
            end as platform,
            event_time,
            case
                when doc.enum is not null
                then doc.document_type
                when e.document_type_enum is not null
                then 'Invalid Document Type'
                else 'N/A'
            end as document_type,
            document_id::bigint,
            coalesce(is_vat, false) as is_vat,
            coalesce(includes_vat, false) as includes_vat,
            coalesce(total_value, 0)::decimal(23, 8) as total_value,
            case
                when source is null
                then 'N/A'
                when source in ('user-analytics', 'audit-stamp')
                then source
                else 'Invalid Source'
            end as source,
            coalesce(s3_key, 'N/A') as s3_key,
            coalesce(user_agent, 'N/A') as user_agent,
            coalesce(document_action, 'N/A') as document_action
        from renamed as e
        left join {{ ref("platform_types") }} as plat on e.platform = plat.enum
        left join
            {{ ref("document_types") }} as doc
            on e.document_type_enum = doc.enum
            or lower(e.document_type_string) = lower(doc.constant_ts)
    )

select *
from cleaned
{% if is_incremental() %}
    where event_time > (select max(event_time) from {{ this }})
{% endif %}
