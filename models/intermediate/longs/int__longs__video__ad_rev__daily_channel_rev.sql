with
    stg__longs__video__ad_rev as (select * from {{ ref("stg__longs__video__ad_rev") }}),

    stg__claim_raw__channel_revenue as (

        select
            report_date_key,
            date_key,
            channel_id,
            channel_display_name,
            sum(partner_revenue) as channel_revenue,
            {{
                dbt_utils.generate_surrogate_key(
                    ["report_date_key", "date_key", "channel_id"]
                )
            }} as surrogate_key
        from stg__longs__video__ad_rev
        group by report_date_key, date_key, channel_id, channel_display_name

    )

select *
from stg__claim_raw__channel_revenue
