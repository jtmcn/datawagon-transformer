with
stg__longs__video__ad_rev as (
    select * from {{ ref("stg__longs__video__ad_rev") }}
),

int__longs__video__ad_rev_daily as (

    select
        report_date,
        date_key,
        channel_id,
        channel_display_name,
        sum(partner_revenue) as channel_revenue,
            {{
                dbt_utils.generate_surrogate_key(
                    ["report_date", "date_key", "channel_id"]
                )
            }} as surrogate_key
    from stg__longs__video__ad_rev
    group by report_date, date_key, channel_id, channel_display_name

)

select *
from int__longs__video__ad_rev_daily
