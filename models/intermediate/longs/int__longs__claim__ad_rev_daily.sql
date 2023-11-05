with
stg__longs__claim__ad_rev as (
    select * from {{ ref("stg__longs__claim__ad_rev") }}
),

int__base__claim__daily as (

    select 
        date_key,
        report_date,
        asset_channel_id,
        asset_id,
        video_id,
        channel_id,
        sum(owned_views) as owned_views,
        sum(partner_revenue) as partner_revenue,
        {{
                dbt_utils.generate_surrogate_key(
                    ["date_key", "report_date", "asset_channel_id", "asset_id", "video_id", "channel_id"]
                )
        }} as surrogate_key
    from stg__longs__claim__ad_rev
    group by
        date_key,
        report_date,
        asset_channel_id,
        asset_id,
        video_id,
        channel_id

)

select *
from int__base__claim__daily
