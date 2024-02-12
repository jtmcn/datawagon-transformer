with
int__base__claim__daily as (
    select * from {{ ref('int__longs__claim__ad_rev_daily__acid_override') }}
),

int__base__claim__monthly as (

    select 
        report_date,
        count(*) as claim_count, -- days
        asset_channel_id,
        asset_id,
        video_id,
        channel_id,
        sum(owned_views) as owned_views,
        sum(partner_revenue) as partner_revenue,
        {{
                dbt_utils.generate_surrogate_key(
                    ["report_date", "asset_channel_id", "asset_id", "video_id", "channel_id"]
                )
        }} as surrogate_key
    from int__base__claim__daily
    group by
        report_date,
        asset_channel_id,
        asset_id,
        video_id,
        channel_id

)

select *
from int__base__claim__monthly
