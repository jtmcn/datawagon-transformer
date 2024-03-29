with 

missing_acid as (

    select
        report_date,
        claim_count,
        asset_channel_id,
        asset_id,
        asset_title,
        video_id,
        video_title,
        channel_id,
        channel_display_name,
        owned_views,
        partner_revenue,
        surrogate_key
    from {{ ref('int__longs__asset_claim_video__ad_rev_monthly') }}
    where asset_channel_id is null

)

select *
from missing_acid
