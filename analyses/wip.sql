with

int__base__claim__monthly as (

    select 
        report_date,
        claim_count, -- days
        asset_channel_id,
        asset_id,
        video_id,
        channel_id,
        owned_views,
        partner_revenue,
        surrogate_key
    from {{ ref('int__longs__claim__ad_rev_monthly') }}

),

int__base__asset__monthly as (

    select
        report_date,
        asset_id,
        asset_title
    from {{ ref('int__longs__asset__ad_rev_monthly') }}


),

int__base__video__monthly as (

    select
        report_date,
        video_id,
        video_title,
        channel_id,
        channel_display_name
    from {{ ref('int__longs__video__ad_rev_monthly') }}
)


select 
    claim.report_date,
    claim.claim_count, -- days
    claim.asset_channel_id,
    claim.asset_id,
    asset.asset_title,
    claim.video_id,
    video.video_title,
    video.channel_id,
    video.channel_display_name,
    claim.owned_views,
    claim.partner_revenue,
    claim.surrogate_key

from int__base__claim__monthly as claim

left join int__base__asset__monthly as asset
    on claim.report_date = asset.report_date
    and claim.asset_id = asset.asset_id

left join int__base__video__monthly as video
    on claim.report_date = video.report_date
    and claim.video_id = video.video_id    

where
    claim.report_date = '2023-05-31'
    and
    claim.asset_channel_id is null
order by claim.partner_revenue desc
