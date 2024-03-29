with
stg__update_acid_mapping as (
  
    select
        report_date_override,
        match_asset_id,
        to_asset_channel_id
    from {{ ref('stg__update_acid_mapping') }}

),

int__longs__claim__ad_rev_daily as (
    
    select 
        claim.date_key,
        claim.report_date,
        coalesce(mapping.to_asset_channel_id, claim.asset_channel_id) as asset_channel_id,
        claim.asset_id,
        claim.video_id,
        claim.channel_id,
        claim.owned_views,
        claim.partner_revenue
    from {{ ref("int__longs__claim__ad_rev_daily") }} as claim
    left join stg__update_acid_mapping as mapping
        on claim.report_date = mapping.report_date_override
        and claim.asset_id = mapping.match_asset_id

),

int__longs__claim__ad_rev_daily__acid_override as (

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
    from int__longs__claim__ad_rev_daily
    group by
        date_key,
        report_date,
        asset_channel_id,
        asset_id,
        video_id,
        channel_id

)

select *
from int__longs__claim__ad_rev_daily__acid_override
