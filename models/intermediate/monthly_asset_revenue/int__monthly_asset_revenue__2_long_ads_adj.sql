with
revenue_by_asset as (

    select
        report_date,
        asset_channel_id,
        {# channel_display_name, #}
        asset_id,
        {# asset_title, #}
        video_id,
        {# video_title, #}
        partner_revenue as longs__ad_revenue,
        surrogate_key
    from {{ ref("int__longs__asset_claim_video__ad_rev_monthly") }}

),

longs__claim__ad_rev_adj__monthly as (

    select
        report_date,
        asset_channel_id,
        asset_id,
        sum(asset_revenue) as longs__ad_revenue_adj
    from {{ ref("int__longs__claim_adj__ad_rev_daily") }}
    group by report_date, asset_channel_id, asset_id

),

int__monthly_asset_revenue__2_long_ads_adj as (

    select
        long_ads.report_date,
        long_ads.asset_channel_id,
        {# long_ads.channel_display_name, #}
        long_ads.longs__ad_revenue,
        long_ads.asset_id,
        {# long_ads.asset_title, #}
        long_ads.video_id,
        {# long_ads.video_title, #}
        long_ads_adj.longs__ad_revenue_adj,
        long_ads.surrogate_key
    from revenue_by_asset as long_ads
    left join
        longs__claim__ad_rev_adj__monthly as long_ads_adj
        on
            long_ads.report_date = long_ads_adj.report_date
            and long_ads.asset_channel_id = long_ads_adj.asset_channel_id
            and long_ads.asset_id = long_ads_adj.asset_id

)

select *
from int__monthly_asset_revenue__2_long_ads_adj
