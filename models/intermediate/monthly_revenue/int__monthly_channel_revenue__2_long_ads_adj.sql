with
revenue_by_channel as (

    select
        report_date,
        asset_channel_id,
        channel_display_name,
        longs__ad_revenue,
        surrogate_key
    from {{ ref("int__monthly_channel_revenue__1_long_ads") }}

),

longs__claim__ad_rev_adj__monthly as (

    select
        report_date,
        asset_channel_id,
        sum(channel_revenue) as longs__ad_revenue_adj
    from {{ ref("int__longs__claim_adj__ad_rev_daily") }}
    group by report_date, asset_channel_id

),

int__monthly_channel_revenue__2_long_ads_adj as (

    select
        long_ads.report_date,
        long_ads.asset_channel_id,
        long_ads.channel_display_name,
        long_ads.longs__ad_revenue,
        long_ads_adj.longs__ad_revenue_adj,
        long_ads.surrogate_key
    from revenue_by_channel as long_ads
    left join
        longs__claim__ad_rev_adj__monthly as long_ads_adj
        on
            long_ads.report_date = long_ads_adj.report_date
            and long_ads.asset_channel_id = long_ads_adj.asset_channel_id

)

select *
from int__monthly_channel_revenue__2_long_ads_adj
