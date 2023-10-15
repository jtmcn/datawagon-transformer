with
channels as (

    select
        report_date_key,
        channel_id,
        channel_display_name
    from {{ ref("int__channel_id_on_report_date") }}
    -- group by report_date_key, channel_id, channel_display_name

),

longs__claim__ad_rev__monthly as (

    select
        report_date_key,
        asset_channel_id,
        sum(channel_revenue) as longs__ad_revenue
    from {{ ref("int__longs__claim__ad_rev__daily_channel_rev") }}
    group by report_date_key, asset_channel_id

),

int__channel_revenue_with_name as (

    select
        longs_ad_rev.report_date_key,
        longs_ad_rev.asset_channel_id,
        channels.channel_display_name,
        longs_ad_rev.longs__ad_revenue
    from longs__claim__ad_rev__monthly as longs_ad_rev
    left join
        channels
        on
            longs_ad_rev.report_date_key = channels.report_date_key
            and longs_ad_rev.asset_channel_id = channels.channel_id

),

int__monthly_channel_revenue__1_long_ads as (

    select
        report_date_key,
        asset_channel_id,
        channel_display_name,
        longs__ad_revenue,
            {{
                dbt_utils.generate_surrogate_key(
                    ["report_date_key", "asset_channel_id"]
                )
            }} as surrogate_key
    from int__channel_revenue_with_name

)

select *
from int__monthly_channel_revenue__1_long_ads
