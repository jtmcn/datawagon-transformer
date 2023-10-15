with
channel_revenue__non_music_sub as (

    select
        report_date_key,
        asset_channel_id,
        channel_display_name,
        longs__ad_revenue,
        longs__ad_revenue_adj,
        longs__music_sub_revenue,
        longs__non_music_sub_revenue,
        surrogate_key
    from {{ ref("int__monthly_channel_revenue__4_long_non_music_sub") }}

),

shorts__video__ad_rev as (

    select
        report_date_key,
        channel_id,
        sum(channel_revenue) as shorts__ad_revenue
    from {{ ref("int__shorts__video__ad_rev__monthly_channel_rev") }}
    group by report_date_key, channel_id

),

int__monthly_channel_revenue__5_short_ads as (

    select
        channel_revenue.report_date_key,
        channel_revenue.asset_channel_id,
        channel_revenue.channel_display_name,
        channel_revenue.longs__ad_revenue,
        channel_revenue.longs__ad_revenue_adj,
        channel_revenue.longs__music_sub_revenue,
        channel_revenue.longs__non_music_sub_revenue,
        shorts_ad.shorts__ad_revenue,
        channel_revenue.surrogate_key
    from channel_revenue__non_music_sub as channel_revenue
    left join
        shorts__video__ad_rev as shorts_ad
        on
            channel_revenue.report_date_key = shorts_ad.report_date_key
            and channel_revenue.asset_channel_id = shorts_ad.channel_id

)

select *
from int__monthly_channel_revenue__5_short_ads
