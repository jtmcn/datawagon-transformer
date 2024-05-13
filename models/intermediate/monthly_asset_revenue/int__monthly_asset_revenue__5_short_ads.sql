with
asset_revenue__non_music_sub as (

    select
        report_date,
        asset_channel_id,
        {# channel_display_name, #}
        asset_id,
        {# asset_title, #}
        video_id,
        {# video_title, #}
        longs__ad_revenue,
        longs__ad_revenue_adj,
        longs__music_sub_revenue,
        longs__non_music_sub_revenue,
        surrogate_key
    from {{ ref("int__monthly_asset_revenue__4_long_non_music_sub") }}

),

shorts__video__ad_rev as (

    select
        report_date,
        channel_id,
        video_id,
        {# video_title, #}
        sum(video_revenue) as shorts__ad_revenue
    from {{ ref("int__shorts__video__ad_rev_monthly") }}
    group by report_date, channel_id, video_id

),

int__monthly_asset_revenue__5_short_ads as (

    select
        asset_revenue.report_date,
        asset_revenue.asset_channel_id,
        asset_revenue.asset_id,
        {# asset_revenue.asset_title, #}
        asset_revenue.video_id,
        {# coalesce(asset_revenue.video_title,
            shorts_ad.video_title) as video_title, #}
        {# asset_revenue.channel_display_name, #}
        asset_revenue.longs__ad_revenue,
        asset_revenue.longs__ad_revenue_adj,
        asset_revenue.longs__music_sub_revenue,
        asset_revenue.longs__non_music_sub_revenue,
        shorts_ad.shorts__ad_revenue,
        asset_revenue.surrogate_key
    from asset_revenue__non_music_sub as asset_revenue
    left join
        shorts__video__ad_rev as shorts_ad
        on
            asset_revenue.report_date = shorts_ad.report_date
            and asset_revenue.asset_channel_id = shorts_ad.channel_id
            and asset_revenue.video_id = shorts_ad.video_id

)

select *
from int__monthly_asset_revenue__5_short_ads
