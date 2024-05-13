with
    revenue_by_channel as (

        select
            report_date,
            asset_channel_id,
            channel_display_name,
            longs__ad_revenue,
            longs__ad_revenue_adj,
            surrogate_key
        from {{ ref("int__monthly_channel_revenue__2_long_ads_adj") }}

    ),

    longs__music_asset__sub_rev__monthly as (

        select
            report_date,
            asset_channel_id,
            sum(asset_revenue) as longs__music_sub_revenue
        from {{ ref("int__longs__music_asset__sub_rev_daily") }}
        group by report_date, asset_channel_id

    ),

    int__monthly_channel_revenue__3_long_music_sub as (

        select
            revenue_by_channel.report_date,
            revenue_by_channel.asset_channel_id,
            revenue_by_channel.channel_display_name,
            revenue_by_channel.longs__ad_revenue,
            revenue_by_channel.longs__ad_revenue_adj,
            music_sub.longs__music_sub_revenue,
            revenue_by_channel.surrogate_key
        from revenue_by_channel
        left join
            longs__music_asset__sub_rev__monthly as music_sub
            on revenue_by_channel.report_date = music_sub.report_date
            and revenue_by_channel.asset_channel_id = music_sub.asset_channel_id

    )

select *
from int__monthly_channel_revenue__3_long_music_sub
