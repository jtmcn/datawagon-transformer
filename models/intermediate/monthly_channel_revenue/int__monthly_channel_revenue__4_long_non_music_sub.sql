with
    int__channel_revenue__music_sub as (

        select
            report_date,
            asset_channel_id,
            channel_display_name,
            longs__ad_revenue,
            longs__ad_revenue_adj,
            longs__music_sub_revenue,
            surrogate_key
        from {{ ref("int__monthly_channel_revenue__3_long_music_sub") }}

    ),

    longs__asset__sub_rev__monthly as (

        select
            report_date,
            asset_channel_id,
            sum(channel_revenue) as longs__non_music_sub_revenue
        from {{ ref("int__longs__asset__sub_rev_daily") }}
        group by report_date, asset_channel_id

    ),

    int__monthly_channel_revenue__4_long_non_music_sub as (

        select
            channel_revenue.report_date,
            channel_revenue.asset_channel_id,
            channel_revenue.channel_display_name,
            channel_revenue.longs__ad_revenue,
            channel_revenue.longs__ad_revenue_adj,
            channel_revenue.longs__music_sub_revenue,
            non_music_sub.longs__non_music_sub_revenue,
            channel_revenue.surrogate_key
        from int__channel_revenue__music_sub as channel_revenue
        left join
            longs__asset__sub_rev__monthly as non_music_sub
            on channel_revenue.report_date = non_music_sub.report_date
            and channel_revenue.asset_channel_id = non_music_sub.asset_channel_id

    )

select *
from int__monthly_channel_revenue__4_long_non_music_sub
