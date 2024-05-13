with
channel_revenue__short_ads as (

    select
        report_date,
        asset_channel_id,
        channel_display_name,
        longs__ad_revenue,
        longs__ad_revenue_adj,
        longs__music_sub_revenue,
        longs__non_music_sub_revenue,
        shorts__ad_revenue,
        surrogate_key
    from {{ ref("int__monthly_channel_revenue__5_short_ads") }}

),

shorts__video__sub_rev as (

    select
        report_date,
        channel_id,
        sum(channel_revenue) as shorts__sub_revenue
    from {{ ref("int__shorts__channel__sub_rev_monthly") }}
    group by report_date, channel_id

),

int__monthly_channel_revenue__6_short_sub as (

    select
        channel_revenue.report_date,
        channel_revenue.asset_channel_id,
        channel_revenue.channel_display_name,
        channel_revenue.longs__ad_revenue,
        channel_revenue.longs__ad_revenue_adj,
        channel_revenue.longs__music_sub_revenue,
        channel_revenue.longs__non_music_sub_revenue,
        channel_revenue.shorts__ad_revenue,
        shorts_sub.shorts__sub_revenue,
        channel_revenue.surrogate_key
    from channel_revenue__short_ads as channel_revenue

    left join
        shorts__video__sub_rev as shorts_sub
        on
            channel_revenue.report_date = shorts_sub.report_date
            and channel_revenue.asset_channel_id = shorts_sub.channel_id

)

select *
from int__monthly_channel_revenue__6_short_sub
