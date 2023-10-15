with
    int__channel_revenue_zeroed as (

        select
            report_date_key,
            asset_channel_id,
            channel_display_name,
            longs__ad_revenue,
            longs__ad_revenue_adj,
            longs__music_sub_revenue,
            longs__non_music_sub_revenue,
            shorts__ad_revenue,
            shorts__sub_revenue,
            surrogate_key
        from {{ ref("int__monthly_channel_revenue__7_zeroed") }}

    ),

    monthly_channel_revenue as (

        select
            {{ date_key_to_date("report_date_key") }} as report_date,
            report_date_key,
            asset_channel_id,
            channel_display_name,
            longs__ad_revenue,
            longs__ad_revenue_adj,
            longs__music_sub_revenue,
            longs__non_music_sub_revenue,
            shorts__ad_revenue,
            shorts__sub_revenue,

            longs__ad_revenue
            + longs__ad_revenue_adj
            + shorts__ad_revenue as ad_revenue,

            longs__music_sub_revenue
            + longs__non_music_sub_revenue
            + shorts__sub_revenue as sub_revenue,

            longs__ad_revenue
            + longs__ad_revenue_adj
            + shorts__ad_revenue
            + longs__music_sub_revenue
            + longs__non_music_sub_revenue
            + shorts__sub_revenue as total_revenue,

            surrogate_key

        from int__channel_revenue_zeroed

    )

select *
from monthly_channel_revenue
