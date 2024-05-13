with
int__asset_revenue_sum as (

    select
        report_date,
        asset_channel_id,
        {# channel_display_name, #}
        asset_id,
        {# asset_title, #}
        {# video_id,
        video_title, #}
        longs__ad_revenue,
        longs__ad_revenue_adj,
        longs__music_sub_revenue,
        longs__non_music_sub_revenue,
        {# shorts__ad_revenue, #}
        {# shorts__sub_revenue, #}
        surrogate_key
    from {{ ref("int__monthly_asset_revenue__8_sum") }}

),

monthly_asset_revenue as (

    select
        report_date,
        asset_channel_id,
        {# channel_display_name, #}
        asset_id,
        {# asset_title, #}
        {# video_id,
        video_title, #}
        longs__ad_revenue,
        longs__ad_revenue_adj,
        longs__music_sub_revenue,
        longs__non_music_sub_revenue,
        {# shorts__ad_revenue, #}
        {# shorts__sub_revenue, #}

        surrogate_key
{# 
        longs__ad_revenue
        + longs__ad_revenue_adj
        + shorts__ad_revenue as ad_revenue, #}
{# 
        longs__music_sub_revenue
        + longs__non_music_sub_revenue
        + shorts__sub_revenue as sub_revenue, #}
{# 
        longs__ad_revenue
        + longs__ad_revenue_adj
        + shorts__ad_revenue
        + longs__music_sub_revenue
        + longs__non_music_sub_revenue
        + shorts__sub_revenue as total_revenue #}

    from int__asset_revenue_sum

)

select *
from monthly_asset_revenue
