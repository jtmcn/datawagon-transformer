with
    int__monthly_asset_revenue__7_zeroed as (

        select
            report_date,
            asset_channel_id,
            {# channel_display_name, #}
            asset_id,
            {# asset_title, #}
            {# video_id, #}
            {# video_title,             #}
            {{ zeroed("longs__ad_revenue") }} as longs__ad_revenue,
            {{ zeroed("longs__ad_revenue_adj") }} longs__ad_revenue_adj,
            {{ zeroed("longs__music_sub_revenue") }} as longs__music_sub_revenue,
            {{ zeroed("longs__non_music_sub_revenue") }} as longs__non_music_sub_revenue,
            {# {{ zeroed("shorts__ad_revenue") }} as shorts__ad_revenue,
            {{ zeroed("shorts__sub_revenue") }} as shorts__sub_revenue, #}
            surrogate_key
        {# from {{ ref("int__monthly_asset_revenue__6_short_sub") }} #}
        from {{ ref("int__monthly_asset_revenue__4_long_non_music_sub") }}

    )

select *
from int__monthly_asset_revenue__7_zeroed
