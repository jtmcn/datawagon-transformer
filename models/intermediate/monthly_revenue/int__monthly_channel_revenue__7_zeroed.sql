with
    int__monthly_channel_revenue__7_zeroed as (

        select
            report_date_key,
            asset_channel_id,
            channel_display_name,
            {{ zeroed("longs__ad_revenue") }} as longs__ad_revenue,
            {{ zeroed("longs__ad_revenue_adj") }} longs__ad_revenue_adj,
            {{ zeroed("longs__music_sub_revenue") }} as longs__music_sub_revenue,
            {{ zeroed("longs__non_music_sub_revenue") }} as longs__non_music_sub_revenue,
            {{ zeroed("shorts__ad_revenue") }} as shorts__ad_revenue,
            {{ zeroed("shorts__sub_revenue") }} as shorts__sub_revenue,
            surrogate_key
        from {{ ref("int__monthly_channel_revenue__6_short_sub") }}

    )

select *
from int__monthly_channel_revenue__7_zeroed
