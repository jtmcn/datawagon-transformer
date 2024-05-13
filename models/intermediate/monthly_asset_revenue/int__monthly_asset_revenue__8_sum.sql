with
int__monthly_asset_revenue__8_sum as (

    select
        report_date,
        asset_channel_id,
        {# channel_display_name, #}
        asset_id,
        {# asset_title, #}
        sum(longs__ad_revenue) as longs__ad_revenue,
        sum(longs__ad_revenue_adj) as longs__ad_revenue_adj,
        sum(longs__music_sub_revenue) as longs__music_sub_revenue,
        sum(longs__non_music_sub_revenue) as longs__non_music_sub_revenue,
        {# sum(shorts__ad_revenue) as shorts__ad_revenue,
        sum(shorts__sub_revenue) as shorts__sub_revenue, #}
        {{ dbt_utils.generate_surrogate_key([
            "report_date", 
            "asset_channel_id",
            "asset_id"
        ]) }} as surrogate_key
    from {{ ref("int__monthly_asset_revenue__7_zeroed") }}
    group by
        report_date,
        asset_channel_id,
        {# channel_display_name, #}
        asset_id
        {# asset_title       #}

)

select *
from int__monthly_asset_revenue__8_sum
