with
{# channels as ( #}
{# 
    select
        report_date,
        channel_id,
        channel_display_name
    from {{ ref("int__channel_id_on_report_date") }}

), #}

longs__claim__ad_rev__monthly as (

    select
        report_date,
        asset_channel_id,
        {# channel_display_name, #}
        asset_id,
        {# asset_title, #}
        video_id,
        {# video_title, #}
        sum(partner_revenue) as longs__ad_revenue
    from {{ ref("int__longs__claim__ad_rev_monthly") }}
    group by
        report_date,
        asset_channel_id,
        {# channel_display_name, #}
        asset_id,
        {# asset_title, #}
        video_id
        {# video_title #}

),

int__monthly_asset_revenue__1_long_ads as (

    select
        longs_ad_rev.report_date,
        longs_ad_rev.asset_channel_id,
        {# longs_ad_rev.channel_display_name, #}
        longs_ad_rev.asset_id,
        {# longs_ad_rev.asset_title, #}
        longs_ad_rev.video_id,
        {# longs_ad_rev.video_title, #}
        longs_ad_rev.longs__ad_revenue,
        {{ dbt_utils.generate_surrogate_key([
            "longs_ad_rev.report_date", 
            "longs_ad_rev.asset_channel_id", 
            "longs_ad_rev.asset_id",
            "longs_ad_rev.video_id"
        ]) }} as surrogate_key
    from longs__claim__ad_rev__monthly as longs_ad_rev
    {# left join
        channels
        on
            longs_ad_rev.report_date = channels.report_date
            and longs_ad_rev.asset_channel_id = channels.channel_id #}

)

select *
from int__monthly_asset_revenue__1_long_ads
