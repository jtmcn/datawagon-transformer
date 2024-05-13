with

int__base__asset__daily as (

    select 
        report_date,
        date_key,
        asset_id,
        {# asset_title, #}
        first_value(asset_title) over (
            partition by report_date, asset_id
            order by date_key desc) as latest_asset_title,
        asset_channel_id,
        owned_views,
        youtube_revenue_split__auction,
        youtube_revenue_split__reserved,
        youtube_revenue_split__partner_sold_youtube_served,
        youtube_revenue_split__partner_sold_partner_served,
        youtube_revenue_split,
        partner_revenue__auction,
        partner_revenue__reserved,
        partner_revenue__partner_sold_youtube_served,
        partner_revenue__partner_sold_partner_served,
        partner_revenue

    from {{ ref('int__longs__asset__ad_rev_daily') }}

),
int__base__asset__monthly as (

    select
        report_date,
        asset_id,
        latest_asset_title as asset_title,
        asset_channel_id,
        sum(owned_views) as owned_views,
        sum(youtube_revenue_split__auction) as youtube_revenue_split__auction,
        sum(youtube_revenue_split__reserved) as youtube_revenue_split__reserved,
        sum(youtube_revenue_split__partner_sold_youtube_served)
            as youtube_revenue_split__partner_sold_youtube_served,
        sum(youtube_revenue_split__partner_sold_partner_served)
            as youtube_revenue_split__partner_sold_partner_served,
        sum(youtube_revenue_split) as youtube_revenue_split,
        sum(partner_revenue__auction) as partner_revenue__auction,
        sum(partner_revenue__reserved) as partner_revenue__reserved,
        sum(partner_revenue__partner_sold_youtube_served)
            as partner_revenue__partner_sold_youtube_served,
        sum(partner_revenue__partner_sold_partner_served)
            as partner_revenue__partner_sold_partner_served,
        sum(partner_revenue) as partner_revenue,
        {{
            dbt_utils.generate_surrogate_key(
                [ "report_date", "asset_channel_id", "asset_id"]
            )
        }} as surrogate_key
    from int__base__asset__daily
    group by
        report_date,
        asset_id,
        latest_asset_title,
        asset_channel_id

)

select *
from int__base__asset__monthly
