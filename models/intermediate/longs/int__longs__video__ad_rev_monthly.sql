with
int__base__video__daily as (

    select 
        report_date,
        date_key,
        video_id,
        first_value(video_title) over (
            partition by report_date, video_id
            order by date_key desc) as latest_video_title,        
        channel_id,
        first_value(channel_display_name) over (
            partition by report_date, channel_id
            order by date_key desc) as latest_channel_name,                    
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
    from {{ ref('int__longs__video__ad_rev_daily') }}

),
int__base__video__monthly as (

    select 
        report_date,
        video_id,
        latest_video_title as video_title,
        channel_id,
        latest_channel_name as channel_display_name,
        sum(owned_views) as owned_views,
        sum(youtube_revenue_split__auction) as youtube_revenue_split__auction,
        sum(youtube_revenue_split__reserved) as youtube_revenue_split__reserved,
        sum(youtube_revenue_split__partner_sold_youtube_served) as youtube_revenue_split__partner_sold_youtube_served,
        sum(youtube_revenue_split__partner_sold_partner_served) as youtube_revenue_split__partner_sold_partner_served,
        sum(youtube_revenue_split) as youtube_revenue_split,
        sum(partner_revenue__auction) as partner_revenue__auction,
        sum(partner_revenue__reserved) as partner_revenue__reserved,
        sum(partner_revenue__partner_sold_youtube_served) as partner_revenue__partner_sold_youtube_served,
        sum(partner_revenue__partner_sold_partner_served) as partner_revenue__partner_sold_partner_served,
        sum(partner_revenue) as partner_revenue,
        {{
                dbt_utils.generate_surrogate_key(
                    ["report_date", "video_id", "channel_id"]
                )
        }} as surrogate_key        
    from int__base__video__daily
    group by 
        report_date,
        video_id,
        latest_video_title,
        channel_id,
        latest_channel_name         

)

select *
from int__base__video__monthly