with

longs__video__ad_rev as (

    select 
    *, 
    first_value(channel_display_name) over (
            partition by report_date, channel_id
            order by date_key desc
    ) as latest_channel_display_name
    {# first_value(video_title) over (
        partition by report_date, video_id
        order by date_key desc
    ) as latest_video_title #}
    from {{ ref('stg__longs__video__ad_rev') }}

),  

int__base__video__monthly as (

    select 
        report_date,
        channel_id,
        video_id,
        latest_channel_display_name as channel_display_name,
        {# latest_video_title as video_title, #}
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
        {{ dbt_utils.generate_surrogate_key([
            "report_date", 
            "video_id", 
            "channel_id"
        ]) }} as surrogate_key        
    from longs__video__ad_rev
    group by 
        report_date,
        channel_id,
        latest_channel_display_name,      
        video_id
        {# latest_video_title #}

)

select *
from int__base__video__monthly