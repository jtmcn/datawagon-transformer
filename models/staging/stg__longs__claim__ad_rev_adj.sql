with
    source as (select * from {{ source("raw_csv", "adj_claim_raw") }}),

    renamed as (

        select
            report_date, {# Partition #}        
            adjustment_type,
            day as date_key,
            country as country_code,
            video_id,
            {{ clean_channel_id("channel_id") }} as channel_id,
            asset_id,
            {{ clean_channel_id("asset_channel_id") }} as asset_channel_id,
            asset_type,
            custom_id,
            content_type,
            policy,
            claim_type,
            claim_origin,
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
        from source
    ),

    incremental as (

        select
            report_date,
            adjustment_type,
            date_key,
            country_code,
            video_id,
            channel_id,
            asset_id,
            asset_channel_id,
            asset_type,
            custom_id,
            content_type,
            policy,
            claim_type,
            claim_origin,
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
        from renamed

        {% if is_incremental() %}

            where report_date > (select max(report_date) from {{ this }})

        {% endif %}

    )

select *
from incremental
