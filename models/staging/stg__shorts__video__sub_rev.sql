with
    source as (

        select *
        from
            {{
                source(
                    "raw_csv", "monthly_shorts_non_music_subscription_video_summary"
                )
            }}

    ),

    renamed as (

        select
            report_date, {# Partition #}        
            adjustment_type,
            day as date_key,
            country as country_code,
            video_id,
            custom_id,
            content_type,
            video_title,
            video_duration__sec,
            username,
            uploader,
            {{ clean_channel_id("channel_id") }} as channel_id,
            owned_subscription_views,
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
            custom_id,
            content_type,
            video_title,
            video_duration__sec,
            username,
            uploader,
            channel_id,
            owned_subscription_views,
            partner_revenue
        from renamed

        {% if is_incremental() %}

            where report_date > (select max(report_date) from {{ this }})

        {% endif %}

    )

select *
from incremental
