with
    source as (

        select *
        from {{ source("raw_csv", "monthly_shorts_non_music_ads_video_summary") }}

    ),

    renamed as (

        select
            adjustment_type,
            video_id,
            video_title,
            video_duration__sec,
            category,
            {{ clean_channel_id("channel_id") }} as channel_id,
            uploader,
            content_type,
            policy,
            total_views,
            net_partner_revenue__post_revshare as partner_revenue,
            report_date_key
        from source

    )

select *
from renamed
