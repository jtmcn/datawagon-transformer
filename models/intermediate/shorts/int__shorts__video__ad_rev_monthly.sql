with
    stg__shorts__video__ad_rev as (

        select * from {{ ref("stg__shorts__video__ad_rev") }}

    ),

    stg__shorts__video__ad_rev__monthly_channel_revenue as (

        select
            report_date,
            channel_id,
            video_id,
            video_title,            
            sum(partner_revenue) as video_revenue,
            {{ dbt_utils.generate_surrogate_key(["report_date", "channel_id", "video_id"]) }}
            as surrogate_key
        from stg__shorts__video__ad_rev
        group by report_date, channel_id, video_id, video_title

    )

select *
from stg__shorts__video__ad_rev__monthly_channel_revenue
    