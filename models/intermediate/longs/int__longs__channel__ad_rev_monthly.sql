
with
int__longs__video__ad_rev_daily as (

    {# TODO: separate channel display name from video_id #}

    select
        report_date,
        channel_id,
        channel_display_name,
        {# video_id, #}


        {# sum(partner_revenue) as channel_revenue, #}
        {{ dbt_utils.generate_surrogate_key([
                        "report_date", 
                        "channel_id"
        ]) }} as surrogate_key
    from {{ ref("stg__longs__video__ad_rev") }}
    {# where rank = 1 #}
    group by 
        report_date, 
        channel_id, 
        channel_display_name
        {# video_id #}
    qualify row_number() over (
            partition by report_date, channel_id 
            order by count(channel_display_name) desc
        ) = 1

)

select *
from int__longs__video__ad_rev_daily
