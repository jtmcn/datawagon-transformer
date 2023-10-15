with
    int__longs__video__ad_rev__daily_channel_rev as (

        select * from {{ ref("int__longs__video__ad_rev__daily_channel_rev") }}

    ),

    int__channels as (

        select
            report_date_key,
            date_key,
            channel_id,
            channel_display_name,
            {{
                dbt_utils.generate_surrogate_key(
                    ["report_date_key", "date_key", "channel_id"]
                )
            }} as surrogate_key
        from int__longs__video__ad_rev__daily_channel_rev
        group by report_date_key, date_key, channel_id, channel_display_name

    )

select *
from int__channels
