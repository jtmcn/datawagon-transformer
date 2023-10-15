with
int__channel_id_on_report_date as (

    select
        report_date_key,
        channel_id,
        channel_display_name,
        {{
            dbt_utils.generate_surrogate_key(
                ["report_date_key", "channel_id"]
            )
        }} as surrogate_key
    from {{ ref("int__channel_id_on_date_ranked") }}
    where latest_name_rank = 1
    group by report_date_key, channel_id, channel_display_name

)

select *
from int__channel_id_on_report_date
