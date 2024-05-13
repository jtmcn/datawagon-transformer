with

int__channel_id_on_date_ranked as (

    select
        report_date,
        channel_id,
        channel_display_name
    from {{ ref("int__longs__channel__ad_rev_monthly") }}

),

int__channel_id_on_report_date as (

    select
        report_date,
        channel_id,
        channel_display_name,
        {{
            dbt_utils.generate_surrogate_key(
                ["report_date", "channel_id"]
            )
        }} as surrogate_key
    from int__channel_id_on_date_ranked
    group by report_date, channel_id, channel_display_name

)

select *
from int__channel_id_on_report_date
