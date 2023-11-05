with

int__longs__video__ad_rev__daily_channel_rev as (

    select * from {{ ref("int__longs__channel__ad_rev_daily") }}

),

int__channel_id_on_date as (

    select distinct
        report_date,
        date_key,
        channel_id,
        channel_display_name
    from int__longs__video__ad_rev__daily_channel_rev

),

int__channel_id_on_date_ranked as (

    select
        report_date,
        channel_id,
        first_value(channel_display_name) over (
            partition by report_date, channel_id
            order by date_key desc) as latest_channel_name
    from int__channel_id_on_date

),

int__channel_id_on_report_date as (

    select
        report_date,
        channel_id,
        latest_channel_name as channel_display_name,
        {{
            dbt_utils.generate_surrogate_key(
                ["report_date", "channel_id"]
            )
        }} as surrogate_key
    from int__channel_id_on_date_ranked
    group by report_date, channel_id, latest_channel_name

)

select *
from int__channel_id_on_report_date
