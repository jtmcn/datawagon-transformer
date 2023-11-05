with
asset_channels as (

    select distinct asset_channel_id
    from {{ ref("stg__longs__claim__ad_rev") }}
    where asset_channel_id is not null

),

/*
    Some channel names change over time, we want to use the latest one
*/

channel_id_and_name_ranked as (

    select
        channel_id,
        channel_display_name,
        date_key,
        rank()
            over (partition by channel_id order by date_key desc)
            as latest_name_rank
    from {{ ref("int__longs__channel__ad_rev_daily") }}

),


channel_id_and_name as (

    select distinct
        channel_id,
        channel_display_name
    from channel_id_and_name_ranked
    where latest_name_rank = 1

),


int__asset_channels as (

    select
        channel_id as asset_channel_id, -- pk
        channel_display_name as asset_channel_display_name
    from channel_id_and_name
    where channel_id in (select asset_channel_id from asset_channels)

)

select *
from int__asset_channels
