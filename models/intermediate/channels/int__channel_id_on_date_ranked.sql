with
int__channel_id_on_date_ranked as (

/* needs to be by report date key ? */
    select 
        report_date_key,
        channel_id,
        channel_display_name,
        date_key,
        rank()
            over (partition by channel_id, report_date_key order by date_key desc)
            as latest_name_rank
    from {{ ref("int__channel_id_on_date") }}

)

select * 
from int__channel_id_on_date_ranked
