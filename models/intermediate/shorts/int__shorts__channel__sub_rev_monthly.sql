with
    stg__shorts__video__sub_rev as (

        select * from {{ ref("stg__shorts__video__sub_rev") }}

    ),

    stg__shorts__video__sub_rev__daily_channel_revenue as (

        select
            report_date,
            channel_id,
            sum(partner_revenue) as channel_revenue,
            {{ dbt_utils.generate_surrogate_key([
                "report_date", "channel_id"
            ]) }} as surrogate_key
        from stg__shorts__video__sub_rev
        group by report_date, channel_id

    )

select *
from stg__shorts__video__sub_rev__daily_channel_revenue
