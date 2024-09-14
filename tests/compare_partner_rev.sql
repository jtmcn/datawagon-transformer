with 
report_channel_revenue as (

    select 
        report_date,
        sum(longs__non_music_sub_revenue) as longs__non_music_sub_revenue
    from {{ ref('monthly_channel_revenue')}}
    group by
        report_date

),

raw_csv_revenue as (

    select 
        report_date,
        sum(partner_revenue) as longs__non_music_sub_revenue
    from {{ source("raw_csv", "red_music_rawdata_asset") }}
    group by
        report_date
),

compare as (

    select
        report_channel_revenue.report_date,
        report_channel_revenue.longs__non_music_sub_revenue - raw_csv_revenue.longs__non_music_sub_revenue as difference,
    from report_channel_revenue
    left join raw_csv_revenue 
        using(report_date)
    where report_date = '2024-03-31'
)

select *
from compare
where difference > 0
