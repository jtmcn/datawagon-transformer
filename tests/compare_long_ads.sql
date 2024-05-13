with 
monthly_asset_revenue as (

    select 
        report_date,
        asset_channel_id,
        sum(longs__ad_revenue) as asset__longs__ad_revenue
    from {{ ref('int__monthly_asset_revenue__1_long_ads') }}
    where report_date = '2024-01-31'
    group by
        report_date,
        asset_channel_id

),

monthly_channel_revenue as (

    select 
        report_date,
        asset_channel_id,
        sum(longs__ad_revenue) as channel__longs__ad_revenue
    from {{ ref('int__monthly_channel_revenue__1_long_ads') }}
    where report_date = '2024-01-31'
    group by
        report_date,
        asset_channel_id

),

differences as (

    select 
        monthly_asset_revenue.report_date,
        monthly_asset_revenue.asset_channel_id,

        asset__longs__ad_revenue - coalesce(channel__longs__ad_revenue, 0) as longs__ad_revenue_difference,

    from monthly_asset_revenue
    left join monthly_channel_revenue
        on monthly_asset_revenue.asset_channel_id = monthly_channel_revenue.asset_channel_id


),

problems as (

    select 
        report_date,
        asset_channel_id,
        sum(longs__ad_revenue_difference) as longs__ad_revenue_difference,

    from differences
    group by 
        report_date,
        asset_channel_id    
    having 
        abs(longs__ad_revenue_difference) > 1

)


select * from problems