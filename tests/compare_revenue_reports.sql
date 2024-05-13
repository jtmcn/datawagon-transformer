with 
monthly_asset_revenue as (

    select 
        report_date,
        asset_channel_id,
        sum(longs__ad_revenue) as asset__longs__ad_revenue,
        sum(longs__ad_revenue_adj) as asset__longs__ad_revenue_adj,
        sum(longs__music_sub_revenue) as asset__longs__music_sub_revenue,
        sum(longs__non_music_sub_revenue) as asset__longs__non_music_sub_revenue,
        {# sum(shorts__ad_revenue) as asset__shorts__ad_revenue, #}
        {# sum(shorts__sub_revenue) as asset__shorts__sub_revenue, #}
        {# sum(ad_revenue) as asset__ad_revenue, #}
        {# sum(sub_revenue) as asset__sub_revenue, #}
        {# sum(total_revenue) as asset__total_revenue #}
    from {{ ref('monthly_asset_revenue') }}
    where report_date = '2024-01-31'
    group by
        report_date,
        asset_channel_id

),

monthly_channel_revenue as (

    select 
        report_date,
        asset_channel_id,
        sum(longs__ad_revenue) as channel__longs__ad_revenue,
        sum(longs__ad_revenue_adj) as channel__longs__ad_revenue_adj,
        sum(longs__music_sub_revenue) as channel__longs__music_sub_revenue,
        sum(longs__non_music_sub_revenue) as channel__longs__non_music_sub_revenue,
        {# sum(shorts__ad_revenue) as channel__shorts__ad_revenue, #}
        {# sum(shorts__sub_revenue) as channel__shorts__sub_revenue,         #}
        {# sum(ad_revenue) as channel__ad_revenue, #}
        {# sum(sub_revenue) as channel__sub_revenue, #}
        {# sum(total_revenue) as channel__total_revenue #}
    from {{ ref('monthly_channel_revenue') }}
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
        asset__longs__ad_revenue_adj - coalesce(channel__longs__ad_revenue_adj, 0) as longs__ad_revenue_adj_difference,
        asset__longs__music_sub_revenue - coalesce(channel__longs__music_sub_revenue, 0) as longs__music_sub_revenue_difference,
        asset__longs__non_music_sub_revenue - coalesce(channel__longs__non_music_sub_revenue, 0) as longs__non_music_sub_revenue_difference
        {# asset__shorts__ad_revenue - coalesce(channel__shorts__ad_revenue, 0) as shorts__ad_revenue_difference,
        asset__shorts__sub_revenue - coalesce(channel__shorts__sub_revenue, 0) as shorts__sub_revenue_difference, #}

        {# asset__ad_revenue - coalesce(channel__ad_revenue, 0) as ad_revenue_difference,
        asset__sub_revenue - coalesce(channel__sub_revenue, 0) as sub_revenue_difference,
        asset__total_revenue - coalesce(channel__total_revenue, 0) as total_revenue_difference #}

    from monthly_asset_revenue
    left join monthly_channel_revenue
        on monthly_asset_revenue.asset_channel_id = monthly_channel_revenue.asset_channel_id


),

problems as (

    select 
        report_date,
        asset_channel_id,
        sum(longs__ad_revenue_difference) as longs__ad_revenue_difference,
        sum(longs__ad_revenue_adj_difference) as longs__ad_revenue_adj_difference,
        sum(longs__music_sub_revenue_difference) as longs__music_sub_revenue_difference,
        sum(longs__non_music_sub_revenue_difference) as longs__non_music_sub_revenue_difference
        {# sum(shorts__ad_revenue_difference) as shorts__ad_revenue_difference, #}
        {# sum(shorts__sub_revenue_difference) as shorts__sub_revenue_difference, #}

        {# sum(ad_revenue_difference) as ad_revenue_difference,
        sum(sub_revenue_difference) as sub_revenue_difference,
        sum(total_revenue_difference) as total_revenue_difference #}
    from differences
    group by 
        report_date,
        asset_channel_id    
    having 
        abs(longs__ad_revenue_difference) > 1
        or
        abs(longs__ad_revenue_adj_difference) > 1
        or
        abs(longs__music_sub_revenue_difference) > 1
        or
        abs(longs__non_music_sub_revenue_difference) > 1
        {# abs(sub_revenue_difference) > 1
        or 
        abs(total_revenue_difference) > 1 #}

)


select * from problems