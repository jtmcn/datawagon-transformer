with
    stg__longs__claim__ad_rev_adj as (

        select * from {{ ref("stg__longs__claim__ad_rev_adj") }}

    ),

    stg__longs_ads__channel_revenue as (

        select
            report_date_key,
            date_key,
            asset_channel_id,
            sum(partner_revenue) as channel_revenue,
            {{
                dbt_utils.generate_surrogate_key(
                    ["report_date_key", "date_key", "asset_channel_id"]
                )
            }} as surrogate_key
        from stg__longs__claim__ad_rev_adj
        group by report_date_key, date_key, asset_channel_id

    )

select *
from stg__longs_ads__channel_revenue
