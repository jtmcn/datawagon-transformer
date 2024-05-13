with
stg__longs__claim__ad_rev_adj as (

    select * from {{ ref("stg__longs__claim__ad_rev_adj") }}

),

int__longs__claim_adj__ad_rev_daily as (

    select
        report_date,
        date_key,
        asset_channel_id,
        asset_id,
        sum(partner_revenue) as asset_revenue,
            {{
                dbt_utils.generate_surrogate_key(
                    ["report_date", "date_key", "asset_channel_id", "asset_id"]
                )
            }} as surrogate_key
    from stg__longs__claim__ad_rev_adj
    group by report_date, date_key, asset_channel_id, asset_id

)

select *
from int__longs__claim_adj__ad_rev_daily
