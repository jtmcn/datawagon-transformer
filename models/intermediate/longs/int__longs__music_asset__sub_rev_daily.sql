with
    stg__longs__music_asset__sub_rev as (

        select * from {{ ref("stg__longs__music_asset__sub_rev") }}

    ),

    int__longs__music_asset__sub_rev_daily as (

        select
            report_date,
            date_key,
            asset_channel_id,
            asset_id,
            sum(partner_revenue) as asset_revenue,
            {{ dbt_utils.generate_surrogate_key([
                        "report_date", 
                        "date_key", 
                        "asset_channel_id", 
                        "asset_id"
                ]) }} as surrogate_key
        from stg__longs__music_asset__sub_rev
        group by report_date, date_key, asset_channel_id, asset_id

    )

select *
from int__longs__music_asset__sub_rev_daily
