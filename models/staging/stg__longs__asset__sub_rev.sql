with
    source as (select * from {{ source("raw_csv", "red_rawdata_asset") }}),

    renamed as (

        select
            adjustment_type,
            day as date_key,
            country as country_code,
            asset_id,
            asset_labels,
            {{ clean_channel_id("asset_channel_id") }} as asset_channel_id,
            custom_id,
            asset_title,
            owned_watchtime,
            partner_revenue,
            report_date_key -- partition
        from source

    )

select *
from renamed