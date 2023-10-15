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

    ),

    incremental as (
        
        select 
            adjustment_type,
            date_key,
            country_code,
            asset_id,
            asset_labels,
            asset_channel_id,
            custom_id,
            asset_title,
            owned_watchtime,
            partner_revenue,
            report_date_key -- partition
        from renamed

        {% if is_incremental() %}

        where report_date_key > (select max(report_date_key) from {{ this }})

        {% endif %}

    )


select *
from incremental
