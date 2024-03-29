with 

source as (

    select * from {{ source('raw_csv', 'update_acid_mapping') }}

),

renamed as (

    select
        report_date_override,
        match_asset_id,
        to_asset_channel_id

    from source

)

select * from renamed
