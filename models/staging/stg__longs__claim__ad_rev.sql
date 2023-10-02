with source as (
    select * from {{ source('raw_csv', 'claim_raw') }}
),

renamed as (

    select
        adjustment_type,
        day as date_key,
        country as country_code,
        video_id,
        {{ clean_channel_id('channel_id') }} as channel_id,
        asset_id,
        {{ clean_channel_id('asset_channel_id') }} as asset_channel_id,
        asset_type,
        custom_id,
        content_type,
        policy,
        claim_type,
        claim_origin,
        owned_views,
        {{ zeroed('youtube_revenue_split__auction') }},
        {{ zeroed('youtube_revenue_split__reserved') }},
        {{ zeroed('youtube_revenue_split__partner_sold_youtube_served') }},
        {{ zeroed('youtube_revenue_split__partner_sold_partner_served') }},
        {{ zeroed('youtube_revenue_split') }},
        {{ zeroed('partner_revenue__auction') }},
        {{ zeroed('partner_revenue__reserved') }},
        {{ zeroed('partner_revenue__partner_sold_youtube_served') }},
        {{ zeroed('partner_revenue__partner_sold_partner_served') }},
        {{ zeroed('partner_revenue') }},
        {# Partition #}
        report_date_key

    from source

)

select * from renamed
