{% macro date_key_to_date(date_key) %}
    -- postgres implementation
    -- case
    --     when {{ date_key }} is null
    --     then null
    --     else to_date(cast({{ date_key }} as varchar), 'YYYYMMDD')
    -- end
    -- bq
    parse_date("%Y%m%d", cast ({{ date_key }} as string))
{% endmacro %} 