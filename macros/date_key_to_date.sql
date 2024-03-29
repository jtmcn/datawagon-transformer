{% macro date_key_to_date(date_key) %}
    parse_date("%Y%m%d", cast ({{ date_key }} as string))
{% endmacro %} 