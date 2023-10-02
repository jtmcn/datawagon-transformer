
{% macro clean_channel_id(channel_id) %}

        trim(
            case
                when upper(substring({{ channel_id }}, 1, 2)) != 'UC'
                    then concat('UC', {{ channel_id }})
                    else {{ channel_id }}
            end
        )

{% endmacro %}
