{% macro zeroed(numeric_val) %}
    -- prevents postgres from using 0.00000000 for 0s in numeric which are annoying to read
    case
        when {{ numeric_val }} is null or {{ numeric_val }} = 0
        then 0
        else {{ numeric_val }} 
    end
{% endmacro %}
