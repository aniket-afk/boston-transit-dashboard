{% macro confidence_flag(count_column, threshold=30) %}
    case
        when {{ count_column }} >= {{ threshold }} then 'reliable'
        else 'low_sample'
    end
{% endmacro %}