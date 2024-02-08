{% macro append_prefix_source(column_name, prefix) %}
    '{{ prefix }}' || lower(replace({{ column_name }}, ' ', '-'))
{% endmacro %}