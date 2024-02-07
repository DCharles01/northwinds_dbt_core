{% macro derive_company_id(company_name, prefix) %}
    '{{ prefix }}' || lower(replace({{ company_name }}, ' ', '-'))
{% endmacro %}