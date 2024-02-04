{% macro clean_phone_number(phone_numbers) %}

    case when length(translate({{ phone_numbers }}, '(, ), ., ,-', '')) < 10 then null else phone end
    



{% endmacro %}
