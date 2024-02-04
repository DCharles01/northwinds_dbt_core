{% macro format_phone_number(cleaned_phone) %}
'(' || substring(translate({{ cleaned_phone }}, '(, ), ., ,-', ''), 1, 3) || ')' 
    || ' ' || substring(translate({{ cleaned_phone }}, '(, ), ., ,-', ''), 4, 3) 
    || '-' || substring(translate({{ cleaned_phone }}, '(, ), ., ,-', ''), 7, 4)

{% endmacro %}
