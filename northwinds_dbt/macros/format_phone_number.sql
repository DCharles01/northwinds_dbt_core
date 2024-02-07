{% macro format_phone_number(phone_number) %}
    {% set cleaned_phone = clean_phone_number(phone_number) %}
    
'(' || substring(translate({{ cleaned_phone }}, '(, ), ., ,-', ''), 1, 3) || ')' 
    || ' ' || substring(translate({{ cleaned_phone }}, '(, ), ., ,-', ''), 4, 3) 
    || '-' || substring(translate({{ cleaned_phone }}, '(, ), ., ,-', ''), 7, 4)

{% endmacro %}
