{% macro get_product_types() %}
    {% set product_types = ['jaffle', 'beverage'] %}
    {{ return(product_types) }}
{% endmacro %}
