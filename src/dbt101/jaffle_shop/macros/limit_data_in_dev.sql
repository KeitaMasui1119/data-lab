{% macro limit_data_in_dev(column_name, days=3) %}
    {% if target.name == 'dev' %}
        where {{ column_name }} >= dateadd('day', -{{ days }}, current_date)
    {% endif %}
{% endmacro %}
