{% macro find_datatypes(model_name) %}
    {% set relation = ref(model_name) %}
    {% set cols = adapter.get_columns_in_relation(relation) %}
    {% for col in cols %}
        {{ log("- name: " ~ col.name | lower, info=True) }}
        {{ log("  data_type: " ~ col.dtype | lower, info=True) }}
    {% endfor %}

{% endmacro %}