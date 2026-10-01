{# Use the custom schema as-is (gold) instead of <target_schema>_<custom_schema> #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{%- endmacro %}
