{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {#-
    Override dbt's default schema naming behavior.

    Default behavior: {target_schema}_{custom_schema}
    Custom behavior: Use custom_schema as-is (ignore target_schema)

    This allows clean schema names:
    - +schema: silver → SILVER (not silver_silver)
    - +schema: gold → GOLD (not silver_gold)
    #}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
