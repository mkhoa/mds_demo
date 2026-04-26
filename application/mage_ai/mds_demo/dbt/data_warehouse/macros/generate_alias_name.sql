{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is not none -%}
        {{ custom_alias_name | trim }}
    {%- else -%}
        {%- set name = node.name -%}
        {%- set ns = namespace(alias=name) -%}
        {%- for prefix in ['raw_', 'stg_', 'bdh_', 'adl_'] -%}
            {%- if name.startswith(prefix) -%}
                {%- set ns.alias = name[prefix | length:] -%}
            {%- endif -%}
        {%- endfor -%}
        {{ ns.alias | replace('__', '_') }}
    {%- endif -%}
{%- endmacro %}
