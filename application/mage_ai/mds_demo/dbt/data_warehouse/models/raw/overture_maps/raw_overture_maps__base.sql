{{
    config(
        materialized = 'view',
        schema       = 'raw'
    )
}}

{{ overture_maps_base_typed_scan() }}
