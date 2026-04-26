{{
    config(
        materialized = 'view',
        schema       = 'raw'
    )
}}

{{ overture_maps_division_area_typed_scan() }}
