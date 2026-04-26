{{
    config(
        materialized = 'view',
        schema       = 'raw'
    )
}}

{{ overture_maps_places_typed_scan() }}
