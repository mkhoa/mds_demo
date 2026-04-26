CREATE EXTENSION IF NOT EXISTS pg_duckdb;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;

-- spatial: covers GeoJSON, GeoTIFF (via ST_Read / GDAL), and geometry functions
SELECT duckdb.install_extension('spatial');
SELECT duckdb.autoload_extension('spatial');

-- h3: Uber H3 hierarchical geospatial indexing (community extension)
SELECT duckdb.install_extension('h3', repository := 'community');
SELECT duckdb.autoload_extension('h3');

