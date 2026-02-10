"""G-ETL Source Plugins."""

from g_etl.plugins.base import SourcePlugin
from g_etl.plugins.geopackage import GeoPackagePlugin
from g_etl.plugins.geopackage import clear_download_cache as _clear_gpkg_direct_cache
from g_etl.plugins.geoparquet import GeoParquetPlugin
from g_etl.plugins.lantmateriet import LantmaterietPlugin
from g_etl.plugins.wfs import WfsPlugin
from g_etl.plugins.wfs_geopandas import WfsGeopandasPlugin
from g_etl.plugins.zip_geopackage import ZipGeoPackagePlugin
from g_etl.plugins.zip_geopackage import clear_download_cache as _clear_gpkg_cache
from g_etl.plugins.zip_shapefile import ZipShapefilePlugin
from g_etl.plugins.zip_shapefile import clear_shapefile_cache as _clear_shp_cache


def clear_download_cache() -> None:
    """Rensa alla nedladdningscacher (GeoPackage och Shapefile)."""
    _clear_gpkg_cache()
    _clear_shp_cache()
    _clear_gpkg_direct_cache()


PLUGINS: dict[str, type[SourcePlugin]] = {
    "wfs": WfsPlugin,
    "wfs_geopandas": WfsGeopandasPlugin,  # Robust WFS för trasiga servrar
    "lantmateriet": LantmaterietPlugin,
    "geopackage": GeoPackagePlugin,
    "geoparquet": GeoParquetPlugin,
    "zip_geopackage": ZipGeoPackagePlugin,
    "zip_shapefile": ZipShapefilePlugin,
}

# mssql är optional - kräver pyodbc och libodbc
try:
    from g_etl.plugins.mssql import MssqlPlugin

    PLUGINS["mssql"] = MssqlPlugin
except ImportError:
    pass  # pyodbc/libodbc saknas


def get_plugin(plugin_name: str) -> SourcePlugin:
    """Hämta plugin-instans baserat på namn."""
    plugin_class = PLUGINS.get(plugin_name)
    if not plugin_class:
        raise ValueError(f"Okänd plugin: {plugin_name}. Tillgängliga: {list(PLUGINS.keys())}")
    return plugin_class()
