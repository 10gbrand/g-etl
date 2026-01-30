"""G-ETL Source Plugins."""

from plugins.base import SourcePlugin
from plugins.geoparquet import GeoParquetPlugin
from plugins.lantmateriet import LantmaterietPlugin
from plugins.wfs import WfsPlugin
from plugins.zip_geopackage import ZipGeoPackagePlugin
from plugins.zip_shapefile import ZipShapefilePlugin

PLUGINS: dict[str, type[SourcePlugin]] = {
    "wfs": WfsPlugin,
    "lantmateriet": LantmaterietPlugin,
    "geoparquet": GeoParquetPlugin,
    "zip_geopackage": ZipGeoPackagePlugin,
    "zip_shapefile": ZipShapefilePlugin,
}

# mssql är optional - kräver pyodbc och libodbc
try:
    from plugins.mssql import MssqlPlugin
    PLUGINS["mssql"] = MssqlPlugin
except ImportError:
    pass  # pyodbc/libodbc saknas


def get_plugin(plugin_name: str) -> SourcePlugin:
    """Hämta plugin-instans baserat på namn."""
    plugin_class = PLUGINS.get(plugin_name)
    if not plugin_class:
        raise ValueError(f"Okänd plugin: {plugin_name}. Tillgängliga: {list(PLUGINS.keys())}")
    return plugin_class()
