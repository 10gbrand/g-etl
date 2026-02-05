"""G-ETL QGIS Plugin.

ETL-pipeline för svenska geodata med H3-indexering.
"""


def classFactory(iface):
    """QGIS Plugin entry point.

    Args:
        iface: QgisInterface instans för att interagera med QGIS.

    Returns:
        GETLPlugin instans.
    """
    from .g_etl_plugin import GETLPlugin

    return GETLPlugin(iface)
