"""Delade tjänster för G-ETL.

Pipeline-runner och databashantering som används av CLI, TUI och QGIS-plugin.
"""

from g_etl.services.db_session import (
    cleanup_all_databases,
    cleanup_all_logs,
    cleanup_all_parquet,
    cleanup_data_subdirs,
    cleanup_old_databases,
    get_current_db_path,
    get_data_stats,
    get_latest_db_path,
    get_session_db_path,
    init_database,
    list_databases,
    update_current_db,
)
from g_etl.services.pipeline_runner import (
    MockPipelineRunner,
    ParallelExtractResult,
    PipelineEvent,
    PipelineRunner,
)

__all__ = [
    "MockPipelineRunner",
    "ParallelExtractResult",
    "PipelineEvent",
    "PipelineRunner",
    "cleanup_all_databases",
    "cleanup_all_logs",
    "cleanup_all_parquet",
    "cleanup_data_subdirs",
    "cleanup_old_databases",
    "get_current_db_path",
    "get_data_stats",
    "get_latest_db_path",
    "get_session_db_path",
    "init_database",
    "list_databases",
    "update_current_db",
]
