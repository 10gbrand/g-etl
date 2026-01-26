"""G-ETL Web Interface - FastAPI application for running pipelines."""

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

import duckdb
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from pyproj import Transformer
from sse_starlette.sse import EventSourceResponse

from scripts.admin.models.dataset import DatasetConfig, DatasetStatus
from scripts.admin.services.pipeline_runner import MockPipelineRunner, PipelineRunner

# Database path
DB_PATH = "data/warehouse.duckdb"

# Koordinattransformerare cache
_transformers: dict[tuple[str, str], Transformer] = {}


def get_transformer(source_crs: str, target_crs: str) -> Transformer:
    """Hämta eller skapa en koordinattransformerare."""
    key = (source_crs, target_crs)
    if key not in _transformers:
        _transformers[key] = Transformer.from_crs(source_crs, target_crs, always_xy=True)
    return _transformers[key]


def transform_coordinates(coords: list, transformer: Transformer) -> list:
    """Transformera koordinater rekursivt (hanterar Point, LineString, Polygon, etc.)."""
    if not coords:
        return coords

    # Om första elementet är ett tal, är detta en koordinatpunkt [x, y]
    if isinstance(coords[0], (int, float)):
        x, y = coords[0], coords[1]
        new_x, new_y = transformer.transform(x, y)
        return [new_x, new_y] + coords[2:]  # Behåll eventuell z-koordinat

    # Annars är det en lista av koordinater/ringar
    return [transform_coordinates(c, transformer) for c in coords]


def transform_geojson(geojson: dict, source_crs: str) -> dict:
    """Transformera en GeoJSON-geometri från source_crs till WGS84."""
    if source_crs == "EPSG:4326":
        return geojson  # Redan i rätt CRS

    transformer = get_transformer(source_crs, "EPSG:4326")

    geom_type = geojson.get("type")
    coords = geojson.get("coordinates")

    if not coords:
        return geojson

    new_coords = transform_coordinates(coords, transformer)

    return {**geojson, "coordinates": new_coords}


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class RunRequest(BaseModel):
    dataset_ids: list[str] | None = None
    typ: str | None = None
    run_all: bool = False
    mock: bool = False


@dataclass
class PipelineJob:
    id: str
    status: JobStatus = JobStatus.PENDING
    datasets: list[str] = field(default_factory=list)
    completed: int = 0
    total: int = 0
    logs: list[str] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None


# Global state
jobs: dict[str, PipelineJob] = {}
active_job_id: str | None = None
log_subscribers: list[asyncio.Queue] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifespan handler."""
    yield
    # Cleanup on shutdown
    for q in log_subscribers:
        await q.put(None)


app = FastAPI(title="G-ETL Admin", lifespan=lifespan)

# Serve static files (Leaflet, etc.)
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def load_config() -> DatasetConfig:
    """Load dataset configuration."""
    return DatasetConfig.load("config/datasets.yml")


async def broadcast_log(message: str, job_id: str | None = None):
    """Send log message to all subscribers."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = {"time": timestamp, "message": message, "job_id": job_id}

    for q in log_subscribers:
        try:
            q.put_nowait(log_entry)
        except asyncio.QueueFull:
            pass


async def run_pipeline_job(job: PipelineJob, mock: bool = False):
    """Run pipeline job in background."""
    global active_job_id

    config = load_config()
    job.status = JobStatus.RUNNING
    job.started_at = datetime.now()
    active_job_id = job.id

    await broadcast_log(f"Startar jobb {job.id[:8]}...", job.id)

    # Create runner
    runner = MockPipelineRunner() if mock else PipelineRunner()

    try:
        # Get datasets to run
        datasets = []
        for dataset_id in job.datasets:
            ds = config.get_by_id(dataset_id)
            if ds and ds.enabled:
                datasets.append(ds)

        job.total = len(datasets)
        await broadcast_log(f"Kör {job.total} dataset(s)", job.id)

        # Get current event loop for thread-safe logging
        loop = asyncio.get_running_loop()

        def log_callback(msg: str):
            """Thread-safe log callback."""
            asyncio.run_coroutine_threadsafe(broadcast_log(msg, job.id), loop)

        # Run each dataset
        for ds in datasets:
            await broadcast_log(f"Startar {ds.name}...", job.id)

            success = await runner.run_dataset(
                dataset_config=ds.config,
                on_log=log_callback,
            )

            if success:
                await broadcast_log(f"Klar: {ds.name}", job.id)
            else:
                await broadcast_log(f"Fel: {ds.name}", job.id)

            job.completed += 1

        # Run transforms
        if job.completed > 0:
            await broadcast_log("Kör SQL-transformationer...", job.id)
            await runner.run_transforms(on_log=log_callback)

        job.status = JobStatus.COMPLETED
        await broadcast_log(f"Klart! {job.completed}/{job.total} datasets körda.", job.id)

    except Exception as e:
        job.status = JobStatus.FAILED
        await broadcast_log(f"Fel: {e}", job.id)

    finally:
        job.completed_at = datetime.now()
        active_job_id = None
        runner.close()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main page with dataset list and controls."""
    config = load_config()
    types = sorted(config.get_types())

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "datasets": config.datasets,
            "types": types,
            "enabled_count": len(config.get_enabled()),
            "active_job": jobs.get(active_job_id) if active_job_id else None,
        },
    )


@app.get("/api/datasets")
async def get_datasets(typ: str | None = None):
    """Get datasets, optionally filtered by type."""
    config = load_config()

    if typ:
        datasets = config.get_by_type(typ)
    else:
        datasets = config.get_enabled()

    return {
        "datasets": [
            {
                "id": d.id,
                "name": d.name,
                "description": d.description,
                "typ": d.typ,
                "enabled": d.enabled,
                "plugin": d.plugin,
            }
            for d in datasets
        ],
        "types": sorted(config.get_types()),
    }


@app.post("/api/run")
async def run_datasets(req: RunRequest):
    """Start a pipeline run."""
    global active_job_id

    if active_job_id:
        return {"error": "En körning pågår redan", "job_id": active_job_id}

    config = load_config()

    # Determine which datasets to run
    if req.run_all:
        datasets = config.get_enabled()
    elif req.typ:
        datasets = config.get_by_type(req.typ)
    elif req.dataset_ids:
        datasets = [config.get_by_id(did) for did in req.dataset_ids]
        datasets = [d for d in datasets if d and d.enabled]
    else:
        return {"error": "Inga datasets valda"}

    if not datasets:
        return {"error": "Inga giltiga datasets att köra"}

    # Create job
    job_id = str(uuid.uuid4())
    job = PipelineJob(id=job_id, datasets=[d.id for d in datasets])
    jobs[job_id] = job

    # Start background task
    asyncio.create_task(run_pipeline_job(job, mock=req.mock))

    return {
        "job_id": job_id,
        "status": "started",
        "datasets": [d.name for d in datasets],
    }


@app.get("/api/job/{job_id}")
async def get_job(job_id: str):
    """Get job status."""
    job = jobs.get(job_id)
    if not job:
        return {"error": "Job not found"}

    return {
        "id": job.id,
        "status": job.status.value,
        "completed": job.completed,
        "total": job.total,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


@app.post("/api/stop")
async def stop_job():
    """Stop the current running job."""
    global active_job_id
    if not active_job_id:
        return {"error": "Ingen körning pågår"}

    await broadcast_log("Stoppar...", active_job_id)
    # Note: Full stop implementation would need runner reference
    return {"status": "stopping"}


@app.get("/api/logs/stream")
async def stream_logs(request: Request):
    """Server-sent events stream for logs."""

    async def event_generator() -> AsyncGenerator:
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        log_subscribers.append(queue)

        try:
            while True:
                if await request.is_disconnected():
                    break

                try:
                    log_entry = await asyncio.wait_for(queue.get(), timeout=30)
                    if log_entry is None:
                        break
                    yield {
                        "event": "log",
                        "data": f"{log_entry['time']} {log_entry['message']}",
                    }
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": ""}

        finally:
            log_subscribers.remove(queue)

    return EventSourceResponse(event_generator())


# =============================================================================
# Data Explorer API
# =============================================================================


def get_db_connection() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection with spatial extension loaded."""
    import os

    db_path = os.environ.get("DB_PATH", DB_PATH)

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Databasen finns inte: {db_path}")

    conn = duckdb.connect(db_path, read_only=True)
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except Exception as e:
        print(f"[Explorer] Warning: spatial extension: {e}")
    return conn


@app.get("/explorer", response_class=HTMLResponse)
async def explorer(request: Request):
    """Data Explorer page with table viewer and map."""
    return templates.TemplateResponse("explorer.html", {"request": request})


@app.get("/api/explorer/tables")
async def list_tables():
    """List all tables in all schemas with column info."""
    try:
        conn = get_db_connection()
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Kunde inte ansluta till databas: {e}"}
        )

    try:
        # Get all tables from information_schema
        tables_query = """
            SELECT
                table_schema as schema,
                table_name as name
            FROM information_schema.tables
            WHERE table_schema IN ('raw', 'staging', 'mart')
            ORDER BY table_schema, table_name
        """
        tables = conn.execute(tables_query).fetchall()

        result = []
        for schema, name in tables:
            # Get column info for each table
            cols_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{name}'
                ORDER BY ordinal_position
            """
            columns = conn.execute(cols_query).fetchall()

            # Check if table has geometry column
            has_geometry = any("geometry" in col[1].lower() for col in columns)

            # Get row count
            try:
                count = conn.execute(
                    f'SELECT COUNT(*) FROM "{schema}"."{name}"'
                ).fetchone()[0]
            except Exception:
                count = 0

            result.append(
                {
                    "schema": schema,
                    "name": name,
                    "columns": [{"name": c[0], "type": c[1]} for c in columns],
                    "has_geometry": has_geometry,
                    "row_count": count,
                }
            )

        return {"tables": result}
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Databasfel: {e}"}
        )
    finally:
        conn.close()


@app.get("/api/explorer/tables/{schema}/{table}")
async def get_table_data(
    schema: str,
    table: str,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """Get table data with pagination."""
    try:
        conn = get_db_connection()
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Kunde inte ansluta till databas: {e}"}
        )

    try:
        # Validate schema
        if schema not in ("raw", "staging", "mart"):
            return JSONResponse(
                status_code=400, content={"error": "Ogiltigt schema"}
            )

        # Get column info to find geometry columns
        cols_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            ORDER BY ordinal_position
        """
        columns = conn.execute(cols_query).fetchall()

        if not columns:
            return JSONResponse(
                status_code=404, content={"error": "Tabellen hittades inte"}
            )

        # Build SELECT with geometry conversion
        select_parts = []
        for col_name, col_type in columns:
            if "geometry" in col_type.lower():
                # Convert geometry to WKT for display (truncated for large geometries)
                select_parts.append(
                    f'LEFT(ST_AsText("{col_name}"), 200) as "{col_name}"'
                )
            else:
                select_parts.append(f'"{col_name}"')

        select_clause = ", ".join(select_parts)
        query = f'SELECT {select_clause} FROM "{schema}"."{table}" LIMIT {limit} OFFSET {offset}'

        result = conn.execute(query).fetchall()
        col_names = [c[0] for c in columns]

        # Get total count
        total = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]

        # Convert rows, handling special types
        rows = []
        for row in result:
            row_dict = {}
            for i, val in enumerate(row):
                col_name = col_names[i]
                if val is None:
                    row_dict[col_name] = None
                elif hasattr(val, "isoformat"):
                    row_dict[col_name] = val.isoformat()
                elif isinstance(val, bytes):
                    row_dict[col_name] = f"<binary {len(val)} bytes>"
                elif not isinstance(val, (str, int, float, bool)):
                    row_dict[col_name] = str(val)
                else:
                    row_dict[col_name] = val
            rows.append(row_dict)

        return {
            "columns": [{"name": c[0], "type": c[1]} for c in columns],
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Databasfel: {e}"}
        )
    finally:
        conn.close()


@app.get("/api/explorer/tables/{schema}/{table}/geojson")
async def get_table_geojson(
    schema: str,
    table: str,
    geometry_column: str = Query(default="geometry"),
    source_crs: str = Query(default="EPSG:3006"),
    limit: int = Query(default=1000, le=10000),
):
    """Export table as GeoJSON for map visualization.

    Args:
        source_crs: Källans koordinatsystem (EPSG:3006, EPSG:4326, EPSG:3857, etc.)
    """
    try:
        conn = get_db_connection()
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Kunde inte ansluta till databas: {e}"}
        )

    try:
        # Validate schema
        if schema not in ("raw", "staging", "mart"):
            return JSONResponse(
                status_code=400, content={"error": "Ogiltigt schema"}
            )

        # Get all columns except geometry
        cols_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            ORDER BY ordinal_position
        """
        columns = conn.execute(cols_query).fetchall()

        if not columns:
            return JSONResponse(
                status_code=404, content={"error": "Tabellen hittades inte"}
            )

        # Find geometry column
        geom_col = None
        for col_name, col_type in columns:
            if "geometry" in col_type.lower():
                geom_col = col_name
                break

        if not geom_col:
            return JSONResponse(
                status_code=400, content={"error": "Ingen geometrikolumn hittades"}
            )

        # Build property columns (non-geometry)
        prop_cols = [c[0] for c in columns if "geometry" not in c[1].lower()]

        # Query: hämta geometri som GeoJSON (utan transformation i DuckDB)
        # Vi gör transformationen i Python med pyproj istället
        prop_select = ", ".join([f'"{c}"' for c in prop_cols]) if prop_cols else "''"

        query = f"""
            SELECT
                ST_AsGeoJSON("{geom_col}") as geom,
                {prop_select}
            FROM "{schema}"."{table}"
            WHERE "{geom_col}" IS NOT NULL
            LIMIT {limit}
        """

        result = conn.execute(query).fetchall()

        # Build GeoJSON FeatureCollection
        features = []
        for row in result:
            geom_json = row[0]
            props = dict(zip(prop_cols, row[1:])) if prop_cols else {}

            # Convert non-serializable types
            for k, v in props.items():
                if hasattr(v, "isoformat"):
                    props[k] = v.isoformat()
                elif not isinstance(v, (str, int, float, bool, type(None))):
                    props[k] = str(v)

            if geom_json:
                geom = json.loads(geom_json)

                # Transformera koordinater med pyproj
                try:
                    geom = transform_geojson(geom, source_crs)
                except Exception as transform_err:
                    print(f"[GeoJSON] Transform error: {transform_err}")
                    # Använd otransformerad geometri som fallback

                features.append(
                    {
                        "type": "Feature",
                        "geometry": geom,
                        "properties": props,
                    }
                )

        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "table": f"{schema}.{table}",
                "total_features": len(features),
                "limit": limit,
                "source_crs": source_crs,
                "target_crs": "EPSG:4326",
            },
        }
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"GeoJSON-fel: {e}"}
        )
    finally:
        conn.close()


@app.post("/api/explorer/query")
async def run_query(request: Request):
    """Run a custom SQL query (read-only)."""
    body = await request.json()
    sql = body.get("sql", "").strip()

    if not sql:
        return JSONResponse(status_code=400, content={"error": "Ingen SQL angiven"})

    # Basic safety check - only allow SELECT
    sql_upper = sql.upper()
    if not sql_upper.startswith("SELECT"):
        return JSONResponse(
            status_code=400, content={"error": "Endast SELECT-frågor tillåtna"}
        )

    conn = get_db_connection()
    try:
        # Add LIMIT if not present
        if "LIMIT" not in sql_upper:
            sql = f"{sql} LIMIT 1000"

        result = conn.execute(sql).fetchall()
        columns = [desc[0] for desc in conn.description]

        # Convert rows to dicts
        rows = []
        for row in result:
            row_dict = {}
            for i, val in enumerate(row):
                if hasattr(val, "isoformat"):
                    row_dict[columns[i]] = val.isoformat()
                elif not isinstance(val, (str, int, float, bool, type(None))):
                    row_dict[columns[i]] = str(val)
                else:
                    row_dict[columns[i]] = val
            rows.append(row_dict)

        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    finally:
        conn.close()


def main():
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(description="G-ETL Web Interface")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    args = parser.parse_args()

    uvicorn.run(
        "scripts.web.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
