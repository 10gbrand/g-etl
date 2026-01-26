"""G-ETL Web Interface - FastAPI application for running pipelines."""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from scripts.admin.models.dataset import DatasetConfig, DatasetStatus
from scripts.admin.services.pipeline_runner import MockPipelineRunner, PipelineRunner


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
