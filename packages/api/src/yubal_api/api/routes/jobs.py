"""Jobs API endpoints.

Handles job lifecycle: creation, listing, cancellation, and deletion.
Jobs are processed sequentially in FIFO order.
"""
import asyncio
from collections.abc import AsyncIterator

from fastapi import APIRouter, status
from fastapi.responses import StreamingResponse

from yubal_api.api.deps import (
    JobEventBusDep,
    JobExecutorDep,
    JobStoreDep,
)
from yubal_api.api.exceptions import (
    ErrorResponse,
    JobConflictError,
    JobNotFoundError,
    QueueFullError,
    ArtistNotFound,
)
from yubal_api.domain.job import Job
from yubal_api.schemas.jobs import (
    CancelJobResponse,
    ClearJobsResponse,
    CreateJobRequest,
    JobCreatedResponse,
    JobsResponse,
    SnapshotEvent,
)
from yubal_api.services.job_store import JobStore

from yubal_api.api.deps import PlaylistInfoServiceDep

from yubal.utils import parse_artist_id

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _get_job_or_raise(job_store: JobStore, job_id: str) -> Job:
    """Get job by ID or raise JobNotFoundError."""
    if not (job := job_store.get(job_id)):
        raise JobNotFoundError(job_id)
    return job


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses={409: {"model": ErrorResponse, "description": "Queue is full"}},
)
async def create_job(
    request: CreateJobRequest,
    job_executor: JobExecutorDep,
    playlist_info: PlaylistInfoServiceDep,
) -> JobCreatedResponse:
    """Create a new sync job.

    Jobs are queued and executed sequentially. Returns 409 if queue is full.
    """
    def _create_job_or_fail(url, max_items):
        job = job_executor.create_and_start_job(url, max_items)
        if job is None:
            raise QueueFullError()
        return job

    def _create_artist_jobs(artist_id: str, max_items: int | None) -> str | None:
        """Create jobs for all artist albums. Returns batch ID or None."""
        # Try direct get_artist first (handles case-insensitive)
        try:
            search_results = playlist_info._client._ytm.search(artist_id, filter="artists", limit=1)
        except Exception:
            artist = None

        if not search_results:
            try:
                artist = playlist_info._client._ytm.get_artist(artist_id)
            except Exception:
                artist = None
        else:
            artist_browseId = search_results[0]["browseId"]
            artist = playlist_info._client._ytm.get_artist(artist_browseId)

        if not artist or "albums" not in artist:
            raise ArtistNotFound()

        artist_albums = artist["albums"]["results"]

        artist_albums_browseId = artist["albums"]["browseId"]
        if artist_albums_browseId is not None:
            artist_albums_params = artist["albums"]["params"]
            artist_albums = playlist_info._client._ytm.get_artist_albums(
                artist_albums_browseId, params=artist_albums_params
            )

        for album in artist_albums:
            playlistId = album.get("playlistId") or album["audioPlaylistId"]
            album_url = f"https://music.youtube.com/playlist?list={playlistId}"
            job_executor.create_and_start_job(album_url, max_items)
        return f"{artist_id}-batch"

    artist_id = parse_artist_id(request.url)
    if artist_id:
        batch_id = _create_artist_jobs(artist_id, request.max_items)
        if batch_id:
            return JobCreatedResponse(id=batch_id)

    job = _create_job_or_fail(request.url, request.max_items)
    return JobCreatedResponse(id=job.id)


@router.get("")
async def list_jobs(job_store: JobStoreDep) -> JobsResponse:
    """List all jobs (oldest first, FIFO order)."""
    jobs = job_store.get_all()
    return JobsResponse(jobs=jobs)


@router.post(
    "/{job_id}/cancel",
    responses={
        404: {"model": ErrorResponse, "description": "Job not found"},
        409: {"model": ErrorResponse, "description": "Job already finished"},
    },
)
async def cancel_job(
    job_id: str,
    job_store: JobStoreDep,
    job_executor: JobExecutorDep,
) -> CancelJobResponse:
    """Cancel a running or queued job."""
    job = _get_job_or_raise(job_store, job_id)

    if job.status.is_finished:
        raise JobConflictError("Job already finished", job_id=job_id)

    # Signal cancellation via cancel token if job is running
    job_executor.cancel_job(job_id)

    success = job_store.cancel(job_id)
    if not success:
        raise JobConflictError("Could not cancel job", job_id=job_id)

    return CancelJobResponse()


@router.delete("")
async def clear_jobs(job_store: JobStoreDep) -> ClearJobsResponse:
    """Clear all completed/failed/cancelled jobs.

    Running and queued jobs are not affected.
    """
    count = job_store.clear_finished()
    return ClearJobsResponse(cleared=count)


@router.delete(
    "/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"model": ErrorResponse, "description": "Job not found"},
        409: {"model": ErrorResponse, "description": "Cannot delete running job"},
    },
)
async def delete_job(job_id: str, job_store: JobStoreDep) -> None:
    """Delete a completed, failed, or cancelled job.

    Running or queued jobs cannot be deleted.
    """
    job = _get_job_or_raise(job_store, job_id)

    if not job.status.is_finished:
        raise JobConflictError("Cannot delete a running or queued job", job_id=job_id)

    job_store.delete(job_id)


HEARTBEAT_INTERVAL = 30.0


@router.get(
    "/sse",
    response_class=StreamingResponse,
    summary="Stream job events via SSE",
    description=(
        "On connect, sends a snapshot event with all current jobs, "
        "then streams events as they occur. "
        "Heartbeat comments sent every 30s."
    ),
)
async def stream_jobs(
    job_store: JobStoreDep, job_event_bus: JobEventBusDep
) -> StreamingResponse:
    """Stream job events via Server-Sent Events."""
    bus = job_event_bus

    async def event_generator() -> AsyncIterator[str]:
        async with bus.subscribe() as queue:
            # Subscribe first, then snapshot (events queue up correctly)
            jobs = job_store.get_all()
            snapshot = SnapshotEvent(jobs=jobs)
            yield f"data: {snapshot.model_dump_json(by_alias=True)}\n\n"

            while True:
                try:
                    data = await asyncio.wait_for(
                        queue.get(), timeout=HEARTBEAT_INTERVAL
                    )
                    yield f"data: {data}\n\n"
                except TimeoutError:
                    yield ": heartbeat\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
