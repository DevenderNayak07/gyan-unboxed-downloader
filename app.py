
import os
import re
import time
import secrets
import tempfile
import shutil
import asyncio
from pathlib import Path
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from parth_dl import download

ORIGIN = os.getenv(
    "ALLOWED_ORIGIN",
    "https://gyanunboxedd.blogspot.com"
)
TURNSTILE_SECRET = os.getenv("TURNSTILE_SECRET_KEY", "")
SITE_HOST = urlsplit(ORIGIN).hostname

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ORIGIN],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

executor = ThreadPoolExecutor(max_workers=2)
jobs = {}
JOB_TTL_SECONDS = 900
MAX_ACTIVE_JOBS = 4


class DownloadRequest(BaseModel):
    url: str = Field(min_length=1, max_length=2048)
    turnstileToken: str = Field(min_length=1, max_length=4096)


def validate_instagram_url(value: str) -> str:
    try:
        parsed = urlsplit(value.strip())
        host = (parsed.hostname or "").lower()
        path = parsed.path.strip("/")
    except ValueError:
        raise HTTPException(400, "Invalid URL.")

    allowed_hosts = {
        "instagram.com",
        "www.instagram.com",
        "m.instagram.com",
    }

    if (
        parsed.scheme != "https"
        or host not in allowed_hosts
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise HTTPException(
            400, "Enter a valid HTTPS Instagram URL."
        )

    parts = path.split("/")
    if (
        len(parts) != 2
        or parts[0] not in {"reel", "reels", "p"}
        or not re.fullmatch(r"[A-Za-z0-9_-]+", parts[1])
    ):
        raise HTTPException(
            400, "Only public Instagram Reel or post URLs are supported."
        )

    return f"https://www.instagram.com/{parts[0]}/{parts[1]}/"


async def verify_turnstile(token: str, request: Request) -> bool:
    if not TURNSTILE_SECRET:
        raise HTTPException(
            503, "The anti-bot protection is not configured yet."
        )

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            response = await client.post(
                "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                data={
                    "secret": TURNSTILE_SECRET,
                    "response": token,
                    "remoteip": request.client.host if request.client else "",
                },
            )
            response.raise_for_status()
            result = response.json()
    except Exception:
        raise HTTPException(
            503, "Anti-bot verification is temporarily unavailable."
        )

    return (
        result.get("success") is True
        and result.get("hostname") == SITE_HOST
    )


def cleanup_expired_jobs():
    now = time.time()
    for job_id, job in list(jobs.items()):
        if now - job["created"] > JOB_TTL_SECONDS:
            shutil.rmtree(job["directory"], ignore_errors=True)
            jobs.pop(job_id, None)


def perform_download(job_id: str, url: str, directory: str):
    try:
        result = download(
            url,
            output_path=directory + os.sep,
            quality="best",
        )

        returned = [result] if isinstance(result, str) else result
        files = []

        for item in returned or []:
            path = Path(item).resolve()
            root = Path(directory).resolve()

            if (
                path.is_file()
                and path.is_relative_to(root)
                and path.stat().st_size > 0
            ):
                files.append(path)

        if not files:
            raise RuntimeError("No downloadable media was returned.")

        jobs[job_id]["files"] = files
        jobs[job_id]["state"] = "complete"

    except Exception:
        shutil.rmtree(directory, ignore_errors=True)
        jobs[job_id]["state"] = "failed"
        jobs[job_id]["error"] = (
            "Download failed. The post may be private, unavailable, "
            "or unsupported by the downloader."
        )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/download")
async def start_download(data: DownloadRequest, request: Request):
    if request.headers.get("origin") != ORIGIN:
        raise HTTPException(403, "Request origin not allowed.")

    url = validate_instagram_url(data.url)

    if not await verify_turnstile(data.turnstileToken, request):
        raise HTTPException(403, "Anti-bot verification failed.")

    cleanup_expired_jobs()

    active = sum(
        job["state"] == "processing"
        for job in jobs.values()
    )
    if active >= MAX_ACTIVE_JOBS:
        raise HTTPException(
            429, "Too many active downloads. Try again shortly."
        )

    job_id = secrets.token_urlsafe(24)
    directory = tempfile.mkdtemp(prefix="gu-download-")

    jobs[job_id] = {
        "created": time.time(),
        "state": "processing",
        "directory": directory,
        "files": [],
    }

    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        executor, perform_download, job_id, url, directory
    )

    return {"job_id": job_id, "status": "processing"}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str, request: Request):
    if request.headers.get("origin") != ORIGIN:
        raise HTTPException(403, "Request origin not allowed.")

    cleanup_expired_jobs()
    job = jobs.get(job_id)

    if not job:
        raise HTTPException(404, "Job expired or not found.")

    result = {"status": job["state"]}

    if job["state"] == "complete":
        result["files"] = [
            {
                "name": f"media-{i + 1}{path.suffix[:10]}",
                "url": f"/api/files/{job_id}/{i}",
            }
            for i, path in enumerate(job["files"])
        ]
    elif job["state"] == "failed":
        result["error"] = job.get("error", "Download failed.")

    return result


@app.get("/api/files/{job_id}/{index}")
def get_file(job_id: str, index: int, request: Request):
    if request.headers.get("origin") != ORIGIN:
        raise HTTPException(403, "Request origin not allowed.")

    cleanup_expired_jobs()
    job = jobs.get(job_id)

    if not job or job["state"] != "complete":
        raise HTTPException(404, "File not found or expired.")

    if index < 0 or index >= len(job["files"]):
        raise HTTPException(404, "File not found.")

    path = job["files"][index]
    if not path.is_file():
        raise HTTPException(404, "File has expired.")

    return FileResponse(
        path,
        filename=f"instagram-media-{index + 1}{path.suffix[:10]}",
    )
