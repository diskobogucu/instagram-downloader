import uuid
import json
import time
import asyncio
import subprocess
import logging
import secrets
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel
from typing import Literal
import yt_dlp

# ── Download logger ───────────────────────────────────
DOWNLOAD_LOG = Path("/var/log/videodrop-downloads.log")
dl_logger = logging.getLogger("videodrop.downloads")
dl_logger.setLevel(logging.INFO)
try:
    fh = logging.FileHandler(DOWNLOAD_LOG)
    fh.setFormatter(logging.Formatter("%(message)s"))
    dl_logger.addHandler(fh)
except Exception:
    pass  # if /var/log not writable (local dev), skip

def detect_platform(url: str) -> str:
    if "instagram.com" in url: return "Instagram"
    if "youtube.com" in url or "youtu.be" in url: return "YouTube"
    if "twitter.com" in url or "x.com" in url: return "Twitter"
    if "facebook.com" in url or "fb.watch" in url: return "Facebook"
    return "Unknown"

def log_download(ip: str, url: str, status: str, quality: str = ""):
    platform = detect_platform(url)
    entry = json.dumps({
        "ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "ip": ip,
        "platform": platform,
        "url": url,
        "quality": quality,
        "status": status,
    })
    dl_logger.info(entry)

# ── Admin auth ────────────────────────────────────────
security = HTTPBasic()
ADMIN_USER = "admin"
ADMIN_PASS = "Videodrop2026!"

def require_admin(credentials: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(credentials.username, ADMIN_USER)
    ok_pass = secrets.compare_digest(credentials.password, ADMIN_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=401, detail="Unauthorized",
                            headers={"WWW-Authenticate": "Basic"})

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
templates = Jinja2Templates(directory="templates")

STATIC_DIR = Path("static")
STATIC_DIR.mkdir(exist_ok=True)


@app.get("/favicon.svg", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.svg", media_type="image/svg+xml")


@app.get("/robots.txt", include_in_schema=False)
async def robots():
    return FileResponse("static/robots.txt", media_type="text/plain")


@app.get("/sitemap.xml", include_in_schema=False)
async def sitemap():
    return FileResponse("static/sitemap.xml", media_type="application/xml")

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

MAX_AGE_SECONDS = 1800

# request_id → progress dict
progress_store: dict = {}
# request_id → cancel flag
cancel_store: dict = {}
# IP → {flag, country}
geo_cache: dict = {}
# live admin SSE subscribers
live_subs: list = []

QUALITY_FORMATS = {
    "auto": "best[ext=mp4]/bestvideo[ext=mp4]+bestaudio/best",
    "2160": "bestvideo[height<=2160][ext=mp4]+bestaudio/best[height<=2160]/best",
    "1440": "bestvideo[height<=1440][ext=mp4]+bestaudio/best[height<=1440]/best",
    "1080": "bestvideo[height<=1080][ext=mp4]+bestaudio/best[height<=1080]/best",
    "720":  "bestvideo[height<=720][ext=mp4]+bestaudio/best[height<=720]/best",
    "480":  "bestvideo[height<=480][ext=mp4]+bestaudio/best[height<=480]/best",
}

SUPPORTED = ["instagram.com", "youtube.com", "youtu.be",
             "twitter.com", "x.com", "facebook.com", "fb.watch"]


class InfoRequest(BaseModel):
    url: str


class FetchRequest(BaseModel):
    url: str
    quality: str = "auto"


class ConvertRequest(BaseModel):
    file_id: str
    format: Literal["video", "mp3", "silent"]


async def get_geo(ip: str) -> dict:
    """Return {flag, country} for an IP, cached in geo_cache."""
    if ip in ("127.0.0.1", "::1", "localhost", "unknown"):
        return {"flag": "🏠", "country": "Local"}
    if ip in geo_cache:
        return geo_cache[ip]
    try:
        import urllib.request as urlreq
        loop = asyncio.get_event_loop()

        def _fetch():
            with urlreq.urlopen(
                f"http://ip-api.com/json/{ip}?fields=country,countryCode",
                timeout=3,
            ) as r:
                return json.loads(r.read())

        data = await loop.run_in_executor(None, _fetch)
        cc = data.get("countryCode", "")
        flag = (
            "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in cc.upper())
            if cc else "🌐"
        )
        result = {"flag": flag, "country": data.get("country", "")}
    except Exception:
        result = {"flag": "🌐", "country": ""}
    geo_cache[ip] = result
    return result


@app.middleware("http")
async def traffic_logger(request: Request, call_next):
    start = time.time()

    # Peek at body for download endpoints to capture the target URL
    dl_url = ""
    path = request.url.path
    if request.method == "POST" and path in ("/info", "/fetch"):
        try:
            raw = await request.body()  # Starlette caches this; route still reads fine
            dl_url = json.loads(raw).get("url", "")
        except Exception:
            pass

    response = await call_next(request)
    ms = round((time.time() - start) * 1000)

    skip = path in ("/admin/live",) or path.startswith("/progress/")
    if not skip and live_subs:
        ip = request.client.host if request.client else "unknown"
        geo = await get_geo(ip)
        event = {
            "ts": datetime.utcnow().strftime("%H:%M:%S"),
            "ip": ip,
            "flag": geo["flag"],
            "country": geo["country"],
            "method": request.method,
            "path": path,
            "status": response.status_code,
            "ms": ms,
            "dl_url": dl_url,
        }
        for q in live_subs[:]:
            try:
                q.put_nowait(event)
            except Exception:
                pass
    return response


def clean_old_files():
    now = time.time()
    for f in DOWNLOAD_DIR.iterdir():
        try:
            if now - f.stat().st_mtime > MAX_AGE_SECONDS:
                f.unlink()
        except Exception:
            pass


def find_file(stem: str) -> Path | None:
    for f in DOWNLOAD_DIR.iterdir():
        if f.stem == stem:
            return f
    return None


def run_ffmpeg(*args):
    subprocess.run(
        ["ffmpeg", *args, "-y", "-loglevel", "quiet"],
        check=True,
    )


async def do_download(request_id: str, file_id: str, url: str, ydl_opts: dict):
    """Background download task — updates progress_store as it goes."""

    def strip_ansi(s: str) -> str:
        import re
        return re.sub(r'\x1b\[[0-9;]*m', '', s).strip()

    def progress_hook(d):
        if cancel_store.get(request_id):
            raise Exception("cancelled")
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            downloaded = d.get("downloaded_bytes", 0)
            pct = round(downloaded / total * 100) if total > 0 else 0
            speed = strip_ansi(d.get("_speed_str", "") or "")
            eta   = strip_ansi(d.get("_eta_str",   "") or "")
            progress_store[request_id] = {
                "status": "downloading",
                "pct": min(pct, 98),
                "speed": speed,
                "eta": eta,
            }
        elif d["status"] == "finished":
            progress_store[request_id] = {"status": "processing", "pct": 99}

    ydl_opts["progress_hooks"] = [progress_hook]
    loop = asyncio.get_event_loop()

    try:
        def run():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=True)

        info = await loop.run_in_executor(None, run)

        downloaded = find_file(file_id)
        if not downloaded:
            progress_store[request_id] = {"status": "error", "message": "File could not be saved."}
            return

        title = info.get("title", "video")
        safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "-", "_")).strip() or "video"

        progress_store[request_id] = {
            "status": "done",
            "pct": 100,
            "file_id": file_id,
            "title": safe_title,
            "ext": downloaded.suffix.lstrip("."),
        }

    except yt_dlp.utils.DownloadError as e:
        msg = str(e).lower()
        if "private" in msg:
            err = "private"
        elif any(k in msg for k in ("inappropriate", "unavailable", "login", "age", "sign in")):
            err = "login"
        else:
            err = f"failed:{str(e)}"
        progress_store[request_id] = {"status": "error", "message": err}
    except Exception as e:
        if "cancelled" in str(e).lower():
            progress_store[request_id] = {"status": "cancelled"}
        else:
            progress_store[request_id] = {"status": "error", "message": f"failed:{str(e)}"}
    finally:
        cancel_store.pop(request_id, None)
        # Clean up partial file on cancel
        if progress_store.get(request_id, {}).get("status") == "cancelled":
            for f in DOWNLOAD_DIR.glob(f"{file_id}.*"):
                try: f.unlink()
                except Exception: pass


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/info")
@limiter.limit("20/minute")
async def get_video_info(request: Request, data: InfoRequest):
    url = data.url.strip()

    if not url:
        raise HTTPException(status_code=400, detail="URL cannot be empty.")
    if not any(d in url for d in SUPPORTED):
        raise HTTPException(status_code=400, detail="Unsupported platform.")

    is_twitter = "twitter.com" in url or "x.com" in url

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 30,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    if is_twitter:
        ydl_opts["extractor_args"] = {"twitter": {"api": ["syndication", "graphql"]}}

    cookies_file = Path("cookies.txt")
    if cookies_file.exists():
        ydl_opts["cookiefile"] = str(cookies_file)

    loop = asyncio.get_event_loop()

    def run():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(url, download=False)

    try:
        info = await loop.run_in_executor(None, run)
    except yt_dlp.utils.DownloadError as e:
        msg = str(e).lower()
        if "private" in msg:
            detail = "private"
        elif any(k in msg for k in ("inappropriate", "unavailable", "login", "age", "sign in")):
            detail = "login"
        else:
            detail = f"failed:{str(e)}"
        raise HTTPException(status_code=400, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed:{str(e)}")

    formats = info.get("formats") or []
    heights = {f.get("height") for f in formats if isinstance(f.get("height"), int)}

    available = ["auto"]
    for q in [2160, 1440, 1080, 720, 480]:
        if any(h >= q for h in heights):
            available.append(str(q))

    title = info.get("title", "video")
    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "-", "_")).strip() or "video"

    ip = request.client.host if request.client else "unknown"
    log_download(ip, url, "info")
    return {"title": safe_title, "qualities": available}


@app.post("/fetch")
@limiter.limit("10/minute")
async def fetch_video(request: Request, data: FetchRequest):
    url = data.url.strip()
    quality = data.quality if data.quality in QUALITY_FORMATS else "auto"

    if not url:
        raise HTTPException(status_code=400, detail="URL cannot be empty.")
    if not any(d in url for d in SUPPORTED):
        raise HTTPException(status_code=400, detail="Unsupported platform.")

    clean_old_files()

    request_id = str(uuid.uuid4())
    file_id    = str(uuid.uuid4())
    output_template = str(DOWNLOAD_DIR / f"{file_id}.%(ext)s")

    is_twitter  = "twitter.com" in url or "x.com" in url
    is_facebook = "facebook.com" in url or "fb.watch" in url

    ydl_opts = {
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
        "format": QUALITY_FORMATS[quality],
        "socket_timeout": 60,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    if is_twitter:
        ydl_opts["extractor_args"] = {"twitter": {"api": ["syndication", "graphql"]}}
    if is_facebook:
        ydl_opts["format"] = "best"

    cookies_file = Path("cookies.txt")
    if cookies_file.exists():
        ydl_opts["cookiefile"] = str(cookies_file)

    progress_store[request_id] = {"status": "starting", "pct": 0}
    ip = request.client.host if request.client else "unknown"
    log_download(ip, url, "started", quality)
    asyncio.create_task(do_download(request_id, file_id, url, ydl_opts))

    return {"request_id": request_id}


@app.get("/progress/{request_id}")
async def get_progress(request_id: str, request: Request):
    async def generate():
        for _ in range(1200):  # max 10 dakika
            if await request.is_disconnected():
                break
            data = progress_store.get(request_id, {"status": "starting", "pct": 0})
            yield f"data: {json.dumps(data)}\n\n"
            if data.get("status") in ("done", "error"):
                progress_store.pop(request_id, None)
                break
            await asyncio.sleep(0.4)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/cancel/{request_id}")
async def cancel_download(request_id: str):
    cancel_store[request_id] = True
    return {"ok": True}


@app.post("/convert")
async def convert_video(data: ConvertRequest):
    file_id = data.file_id
    fmt     = data.format

    if ".." in file_id or "/" in file_id or "\\" in file_id:
        raise HTTPException(status_code=400, detail="Invalid request.")

    source = find_file(file_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source file not found. Fetch the video again.")

    loop   = asyncio.get_event_loop()
    out_id = str(uuid.uuid4())

    local_ffmpeg = Path(__file__).parent / "ffmpeg.exe"
    ffmpeg_bin   = str(local_ffmpeg) if local_ffmpeg.exists() else "ffmpeg"

    if fmt == "video":
        return {"file_id": file_id, "ext": "mp4", "media_type": "video/mp4"}

    def ffmpeg_mp3(src, dst):
        subprocess.run(
            [ffmpeg_bin, "-i", str(src), "-vn", "-ar", "44100", "-ac", "2",
             "-b:a", "192k", str(dst), "-y", "-loglevel", "quiet"],
            check=True,
        )

    def ffmpeg_silent(src, dst):
        subprocess.run(
            [ffmpeg_bin, "-i", str(src), "-c:v", "copy", "-an",
             str(dst), "-y", "-loglevel", "quiet"],
            check=True,
        )

    if fmt == "mp3":
        out_file = DOWNLOAD_DIR / f"{out_id}.mp3"
        try:
            await loop.run_in_executor(None, ffmpeg_mp3, source, out_file)
        except FileNotFoundError:
            raise HTTPException(status_code=500, detail="ffmpeg not found.")
        except subprocess.CalledProcessError:
            raise HTTPException(status_code=500, detail="MP3 conversion failed.")
        return {"file_id": out_id, "ext": "mp3", "media_type": "audio/mpeg"}

    if fmt == "silent":
        out_file = DOWNLOAD_DIR / f"{out_id}.mp4"
        try:
            await loop.run_in_executor(None, ffmpeg_silent, source, out_file)
        except FileNotFoundError:
            raise HTTPException(status_code=500, detail="ffmpeg not found.")
        except subprocess.CalledProcessError:
            raise HTTPException(status_code=500, detail="Silent video conversion failed.")
        return {"file_id": out_id, "ext": "mp4", "media_type": "video/mp4"}

    raise HTTPException(status_code=400, detail="Unknown format.")


@app.get("/file/{file_id}")
async def serve_file(file_id: str, filename: str = "video.mp4"):
    if ".." in file_id or "/" in file_id or "\\" in file_id:
        raise HTTPException(status_code=400, detail="Invalid request.")

    matched = find_file(file_id)
    if not matched:
        raise HTTPException(status_code=404, detail="File not found.")

    ext        = matched.suffix.lstrip(".")
    media_type = "audio/mpeg" if ext == "mp3" else "video/mp4"

    return FileResponse(path=matched, filename=filename, media_type=media_type)


# ── Admin panel ───────────────────────────────────────
def read_download_logs():
    entries = []
    if not DOWNLOAD_LOG.exists():
        return entries
    with open(DOWNLOAD_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
    return entries


@app.get("/admin/stats")
async def admin_stats(_: None = Depends(require_admin)):
    entries = read_download_logs()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    platform_counts = {}
    hourly = {str(h).zfill(2): 0 for h in range(24)}
    recent = []
    daily = {}

    for e in entries:
        if e.get("status") != "started":
            continue
        p = e.get("platform", "Unknown")
        platform_counts[p] = platform_counts.get(p, 0) + 1
        ts = e.get("ts", "")
        day = ts[:10]
        daily[day] = daily.get(day, 0) + 1
        if ts.startswith(today):
            hour = ts[11:13]
            hourly[hour] = hourly.get(hour, 0) + 1
        recent.append(e)

    recent = list(reversed(recent))[:50]

    # Nginx visitor count
    nginx_log = Path("/var/log/nginx/access.log")
    visitors_today = 0
    total_visitors = 0
    if nginx_log.exists():
        with open(nginx_log) as f:
            for line in f:
                if '"GET / ' in line or '"GET / H' in line:
                    if "bot" not in line.lower() and "ClaudeBot" not in line:
                        total_visitors += 1
                        if today.replace("-", "/") in line or \
                           datetime.utcnow().strftime("%d/%b/%Y") in line:
                            visitors_today += 1

    return JSONResponse({
        "platform_counts": platform_counts,
        "hourly": hourly,
        "daily": daily,
        "recent": recent,
        "visitors_today": visitors_today,
        "total_visitors": total_visitors,
        "total_downloads": sum(1 for e in entries if e.get("status") == "started"),
    })


@app.get("/admin/history")
async def admin_history(_: None = Depends(require_admin)):
    """Return last 200 download attempts from disk log (persists across restarts)."""
    entries = read_download_logs()
    started = [e for e in entries if e.get("status") == "started"]
    return JSONResponse(list(reversed(started))[:200])


@app.get("/admin/live")
async def admin_live(request: Request, _: None = Depends(require_admin)):
    """SSE stream of all HTTP traffic for the admin Traffic Inspector."""
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    live_subs.append(q)

    async def generate():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield 'data: {"ping":1}\n\n'
        finally:
            try:
                live_subs.remove(q)
            except ValueError:
                pass

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(_: None = Depends(require_admin)):
    html = Path("templates/admin.html").read_text(encoding="utf-8")
    return HTMLResponse(html)
