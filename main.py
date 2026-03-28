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
ADMIN_PASS = "W1ckedPro*Gabija"

def require_admin(credentials: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(credentials.username, ADMIN_USER)
    ok_pass = secrets.compare_digest(credentials.password, ADMIN_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=401, detail="Unauthorized",
                            headers={"WWW-Authenticate": "Basic"})

limiter = Limiter(key_func=get_remote_address)
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
templates = Jinja2Templates(directory="templates")

STATIC_DIR = Path("static")
STATIC_DIR.mkdir(exist_ok=True)


@app.get("/favicon.svg", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.svg", media_type="image/svg+xml")

@app.get("/og-image.png", include_in_schema=False)
async def og_image():
    return FileResponse("static/og-image.svg", media_type="image/svg+xml")

@app.get("/robots.txt", include_in_schema=False)
async def robots():
    return FileResponse("static/robots.txt", media_type="text/plain")


@app.get("/ads.txt", include_in_schema=False)
async def ads_txt():
    return FileResponse("static/ads.txt", media_type="text/plain")


@app.get("/sitemap.xml", include_in_schema=False)
async def sitemap():
    return FileResponse("static/sitemap.xml", media_type="application/xml")


@app.get("/privacy", include_in_schema=False)
async def privacy(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})


@app.get("/terms", include_in_schema=False)
async def terms(request: Request):
    return templates.TemplateResponse("terms.html", {"request": request})

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

MAX_AGE_SECONDS = 1800

# request_id → (progress dict, timestamp)
progress_store: dict = {}
# request_id → cancel flag
cancel_store: dict = {}
# IP → {flag, country}
geo_cache: dict = {}
# live admin SSE subscribers
live_subs: list = []

# ── Periodic cleanup ─────────────────────────────────
async def _cleanup_loop():
    """Clean stale progress entries and limit geo_cache size every 5 min."""
    while True:
        await asyncio.sleep(300)
        now = time.time()
        # Remove progress entries older than 15 min
        stale = [k for k, v in progress_store.items()
                 if isinstance(v, dict) and now - v.get("_ts", now) > 900]
        for k in stale:
            progress_store.pop(k, None)
            cancel_store.pop(k, None)
        # Cap geo_cache at 500 entries
        if len(geo_cache) > 500:
            geo_cache.clear()
        # Clean old download files
        clean_old_files()

@app.on_event("startup")
async def _start_cleanup():
    asyncio.create_task(_cleanup_loop())


@app.middleware("http")
async def traffic_broadcast_middleware(request: Request, call_next):
    """Broadcast every HTTP request to live admin SSE subscribers."""
    start = time.time()
    response = await call_next(request)
    ms = int((time.time() - start) * 1000)

    # Skip SSE/admin-live to avoid infinite loop, and static assets
    path = request.url.path
    skip = ("/admin", "/progress/", "/favicon", "/robots.txt", "/sitemap.xml", "/ads.txt")
    if any(path.startswith(s) for s in skip):
        return response

    # Skip static file extensions
    if path.rsplit(".", 1)[-1] in ("css", "js", "png", "jpg", "svg", "ico", "woff2", "woff", "ttf"):
        return response

    ip = get_real_ip(request)
    geo = await get_geo(ip)
    method = request.method

    await broadcast_live({
        "ts": datetime.utcnow().strftime("%H:%M:%S"),
        "ip": ip, "flag": geo["flag"], "country": geo["country"],
        "method": method, "path": path, "status": response.status_code,
        "ms": ms,
    })

    return response

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


def get_real_ip(request: Request) -> str:
    """Get real client IP, accounting for nginx reverse proxy headers."""
    return (
        request.headers.get("x-real-ip") or
        request.headers.get("x-forwarded-for", "").split(",")[0].strip() or
        (request.client.host if request.client else "unknown")
    )


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


async def broadcast_live(event: dict):
    """Send event to all live admin SSE subscribers."""
    for q in live_subs[:]:
        try:
            q.put_nowait(event)
        except Exception:
            pass


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
        "remote_components": ["ejs:github"],
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

    ip = get_real_ip(request)
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
        "remote_components": ["ejs:github"],
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

    progress_store[request_id] = {"status": "starting", "pct": 0, "_ts": time.time()}
    ip = get_real_ip(request)
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
async def serve_file(file_id: str, request: Request, filename: str = "video.mp4"):
    if ".." in file_id or "/" in file_id or "\\" in file_id:
        raise HTTPException(status_code=400, detail="Invalid request.")

    matched = find_file(file_id)
    if not matched:
        raise HTTPException(status_code=404, detail="File not found.")

    ext        = matched.suffix.lstrip(".")
    media_type = "audio/mpeg" if ext == "mp3" else "video/mp4"
    file_size  = matched.stat().st_size

    # Support Range requests for video seeking
    range_header = request.headers.get("range")
    if range_header:
        # Parse "bytes=start-end"
        range_spec = range_header.replace("bytes=", "").strip()
        parts = range_spec.split("-")
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if parts[1] else file_size - 1
        end = min(end, file_size - 1)
        length = end - start + 1

        def iter_file():
            with open(matched, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(8192, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

        return StreamingResponse(
            iter_file(),
            status_code=206,
            media_type=media_type,
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(length),
                "Content-Disposition": f'inline; filename="{filename}"',
            },
        )

    return FileResponse(path=matched, filename=filename, media_type=media_type,
                        headers={"Accept-Ranges": "bytes"})


# ── Admin panel ───────────────────────────────────────
def read_download_logs(max_lines: int = 2000):
    entries = []
    if not DOWNLOAD_LOG.exists():
        return entries
    try:
        with open(DOWNLOAD_LOG) as f:
            # Only read last max_lines to avoid OOM on large logs
            lines = f.readlines()[-max_lines:]
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
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

    # Nginx visitor count — run in executor to avoid blocking event loop
    nginx_log = Path("/var/log/nginx/access.log")

    def _count_visitors():
        v_today = 0
        v_total = 0
        if not nginx_log.exists():
            return v_today, v_total
        try:
            # Only read last 50k lines max to avoid OOM
            with open(nginx_log) as f:
                lines = f.readlines()[-50000:]
            for line in lines:
                if '"GET / ' in line or '"GET / H' in line:
                    if "bot" not in line.lower() and "ClaudeBot" not in line:
                        v_total += 1
                        if today.replace("-", "/") in line or \
                           datetime.utcnow().strftime("%d/%b/%Y") in line:
                            v_today += 1
        except Exception:
            pass
        return v_today, v_total

    loop = asyncio.get_event_loop()
    visitors_today, total_visitors = await loop.run_in_executor(None, _count_visitors)

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


@app.get("/admin/visitors")
async def admin_visitors(_: None = Depends(require_admin)):
    """Parse nginx access.log and return recent unique visitors with details."""
    import re
    nginx_log = Path("/var/log/nginx/access.log")

    def _parse():
        if not nginx_log.exists():
            return []
        # nginx combined log format regex
        pattern = re.compile(
            r'(?P<ip>[\d.]+) - - \[(?P<date>[^\]]+)\] '
            r'"(?P<method>\w+) (?P<path>[^ ]+) [^"]*" '
            r'(?P<status>\d+) \d+ '
            r'"(?P<referrer>[^"]*)" '
            r'"(?P<ua>[^"]*)"'
        )
        visitors = {}  # ip -> latest visit info
        try:
            with open(nginx_log) as f:
                lines = f.readlines()[-20000:]
            for line in lines:
                m = pattern.match(line)
                if not m:
                    continue
                d = m.groupdict()
                # Only page visits (GET /), skip assets/api
                if d["method"] != "GET":
                    continue
                path = d["path"]
                if path.startswith(("/file/", "/progress/", "/admin/", "/favicon", "/robots", "/sitemap", "/ads.txt")):
                    continue
                ip = d["ip"]
                ua = d["ua"]
                if "bot" in ua.lower() or "spider" in ua.lower() or "crawl" in ua.lower():
                    continue
                ref = d["referrer"] if d["referrer"] != "-" else ""
                # Parse date: 27/Mar/2026:14:30:00 +0000
                raw_date = d["date"].split()[0]  # remove timezone
                # Detect device type from user-agent
                ua_lower = ua.lower()
                if "mobile" in ua_lower or "android" in ua_lower or "iphone" in ua_lower:
                    device = "Mobile"
                elif "tablet" in ua_lower or "ipad" in ua_lower:
                    device = "Tablet"
                else:
                    device = "Desktop"
                # Detect browser
                if "edg" in ua_lower:
                    browser = "Edge"
                elif "chrome" in ua_lower and "safari" in ua_lower:
                    browser = "Chrome"
                elif "firefox" in ua_lower:
                    browser = "Firefox"
                elif "safari" in ua_lower:
                    browser = "Safari"
                else:
                    browser = "Other"
                # Update visitor record (keep latest visit)
                # Convert nginx date "27/Mar/2026:14:30:00" to "2026-03-27 14:30:00"
                try:
                    from datetime import datetime as _dt
                    dt = _dt.strptime(raw_date, "%d/%b/%Y:%H:%M:%S")
                    iso_date = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    iso_date = raw_date

                if ip not in visitors:
                    visitors[ip] = {
                        "ip": ip,
                        "first_seen": iso_date,
                        "last_seen": iso_date,
                        "visits": 0,
                        "pages_visited": set(),
                        "referrer": ref,
                        "device": device,
                        "browser": browser,
                        "ua": ua[:150],
                    }
                v = visitors[ip]
                v["last_seen"] = iso_date
                v["visits"] += 1
                v["pages_visited"].add(path)
                if ref and not v["referrer"]:
                    v["referrer"] = ref
        except Exception:
            pass
        # Convert sets to lists, sort by last_seen desc
        result = []
        for v in visitors.values():
            v["pages_visited"] = list(v["pages_visited"])
            result.append(v)
        result.sort(key=lambda x: x["last_seen"], reverse=True)
        return result[:500]

    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _parse)

    # Enrich with geo data
    for v in data[:100]:  # limit geo lookups
        geo = await get_geo(v["ip"])
        v["flag"] = geo["flag"]
        v["country"] = geo["country"]

    return JSONResponse(data)


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
