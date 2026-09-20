#!/usr/bin/env python3
"""
MovieBox → Archive.org distributed worker.
Paste this + set env vars on any platform (GitHub Actions, Replit, Cloud Shell, etc).

DB: private GitHub repo. ONE FILE PER TITLE = the lock.
    - titles/{subject_id}.json  — created atomically as claim, updated by owner only
    - queue.json                — discovery output (read-heavy, rarely written)
    No separate lock/claim files. Creating the title file IS the claim.
    Two workers racing to create the same file → only one gets 201, other gets 409/422.

Required env vars:
  GITHUB_TOKEN     - GitHub PAT with repo scope
  DB_REPO          - e.g. "youruser/moviebox-db"
  IA_ACCESS        - Archive.org S3 access key
  IA_SECRET        - Archive.org S3 secret key

Optional:
  IA_ACCOUNT       - account label (default: "default")
  WORKER_ID        - unique name (auto-generated if missing)
  MODE             - "discover" | "download" | "both" (default: both)
  MAX_TITLES       - titles to process this run (default: 5)
  QUALITY          - 1080 / 720 / 480 (default: 1080)
"""

import base64, hashlib, hmac, json, os, random, sys, time, urllib.parse, uuid
import subprocess, tempfile, shutil
from pathlib import Path

def _env(key, prompt=None, required=True):
    val = os.environ.get(key, "")
    if val:
        return val
    if not required:
        return ""
    if sys.stdin.isatty() and prompt:
        val = input(f"  {prompt}: ").strip()
        if val:
            return val
    print(f"ERROR: {key} not set. Set it as an environment variable or Replit Secret.")
    sys.exit(1)

print("=" * 50)
print("  MovieBox -> Archive.org Worker")
print("=" * 50)

GITHUB_TOKEN = _env("GITHUB_TOKEN", "GitHub token (ghp_xxx)")
DB_REPO = _env("DB_REPO", "DB repo (user/moviebox-db)")
IA_ACCESS = _env("IA_ACCESS", "Archive.org S3 access key")
IA_SECRET = _env("IA_SECRET", "Archive.org S3 secret key")
IA_ACCOUNT = os.environ.get("IA_ACCOUNT", "default")
WORKER_ID = os.environ.get("WORKER_ID", f"w-{uuid.uuid4().hex[:8]}")
MODE = os.environ.get("MODE", "both")
MAX_TITLES = int(os.environ.get("MAX_TITLES", "5"))
QUALITY = int(os.environ.get("QUALITY", "1080"))
WORK_DIR = Path(tempfile.gettempdir()) / "moviebox-work"
WORK_DIR.mkdir(exist_ok=True)
CLAIM_TTL = 7200

try:
    import requests as req_lib
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "-q"])
    import requests as req_lib

try:
    import internetarchive
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "internetarchive", "-q"])
    import internetarchive

# ============================= GITHUB REPO DB =============================
GH = "https://api.github.com"
GH_H = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

def gh_read(path):
    r = req_lib.get(f"{GH}/repos/{DB_REPO}/contents/{path}", headers=GH_H, timeout=30)
    if r.status_code == 404:
        return None, None
    r.raise_for_status()
    d = r.json()
    return json.loads(base64.b64decode(d["content"]).decode("utf-8")), d["sha"]

def gh_write(path, data, sha=None, msg=None):
    encoded = base64.b64encode(json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).decode()
    payload = {"message": msg or f"{WORKER_ID} {path}", "content": encoded}
    if sha:
        payload["sha"] = sha
    r = req_lib.put(f"{GH}/repos/{DB_REPO}/contents/{path}", headers=GH_H, json=payload, timeout=30)
    if r.status_code in (409, 422):
        return None
    if r.status_code in (200, 201):
        return r.json()["content"]["sha"]
    r.raise_for_status()

def gh_list_dir(path):
    r = req_lib.get(f"{GH}/repos/{DB_REPO}/contents/{path}", headers=GH_H, timeout=30)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    items = r.json()
    return [{"name": i["name"], "path": i["path"], "sha": i["sha"]} for i in items] if isinstance(items, list) else []

# ========================= CLAIM = CREATE TITLE FILE =========================
# Creating titles/{sid}.json with sha=None → 201 if new (you own it), 409/422 if
# another worker already created it. No race window. The file IS the lock.

def try_claim(subject_id):
    """Atomically claim by creating the title file. Returns sha if we got it, None if taken."""
    existing, sha = gh_read(f"titles/{subject_id}.json")
    if existing:
        # Already exists — check if it's a dead claim we can steal
        if existing.get("status") == "claimed" and existing.get("worker") != WORKER_ID:
            claimed_at = existing.get("claimed_at", "")
            try:
                age = time.time() - time.mktime(time.strptime(claimed_at, "%Y-%m-%dT%H:%M:%SZ"))
            except (ValueError, TypeError, OverflowError):
                age = CLAIM_TTL + 1
            if age < CLAIM_TTL:
                return None  # Still alive
            # Expired — steal it by overwriting with our worker (sha-locked so no double-steal)
            existing["worker"] = WORKER_ID
            existing["claimed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            return gh_write(f"titles/{subject_id}.json", existing, sha=sha,
                            msg=f"{WORKER_ID} steals expired {subject_id}")
        return None  # Already processing/done by someone

    # File doesn't exist — create it atomically
    claim_data = {
        "subject_id": subject_id, "status": "claimed", "worker": WORKER_ID,
        "claimed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    return gh_write(f"titles/{subject_id}.json", claim_data, sha=None,
                    msg=f"{WORKER_ID} claims {subject_id}")

def save_title(subject_id, data, sha):
    return gh_write(f"titles/{subject_id}.json", data, sha=sha,
                    msg=f"{WORKER_ID} updates {subject_id}")

def update_episode(subject_id, ep_key, status, extra=None):
    """Retry on sha conflicts — multiple parallel workers writing to same title file."""
    for attempt in range(10):
        data, sha = gh_read(f"titles/{subject_id}.json")
        if not data:
            return
        if ep_key not in data.get("episodes", {}):
            return
        data["episodes"][ep_key]["status"] = status
        if extra:
            data["episodes"][ep_key].update(extra)
        data["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        result = save_title(subject_id, data, sha)
        if result is not None:
            return
        # 409 conflict — another thread wrote first, re-read and retry
        time.sleep(random.uniform(0.5, 2))

def heartbeat(subject_id):
    data, sha = gh_read(f"titles/{subject_id}.json")
    if data and data.get("worker") == WORKER_ID:
        data["claimed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        save_title(subject_id, data, sha)

# ================================= QUEUE =================================
def get_queue():
    data, sha = gh_read("queue.json")
    if data is None:
        return [], None
    return data.get("pending", []), sha

def get_blacklist():
    """Read blacklist.json — subject_ids that have no downloadable source, never re-claim."""
    data, _ = gh_read("blacklist.json")
    if data is None:
        return {}
    return data.get("ids", {})

def add_to_blacklist(subject_id, title=None, ttype=None, reason=""):
    """Add a subject to blacklist so it's never re-claimed. Retries on conflict."""
    for _ in range(5):
        data, sha = gh_read("blacklist.json")
        if data is None:
            data = {"ids": {}}
            sha = None
        data["ids"][str(subject_id)] = {
            "title": title or "", "type": ttype or "",
            "reason": reason, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        result = gh_write("blacklist.json", data, sha=sha, msg=f"{WORKER_ID} blacklists {subject_id}")
        if result is not None:
            return
        time.sleep(random.uniform(0.5, 2))

def add_to_queue(subject_ids):
    data, sha = gh_read("queue.json")
    if data is None:
        data = {"pending": []}
        sha = None
    existing_q = set(data["pending"])
    known = {f["name"].replace(".json", "") for f in gh_list_dir("titles")}
    blacklist = set(get_blacklist().keys())
    new = [s for s in subject_ids if s not in existing_q and s not in known and s not in blacklist]
    if not new:
        return 0
    data["pending"].extend(new)
    data["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for _ in range(5):
        result = gh_write("queue.json", data, sha=sha)
        if result is not None:
            return len(new)
        time.sleep(random.uniform(1, 3))
        data, sha = gh_read("queue.json")
        if data is None:
            data = {"pending": []}
            sha = None
        existing_q = set(data["pending"])
        new = [s for s in subject_ids if s not in existing_q and s not in known and s not in blacklist]
        data["pending"].extend(new)
    return 0

def remove_from_queue(subject_id):
    q, sha = gh_read("queue.json")
    if q and subject_id in q.get("pending", []):
        q["pending"] = [s for s in q["pending"] if s != subject_id]
        gh_write("queue.json", q, sha=sha)

def pick_from_queue():
    queue, _ = get_queue()
    blacklist = set(get_blacklist().keys())
    queue = [s for s in queue if s not in blacklist]
    random.shuffle(queue)
    for sid in queue:
        claim_sha = try_claim(sid)
        if claim_sha:
            log(f"Claimed {sid}")
            remove_from_queue(sid)
            return sid, claim_sha
    return None, None

# ============================== MOVIEBOX API ==============================
MB_KEY = base64.urlsafe_b64decode("76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O==")
MB_BASES = ["https://api6.aoneroom.com", "https://api5.aoneroom.com", "https://api4.aoneroom.com"]
MB_JWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjIwOTI2Mjc0MjUzNjMzODUwNDgsImV4cCI6MTc5NzUwOTcxOSwiaWF0IjoxNzg5NzMzNDE5fQ.lk5GbDkDTlP3kuw1LgLmuRhsrF1ugN8BvQrZFb_HqQo"

def _dev():
    brands = [("Samsung","SM-G991B"),("Samsung","SM-A536B"),("Xiaomi","M2102J20SG"),
              ("Google","Pixel 7"),("OnePlus","NE2211"),("OPPO","CPH2399")]
    b, m = random.choice(brands)
    return {"package_name":"com.community.oneroom","version_name":"3.0.03.0528.03",
            "version_code":50020041,"os":"android","os_version":random.choice(["11","12","13","14"]),
            "install_ch":"ps","device_id":hashlib.md5(uuid.uuid4().bytes).hexdigest(),
            "install_store":"ps","gaid":str(uuid.uuid4()),"brand":b,"model":m,
            "system_language":"en","net":"NETWORK_WIFI",
            "region":random.choice(["US","DE","EG","SA","DZ"]),
            "timezone":random.choice(["Africa/Lagos","Europe/Berlin","Asia/Riyadh"]),
            "sp_code":"26202","X-Play-Mode":"1"}
DEV = _dev()
_sess = req_lib.Session(); _sess.headers.clear()

def _sq(q):
    if not q: return ""
    p = {}
    for pair in q.split("&"):
        if "=" not in pair: continue
        i = pair.index("=")
        p[urllib.parse.unquote(pair[:i])] = urllib.parse.unquote(pair[i+1:])
    return "&".join(f"{k}={v}" for k, v in sorted(p.items()))

def _sign(method, url, body=None):
    ts = int(time.time() * 1000)
    pr = urllib.parse.urlsplit(url)
    up = pr.path + ("?" + _sq(pr.query) if pr.query else "")
    ct = "application/json; charset=UTF-8" if body else ""
    cl = str(len(body)) if body else ""
    bm = hashlib.md5((body[:102400] if body and len(body)>102400 else body or "").encode()).hexdigest() if body else ""
    ss = f"{method.upper()}\n\n{ct}\n{cl}\n{ts}\n{bm}\n{up}"
    sig = base64.b64encode(hmac.new(MB_KEY, ss.encode(), hashlib.md5).digest()).decode()
    return ts, sig

def _hdr(method, url, body=None):
    ts, sig = _sign(method, url, body)
    h = {"X-Play-Mode":"1","X-Client-Info":json.dumps(DEV,separators=(",",":")),"X-Client-Status":"1",
         "x-tr-signature":f"{ts}|2|{sig}",
         "Authorization":f"Bearer {MB_JWT}",
         "User-Agent":"okhttp/4.12.0"}
    if body: h["Content-Type"] = "application/json; charset=UTF-8"
    return h

def mb_get(path, params=None):
    url = MB_BASES[0] + path + ("?" + urllib.parse.urlencode(params) if params else "")
    for b in MB_BASES:
        try:
            u = url.replace(MB_BASES[0], b)
            r = _sess.get(u, headers=_hdr("GET", u), timeout=20)
            if r.status_code == 200: return r.json()
        except: continue
    return None

def mb_post(path, data):
    url = MB_BASES[0] + path
    body = json.dumps(data, separators=(",",":"))
    for b in MB_BASES:
        try:
            u = url.replace(MB_BASES[0], b)
            r = _sess.post(u, data=body.encode(), headers=_hdr("POST", u, body), timeout=20)
            if r.status_code == 200: return r.json()
        except: continue
    return None

def mb_catalog(page=1): return mb_post("/wefeed-mobile-bff/subject-api/list", {"page":page,"pageSize":20})
def mb_detail(sid): return mb_get("/wefeed-mobile-bff/subject-api/get", {"subjectId":str(sid)})
def mb_seasons(sid): return mb_get("/wefeed-mobile-bff/subject-api/season-info", {"subjectId":str(sid)})
def mb_dub(sid): return mb_get("/wefeed-mobile-bff/subject-api/dub-info", {"subjectId":str(sid),"episodeId":"0"})
def mb_resource_all(sid, se, res=1080, max_ep=200):
    """Fetch ALL episodes of one season by paging in chunks of 20. Returns dict {ep_num: resource}.
    Filters client-side by actual `se` field — the API sometimes returns other seasons."""
    out = {}
    ep_from = 1
    CHUNK = 20
    seen_any_correct = False
    consecutive_empty = 0
    while ep_from <= max_ep:
        ep_to = ep_from + CHUNK - 1
        r = mb_get("/wefeed-mobile-bff/subject-api/resource",
                   {"subjectId":str(sid),"resolution":str(res),"se":str(se),
                    "epFrom":str(ep_from),"epTo":str(ep_to),"page":"1","perPage":"20",
                    "all":"1","startPosition":str(ep_from),"endPosition":str(ep_to),"pagerMode":"0"})
        if not r or "data" not in r:
            break
        items = r["data"].get("list", [])
        if not items:
            break
        matching = [it for it in items if int(it.get("se", 0)) == int(se)]
        for item in matching:
            out[int(item.get("ep", 0))] = item
        if matching:
            seen_any_correct = True
            consecutive_empty = 0
        else:
            consecutive_empty += 1
            if consecutive_empty >= 2 and seen_any_correct:
                break
        if len(items) < CHUNK:
            break
        ep_from += CHUNK
    return out

def mb_resource(sid, se, ep, res=1080):
    """Fetch a single episode. Uses all=1 because all=0 always returns E01."""
    eps = mb_resource_all(sid, se, res, max_ep=99)
    if ep in eps:
        return {"data": {"list": [eps[ep]]}}
    return None

def get_arabic_id(sid):
    data = mb_dub(sid)
    if not data or "data" not in data: return str(sid)
    for d in data["data"].get("dubs", []):
        if d.get("lanCode") == "ar" and d.get("type") == 1: return str(d["subjectId"])
    return str(sid)

# ============================== ARCHIVE.ORG ==============================
def mk_aid():
    return str(random.randint(10000000, 99999999))

def _ia():
    return internetarchive.get_session(config={"s3":{"access":IA_ACCESS,"secret":IA_SECRET}})

def ia_exists(ident):
    return _ia().get_item(ident).exists

def ia_upload(ident, local, remote, md=None, is_last=False, size_hint=None):
    """Upload to Archive.org. Optimized headers per IAS3 docs:
    - verify=False: skip client-side SHA1 recompute (minutes saved per big file)
    - queue_derive on last file only: derive triggered once, not per-file
    - x-archive-interactive-priority: express queue lane
    - x-archive-size-hint: pre-allocate storage
    - retries=10: IA 503 SlowDown is common under load"""
    s = _ia()
    m = md or {}; m.setdefault("mediatype","movies"); m.setdefault("collection","opensource_movies")
    headers = {"x-archive-interactive-priority": "1"}
    if size_hint:
        headers["x-archive-size-hint"] = str(size_hint)
    for a in range(6):
        try:
            s.get_item(ident).upload({remote:str(local)}, metadata=m, headers=headers,
                                     verify=False, queue_derive=is_last,
                                     retries=10, retries_sleep=5)
            return True
        except Exception as e:
            if "SlowDown" in str(e) or "503" in str(e): time.sleep(30*(a+1))
            elif a >= 5: raise
            else: time.sleep(5)
    return False

# ================================ DOWNLOAD ================================
import threading, concurrent.futures

def _dl_chunk(url, start, end, dest_fp, lock, hdr):
    """Download bytes [start, end] using Range request, write to dest_fp at start."""
    h = dict(hdr); h["Range"] = f"bytes={start}-{end}"
    r = req_lib.get(url, headers=h, stream=True, timeout=120)
    if r.status_code not in (200, 206):
        return False
    buf = b""
    for chunk in r.iter_content(524288):
        buf += chunk
    with lock:
        dest_fp.seek(start)
        dest_fp.write(buf)
    return True

def dl_file(url, dest_path, expected_size=None, connections=8):
    """Multi-connection parallel download using HTTP Range. Falls back to single-stream."""
    dest = Path(dest_path); part = dest.with_suffix(dest.suffix + ".part")
    hdr = {"User-Agent": "okhttp/4.12.0"}

    # First probe: HEAD-ish request to check Range support and size
    size = int(expected_size) if expected_size and str(expected_size).isdigit() else 0
    if size < 10_000_000 or connections < 2:
        # Small file or single-connection mode — simple stream
        r = req_lib.get(url, headers=hdr, stream=True, timeout=60)
        if r.status_code != 200:
            log(f"DL HTTP {r.status_code}"); return False
        with open(part, "wb") as f:
            for chunk in r.iter_content(524288): f.write(chunk)
        part.rename(dest); return True

    # Verify Range support with a small probe
    probe = req_lib.get(url, headers={**hdr, "Range": "bytes=0-1023"}, stream=True, timeout=60)
    if probe.status_code != 206:
        probe.close()
        # No Range support — fallback single stream
        r = req_lib.get(url, headers=hdr, stream=True, timeout=60)
        if r.status_code != 200:
            log(f"DL HTTP {r.status_code}"); return False
        with open(part, "wb") as f:
            for chunk in r.iter_content(524288): f.write(chunk)
        part.rename(dest); return True
    probe.close()

    # Parallel Range download
    chunk_size = size // connections
    ranges = []
    for i in range(connections):
        start = i * chunk_size
        end = size - 1 if i == connections - 1 else (start + chunk_size - 1)
        ranges.append((start, end))

    # Preallocate file
    with open(part, "wb") as f:
        f.truncate(size)

    lock = threading.Lock()
    ok = True
    with open(part, "r+b") as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=connections) as ex:
            futs = [ex.submit(_dl_chunk, url, s, e, f, lock, hdr) for s, e in ranges]
            for fu in concurrent.futures.as_completed(futs):
                if not fu.result():
                    ok = False
    if not ok:
        part.unlink(missing_ok=True); return False
    part.rename(dest); return True

# =============================== DISCOVERY ================================
def discover(max_pages=50):
    """Only add movies (subjectType=1) and series (subjectType=2).
    Skip subjectType=6 (user-uploaded music videos) — those pollute the queue."""
    log("Discovering...")
    found = []
    skipped_junk = 0
    for pg in range(1, max_pages + 1):
        data = mb_catalog(pg)
        if not data or "data" not in data: break
        items = data["data"].get("items", [])
        if not items: break
        for it in items:
            sid = str(it.get("subjectId", ""))
            st = it.get("subjectType", 0)
            if not sid: continue
            if st not in (1, 2):
                skipped_junk += 1
                continue
            found.append(sid)
        log(f"  Page {pg}: {len(items)} items ({skipped_junk} junk skipped so far)")
        if not data["data"].get("pager",{}).get("hasMore"): break
        time.sleep(0.5)
    if found:
        added = add_to_queue(found)
        log(f"Discovery: {len(found)} real titles seen, {skipped_junk} junk skipped, {added} new")
    else:
        log(f"Discovery: nothing found ({skipped_junk} junk skipped)")

# ============================ PROCESS TITLE ===============================
def process_title(subject_id, claim_sha):
    log(f"Processing {subject_id}")

    # Full detail from API
    detail = mb_detail(subject_id)
    dd = detail.get("data", {}) if detail else {}
    tname = dd.get("title", "Unknown")
    st = dd.get("subjectType", 0)
    # Reject non-movie/series (subjectType 6 = user-uploaded music, etc.)
    if st not in (1, 2):
        log(f"  {tname}: not a movie/series (type={st}) — blacklisting")
        add_to_blacklist(subject_id, tname, f"type_{st}", "not_movie_or_series")
        _, sha0 = gh_read(f"titles/{subject_id}.json")
        if sha0:
            req_lib.delete(f"{GH}/repos/{DB_REPO}/contents/titles/{subject_id}.json",
                           headers=GH_H,
                           json={"message": f"{WORKER_ID} rejects type_{st} {subject_id}", "sha": sha0},
                           timeout=30)
        return
    ttype = "movie" if st == 1 else "series"
    description = dd.get("description", "")
    genre = dd.get("genre", "")
    country = dd.get("countryName", "")
    language = dd.get("language", "")
    imdb = dd.get("imdbRatingValue", "")
    release_date = dd.get("releaseDate", "")
    cover_url = dd.get("cover", {}).get("url", "")
    cover_fmt = dd.get("cover", {}).get("format", "jpg")
    staff = [{"name": s.get("name",""), "role": s.get("character","")}
             for s in dd.get("staffList", [])[:20]]

    arabic_id = get_arabic_id(subject_id)
    log(f"  {tname} ({ttype}) arabic={arabic_id}")

    # Get Arabic title label from resource endpoint
    arabic_title = ""
    res_check = mb_resource(arabic_id, 1, 1, QUALITY)
    if res_check and "data" in res_check:
        arabic_title = res_check["data"].get("subjectTitle", "")
        total_size = res_check["data"].get("totalSize", "0")
        total_eps_api = res_check["data"].get("totalEpisode", 0)
    else:
        total_size = "0"
        total_eps_api = 0

    # Seasons and episodes — authoritative source is mb_resource_all per season,
    # not season-info (which lies about episode counts for many shows).
    eps = []
    season_info = []
    season_resources_cache = {}
    if ttype == "series":
        sd = mb_seasons(arabic_id)
        if sd and "data" in sd:
            sl = sd["data"].get("seasons", [sd["data"]] if isinstance(sd["data"], dict) else [])
            for s in sl:
                se = s.get("se", 1)
                available = mb_resource_all(arabic_id, se, QUALITY)
                if not available:
                    log(f"  S{se}: no {QUALITY}p episodes available")
                    continue
                season_resources_cache[se] = available
                ep_nums = sorted(available.keys())
                for e in ep_nums:
                    eps.append((se, e))
                season_info.append({"season": se, "episodes": ep_nums})
                log(f"  S{se}: {len(ep_nums)} episodes available")
    else:
        # Movie: probe directly first — cheap way to detect "no source" before wasting IA calls
        probe = mb_resource_all(arabic_id, 1, QUALITY)
        if probe and 1 in probe:
            season_resources_cache[1] = probe
            eps = [(1, 1)]
            season_info = [{"season": 1, "episodes": [1]}]

    # No sources anywhere — blacklist and bail without touching IA
    if not eps or not season_resources_cache:
        log(f"  {tname}: no downloadable sources — blacklisting")
        add_to_blacklist(subject_id, tname, ttype, "no_source_at_all")
        # Delete title file (drops the claim)
        _, sha0 = gh_read(f"titles/{subject_id}.json")
        if sha0:
            req_lib.delete(f"{GH}/repos/{DB_REPO}/contents/titles/{subject_id}.json",
                           headers=GH_H,
                           json={"message": f"{WORKER_ID} blacklists {subject_id}", "sha": sha0},
                           timeout=30)
        return

    # Reserve archive ID
    aid = mk_aid()
    while ia_exists(aid): aid = mk_aid()
    log(f"  Archive: {aid}")

    ep_dict = {}
    for se, ep in eps:
        ep_dict[f"S{se:02d}E{ep:02d}"] = {"season": se, "episode": ep, "status": "pending"}

    title_data = {
        "subject_id": subject_id, "arabic_id": arabic_id, "archive_id": aid,
        "title": tname, "title_arabic": arabic_title, "type": ttype,
        "description": description, "genre": genre, "country": country,
        "language": language, "imdb_rating": imdb, "release_date": release_date,
        "cover_url": cover_url, "staff": staff,
        "account": IA_ACCOUNT,
        "status": "processing", "worker": WORKER_ID, "episodes": ep_dict,
        "claimed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    title_sha = save_title(subject_id, title_data, claim_sha)

    tdir = WORK_DIR / aid; tdir.mkdir(exist_ok=True)
    edir = tdir / "epmm"; edir.mkdir(exist_ok=True)

    # Download cover image
    cover_remote = None
    if cover_url:
        cover_ext = cover_fmt if cover_fmt in ("jpg", "png", "webp") else "jpg"
        cover_local = tdir / f"{aid}_cover.{cover_ext}"
        log(f"  Downloading cover...")
        try:
            cr = req_lib.get(cover_url, timeout=30)
            if cr.status_code == 200:
                cover_local.write_bytes(cr.content)
                cover_remote = f"{aid}_cover.{cover_ext}"
        except Exception:
            pass

    # Build rich in.txt metadata
    meta = {
        "subject_id": subject_id,
        "arabic_id": arabic_id,
        "archive_id": aid,
        "title": tname,
        "title_arabic": arabic_title,
        "description": description,
        "type": ttype,
        "genre": genre,
        "country": country,
        "language": language,
        "imdb_rating": imdb,
        "release_date": release_date,
        "cover_url": cover_url,
        "staff": staff,
        "seasons": season_info,
        "total_episodes": total_eps_api or len(eps),
        "total_size_bytes": int(total_size) if total_size.isdigit() else 0,
        "resolution": QUALITY,
        "source": "moviebox",
        "archived_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    itxt = tdir / "in.txt"
    itxt.write_text(base64.b64encode(json.dumps(meta, ensure_ascii=False).encode()).decode())

    # Upload in.txt + cover
    ia_upload(aid, itxt, "in.txt", {"mediatype": "movies", "collection": "opensource_movies",
                                     "title": aid, "description": "media archive"})
    if cover_remote:
        ia_upload(aid, tdir / cover_remote, cover_remote)
        log(f"  Cover uploaded")

    # Parallel pipeline: 4 workers each doing dl+upload independently.
    # Each thread claims one episode from the queue, downloads it, uploads it, deletes local file, next.
    # This keeps both network legs busy — while one thread waits on IA upload another is downloading.
    ep_items = list(ep_dict.items())
    total_bytes_hint = sum(int(season_resources_cache.get(ei["season"], {}).get(ei["episode"], {}).get("size", 0) or 0)
                            for _, ei in ep_items)
    PARALLEL_EPISODES = 4

    ep_queue = list(enumerate(ep_items))  # (idx, (ek, ei))
    qlock = threading.Lock()
    hb_lock = threading.Lock()
    last_hb = [time.time()]

    def _next_ep():
        with qlock:
            return ep_queue.pop(0) if ep_queue else None

    def _pipeline_worker(worker_slot):
        while True:
            item = _next_ep()
            if item is None: return
            idx, (ek, ei) = item
            is_last_ep = (idx == len(ep_items) - 1)
            se, ep = ei["season"], ei["episode"]

            # Heartbeat at most every 60s from any thread
            with hb_lock:
                if time.time() - last_hb[0] > 60:
                    try: heartbeat(subject_id)
                    except Exception: pass
                    last_hb[0] = time.time()

            r0 = season_resources_cache.get(se, {}).get(ep)
            if not r0:
                log(f"    [{worker_slot}] {ek}: no source"); update_episode(subject_id, ek, "no_source"); continue

            dl_url = r0.get("resourceLink",""); fsize = r0.get("size","0"); rez = r0.get("resolution",0)
            if not dl_url:
                update_episode(subject_id, ek, "no_link"); continue

            fn = f"{aid}_movie.mp4" if ttype == "movie" else f"{aid}_{ek}.mp4"
            lp = edir / fn
            log(f"    [{worker_slot}] {ek}: dl {rez}p {int(fsize)/(1024*1024):.0f}MB")

            # Fewer per-file connections (2) since we're running 4 in parallel = 8 total socket count
            if not dl_file(dl_url, lp, fsize, connections=2):
                update_episode(subject_id, ek, "download_failed"); continue

            log(f"    [{worker_slot}] {ek}: uploading")
            try:
                ia_upload(aid, lp, f"epmm/{fn}", is_last=is_last_ep, size_hint=total_bytes_hint if idx == 0 else None)
                update_episode(subject_id, ek, "uploaded", {"resolution": rez, "size": int(fsize)})
                log(f"    [{worker_slot}] {ek}: ok")
            except Exception as e:
                log(f"    [{worker_slot}] {ek}: upload fail {e}")
                update_episode(subject_id, ek, "upload_failed", {"error": str(e)[:200]})
            lp.unlink(missing_ok=True)

    log(f"  starting {PARALLEL_EPISODES} parallel episode workers ({len(ep_items)} episodes)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL_EPISODES) as tex:
        futs = [tex.submit(_pipeline_worker, i+1) for i in range(PARALLEL_EPISODES)]
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except Exception as e: log(f"  pipeline worker crashed: {e}")

    data, sha = gh_read(f"titles/{subject_id}.json")
    if data:
        ae = data.get("episodes", {})
        uploaded = [e for e in ae.values() if e.get("status") == "uploaded"]
        # If NOTHING actually got uploaded, drop the title entirely + blacklist
        if not uploaded:
            log(f"  {tname}: no episodes actually uploaded — removing + blacklisting")
            add_to_blacklist(subject_id, tname, ttype, "no_uploads")
            req_lib.delete(f"{GH}/repos/{DB_REPO}/contents/titles/{subject_id}.json",
                           headers=GH_H,
                           json={"message": f"{WORKER_ID} drops empty {subject_id}", "sha": sha},
                           timeout=30)
            # Try to delete the empty archive.org item — remove all uploaded metadata files
            try:
                s = _ia()
                item = s.get_item(aid)
                item.refresh()
                for f_ia in item.files:
                    if not f_ia["name"].startswith("_") and "meta" not in f_ia["name"] and "torrent" not in f_ia["name"] and "thumb" not in f_ia["name"]:
                        try:
                            item.delete_file(f_ia["name"])
                            log(f"    IA deleted {f_ia['name']}")
                        except Exception:
                            pass
            except Exception as e:
                log(f"    (couldn't clean IA item: {e})")
        else:
            data["status"] = "done" if all(e.get("status") in ("uploaded","no_source","no_link") for e in ae.values()) else "partial"
            data["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            save_title(subject_id, data, sha)

    shutil.rmtree(tdir, ignore_errors=True)
    log(f"  {tname} complete!")

# ================================= MAIN ==================================
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [{WORKER_ID}] {msg}", flush=True)

def main():
    log(f"Starting — mode={MODE}, max={MAX_TITLES}, quality={QUALITY}p")
    if MODE in ("discover", "both"):
        discover()
    if MODE in ("download", "both"):
        done = 0
        while done < MAX_TITLES:
            sid, claim_sha = pick_from_queue()
            if not sid:
                log("Queue empty"); break
            try:
                process_title(sid, claim_sha)
                done += 1
            except Exception as e:
                log(f"Error on {sid}: {e}")
                import traceback; traceback.print_exc()
                data, sha = gh_read(f"titles/{sid}.json")
                if data:
                    data["status"] = "error"; data["error"] = str(e)[:300]
                    save_title(sid, data, sha)
            time.sleep(1)
    log(f"Finished ({done if MODE in ('download','both') else 0} titles)")

if __name__ == "__main__":
    main()
