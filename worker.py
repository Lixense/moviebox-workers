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
    data, sha = gh_read(f"titles/{subject_id}.json")
    if not data:
        return
    if ep_key in data.get("episodes", {}):
        data["episodes"][ep_key]["status"] = status
        if extra:
            data["episodes"][ep_key].update(extra)
        data["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        save_title(subject_id, data, sha)

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

def add_to_queue(subject_ids):
    data, sha = gh_read("queue.json")
    if data is None:
        data = {"pending": []}
        sha = None
    existing_q = set(data["pending"])
    known = {f["name"].replace(".json", "") for f in gh_list_dir("titles")}
    new = [s for s in subject_ids if s not in existing_q and s not in known]
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
        new = [s for s in subject_ids if s not in existing_q and s not in known]
        data["pending"].extend(new)
    return 0

def remove_from_queue(subject_id):
    q, sha = gh_read("queue.json")
    if q and subject_id in q.get("pending", []):
        q["pending"] = [s for s in q["pending"] if s != subject_id]
        gh_write("queue.json", q, sha=sha)

def pick_from_queue():
    queue, _ = get_queue()
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
    ts2 = str(int(time.time()*1000))
    h = {"X-Play-Mode":"1","X-Client-Info":json.dumps(DEV,separators=(",",":")),"X-Client-Status":"1",
         "x-tr-signature":f"{ts}|2|{sig}",
         "X-Client-Token":f"{ts2},{hashlib.md5(ts2[::-1].encode()).hexdigest()}",
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
def mb_resource(sid, se, ep, res=1080):
    return mb_get("/wefeed-mobile-bff/subject-api/resource",
                  {"subjectId":str(sid),"resolution":str(res),"se":str(se),
                   "epFrom":str(ep),"epTo":str(ep),"page":"1","perPage":"50",
                   "all":"0","startPosition":"1","endPosition":"1","pagerMode":"0"})

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

def ia_upload(ident, local, remote, md=None):
    s = _ia()
    m = md or {}; m.setdefault("mediatype","movies"); m.setdefault("collection","opensource_movies")
    for a in range(5):
        try:
            s.get_item(ident).upload({remote:str(local)}, metadata=m, verify=True, retries=3, retries_sleep=10)
            return True
        except Exception as e:
            if "SlowDown" in str(e) or "503" in str(e): time.sleep(30*(a+1))
            elif a >= 4: raise
            else: time.sleep(10)
    return False

# ================================ DOWNLOAD ================================
def dl_file(url, dest_path, expected_size=None):
    dest = Path(dest_path); part = dest.with_suffix(dest.suffix + ".part")
    ex = part.stat().st_size if part.exists() else 0
    hdr = {"User-Agent": "okhttp/4.12.0"}
    if ex > 0: hdr["Range"] = f"bytes={ex}-"
    r = req_lib.get(url, headers=hdr, stream=True, timeout=60)
    if r.status_code == 416:
        if ex and expected_size and ex >= int(expected_size):
            part.rename(dest); return True
        part.unlink(missing_ok=True); ex = 0; hdr.pop("Range", None)
        r = req_lib.get(url, headers=hdr, stream=True, timeout=60)
    if r.status_code == 200: mode = "wb"
    elif r.status_code == 206: mode = "ab"
    else: log(f"DL HTTP {r.status_code}"); return False
    with open(part, mode) as f:
        for chunk in r.iter_content(131072): f.write(chunk)
    part.rename(dest); return True

# =============================== DISCOVERY ================================
def discover(max_pages=50):
    log("Discovering...")
    found = []
    for pg in range(1, max_pages + 1):
        data = mb_catalog(pg)
        if not data or "data" not in data: break
        items = data["data"].get("items", [])
        if not items: break
        for it in items:
            sid = str(it.get("subjectId", ""))
            if sid: found.append(sid)
        log(f"  Page {pg}: {len(items)} items")
        if not data["data"].get("pager",{}).get("hasMore"): break
        time.sleep(0.5)
    if found:
        added = add_to_queue(found)
        log(f"Discovery: {len(found)} seen, {added} new")
    else:
        log("Discovery: nothing")

# ============================ PROCESS TITLE ===============================
def process_title(subject_id, claim_sha):
    log(f"Processing {subject_id}")

    # Full detail from API
    detail = mb_detail(subject_id)
    dd = detail.get("data", {}) if detail else {}
    tname = dd.get("title", "Unknown")
    ttype = "movie" if dd.get("subjectType") == 1 else "series"
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
    res_check = mb_resource(arabic_id, 1, 1, 1, QUALITY)
    if res_check and "data" in res_check:
        arabic_title = res_check["data"].get("subjectTitle", "")
        total_size = res_check["data"].get("totalSize", "0")
        total_eps_api = res_check["data"].get("totalEpisode", 0)
    else:
        total_size = "0"
        total_eps_api = 0

    # Seasons and episodes
    eps = []
    season_info = []
    if ttype == "series":
        sd = mb_seasons(arabic_id)
        if sd and "data" in sd:
            sl = sd["data"].get("seasons", [sd["data"]] if isinstance(sd["data"], dict) else [])
            for s in sl:
                se = s.get("se", 1)
                all_ep = s.get("allEp", "") or ""
                ep_nums = []
                if all_ep:
                    for e in all_ep.split(","):
                        e = e.strip()
                        if e.isdigit():
                            eps.append((se, int(e)))
                            ep_nums.append(int(e))
                else:
                    for e in range(1, s.get("maxEp", 0) + 1):
                        eps.append((se, e))
                        ep_nums.append(e)
                season_info.append({"season": se, "episodes": ep_nums})
    if not eps:
        eps = [(1, 1)]
        season_info = [{"season": 1, "episodes": [1]}]

    # Reserve archive ID
    aid = mk_aid()
    while ia_exists(aid): aid = mk_aid()
    log(f"  Archive: {aid}")

    ep_dict = {}
    for se, ep in eps:
        ep_dict[f"S{se:02d}E{ep:02d}"] = {"season": se, "episode": ep, "status": "pending"}

    title_data = {
        "subject_id": subject_id, "arabic_id": arabic_id, "archive_id": aid,
        "title": tname, "type": ttype, "account": IA_ACCOUNT,
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

    for ek, ei in ep_dict.items():
        heartbeat(subject_id)
        se, ep = ei["season"], ei["episode"]
        log(f"    {ek}...")

        res = mb_resource(arabic_id, se, ep, QUALITY)
        if not res or "data" not in res or not res["data"].get("list"):
            log(f"    {ek}: no source"); update_episode(subject_id, ek, "no_source"); continue

        r0 = res["data"]["list"][0]
        dl_url = r0.get("resourceLink",""); fsize = r0.get("size","0"); rez = r0.get("resolution",0)
        if not dl_url: update_episode(subject_id, ek, "no_link"); continue

        fn = f"{aid}_movie.mp4" if ttype == "movie" else f"{aid}_{ek}.mp4"
        lp = edir / fn
        log(f"    {ek}: dl {rez}p {int(fsize)/(1024*1024):.0f}MB")

        if not dl_file(dl_url, lp, fsize):
            update_episode(subject_id, ek, "download_failed"); continue

        log(f"    {ek}: uploading")
        try:
            ia_upload(aid, lp, f"epmm/{fn}")
            update_episode(subject_id, ek, "uploaded", {"resolution": rez, "size": int(fsize)})
            log(f"    {ek}: ok")
        except Exception as e:
            log(f"    {ek}: upload fail {e}")
            update_episode(subject_id, ek, "upload_failed", {"error": str(e)[:200]})
        lp.unlink(missing_ok=True)

    data, sha = gh_read(f"titles/{subject_id}.json")
    if data:
        ae = data.get("episodes", {})
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
