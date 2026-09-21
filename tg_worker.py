#!/usr/bin/env python3
"""MovieBox -> Telegram worker. Uses Local Bot API Server for 2GB uploads."""
import base64, hashlib, hmac, json, os, random, sys, time, urllib.parse, uuid
import tempfile, re, requests
from pathlib import Path

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
DB_REPO = os.environ["DB_REPO"]
TG_TOKEN = os.environ["TG_BOT_TOKEN"]
TG_CHATS = [c for c in [os.environ.get("TG_CHAT_1",""), os.environ.get("TG_CHAT_2","")] if c]
TG_API = os.environ.get("TG_API_BASE", "https://api.telegram.org")
WORKER_ID = os.environ.get("WORKER_ID", f"tg-{uuid.uuid4().hex[:8]}")
MAX_TITLES = int(os.environ.get("MAX_TITLES", "2"))
QUALITY = int(os.environ.get("QUALITY", "720"))
WORK_DIR = Path(tempfile.gettempdir()) / "tg-work"
WORK_DIR.mkdir(exist_ok=True)

def log(m): print(f"[{time.strftime('%H:%M:%S')}] [{WORKER_ID}] {m}", flush=True)

GH = "https://api.github.com"
GH_H = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

def gh_read(p):
    r = requests.get(f"{GH}/repos/{DB_REPO}/contents/{p}", headers=GH_H, timeout=30)
    if r.status_code == 404: return None, None
    r.raise_for_status(); d = r.json()
    return json.loads(base64.b64decode(d["content"]).decode()), d["sha"]

def gh_write(p, data, sha=None, msg=None):
    enc = base64.b64encode(json.dumps(data, ensure_ascii=False, separators=(",",":")).encode()).decode()
    pay = {"message": msg or f"{WORKER_ID} {p}", "content": enc}
    if sha: pay["sha"] = sha
    r = requests.put(f"{GH}/repos/{DB_REPO}/contents/{p}", headers=GH_H, json=pay, timeout=30)
    if r.status_code in (409, 422): return None
    if r.status_code in (200, 201): return r.json()["content"]["sha"]
    r.raise_for_status()

def gh_list_dir(p):
    r = requests.get(f"{GH}/repos/{DB_REPO}/contents/{p}", headers=GH_H, timeout=30)
    if r.status_code == 404: return []
    r.raise_for_status(); items = r.json()
    return [{"name": i["name"]} for i in items] if isinstance(items, list) else []

def try_claim(sid):
    ex, sha = gh_read(f"titles/{sid}.json")
    if ex: return None
    return gh_write(f"titles/{sid}.json",
        {"subject_id": sid, "status": "claimed", "worker": WORKER_ID,
         "claimed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
        sha=None, msg=f"{WORKER_ID} claims {sid}")

def save_title(sid, data, sha):
    return gh_write(f"titles/{sid}.json", data, sha=sha, msg=f"{WORKER_ID} updates {sid}")

def get_queue():
    data, sha = gh_read("queue.json")
    return (data.get("pending", []), sha) if data else ([], None)

def add_to_queue(sids):
    data, sha = gh_read("queue.json")
    if data is None: data, sha = {"pending": []}, None
    existing = set(data["pending"])
    known = {f["name"].replace(".json","") for f in gh_list_dir("titles")}
    bl, _ = gh_read("blacklist.json")
    blacklist = set((bl or {}).get("ids", {}).keys())
    new = [s for s in sids if s not in existing and s not in known and s not in blacklist]
    if not new: return 0
    data["pending"].extend(new)
    for _ in range(5):
        r = gh_write("queue.json", data, sha=sha)
        if r: return len(new)
        time.sleep(random.uniform(1, 3))
        data, sha = gh_read("queue.json")
        if data is None: data, sha = {"pending": []}, None
        existing = set(data["pending"])
        new = [s for s in sids if s not in existing and s not in known and s not in blacklist]
        data["pending"].extend(new)
    return 0

def pick_from_queue():
    queue, _ = get_queue()
    random.shuffle(queue)
    for sid in queue:
        cs = try_claim(sid)
        if cs:
            log(f"Claimed {sid}")
            q, qsha = gh_read("queue.json")
            if q and sid in q.get("pending", []):
                q["pending"] = [s for s in q["pending"] if s != sid]
                gh_write("queue.json", q, sha=qsha)
            return sid, cs
    return None, None

def add_to_blacklist(sid, title="", ttype="", reason=""):
    for _ in range(5):
        data, sha = gh_read("blacklist.json")
        if data is None: data, sha = {"ids": {}}, None
        data["ids"][str(sid)] = {"title": title, "type": ttype, "reason": reason}
        r = gh_write("blacklist.json", data, sha=sha, msg=f"{WORKER_ID} blacklists {sid}")
        if r: return
        time.sleep(random.uniform(0.5, 2))

# MovieBox API
MB_KEY = base64.urlsafe_b64decode("76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O==")
MB_JWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjIwOTI2Mjc0MjUzNjMzODUwNDgsImV4cCI6MTc5NzUwOTcxOSwiaWF0IjoxNzg5NzMzNDE5fQ.lk5GbDkDTlP3kuw1LgLmuRhsrF1ugN8BvQrZFb_HqQo"
MB_BASES = ["https://api6.aoneroom.com", "https://api5.aoneroom.com", "https://api4.aoneroom.com"]
DEV = {"package_name":"com.community.oneroom","version_name":"3.0.03.0528.03","version_code":50020041,
       "os":"android","os_version":"13","install_ch":"ps",
       "device_id": hashlib.md5(uuid.uuid4().bytes).hexdigest(),
       "install_store":"ps","gaid":str(uuid.uuid4()),"brand":"Samsung","model":"SM-G991B",
       "system_language":"en","net":"NETWORK_WIFI","region":"US",
       "timezone":"America/New_York","sp_code":"26202","X-Play-Mode":"1"}
_sess = requests.Session(); _sess.headers.clear()

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
    return ts, base64.b64encode(hmac.new(MB_KEY, ss.encode(), hashlib.md5).digest()).decode()

def _hdr(method, url, body=None):
    ts, sig = _sign(method, url, body)
    h = {"X-Play-Mode":"1", "X-Client-Info": json.dumps(DEV, separators=(",",":")),
         "X-Client-Status":"1", "x-tr-signature": f"{ts}|2|{sig}",
         "Authorization": f"Bearer {MB_JWT}", "User-Agent": "okhttp/4.12.0"}
    if body: h["Content-Type"] = "application/json; charset=UTF-8"
    return h

def mb_get(path, params=None):
    url = MB_BASES[0] + path + ("?" + urllib.parse.urlencode(params) if params else "")
    for b in MB_BASES:
        try:
            r = _sess.get(url.replace(MB_BASES[0], b), headers=_hdr("GET", url.replace(MB_BASES[0], b)), timeout=20)
            if r.status_code == 200: return r.json()
        except: continue
    return None

def mb_post(path, data):
    url = MB_BASES[0] + path; body = json.dumps(data, separators=(",",":"))
    for b in MB_BASES:
        try:
            u = url.replace(MB_BASES[0], b)
            r = _sess.post(u, data=body.encode(), headers=_hdr("POST", u, body), timeout=20)
            if r.status_code == 200: return r.json()
        except: continue
    return None

def mb_catalog(page=1): return mb_post("/wefeed-mobile-bff/subject-api/list", {"page": page, "pageSize": 20})
def mb_detail(sid): return mb_get("/wefeed-mobile-bff/subject-api/get", {"subjectId": str(sid)})
def mb_seasons(sid): return mb_get("/wefeed-mobile-bff/subject-api/season-info", {"subjectId": str(sid)})
def mb_dub(sid): return mb_get("/wefeed-mobile-bff/subject-api/dub-info", {"subjectId": str(sid), "episodeId": "0"})

def mb_resource_all(sid, se, res=1080):
    out = {}; ef = 1
    while ef <= 200:
        et = ef + 19
        r = mb_get("/wefeed-mobile-bff/subject-api/resource",
            {"subjectId": str(sid), "resolution": str(res), "se": str(se),
             "epFrom": str(ef), "epTo": str(et), "page": "1", "perPage": "20",
             "all": "1", "startPosition": str(ef), "endPosition": str(et), "pagerMode": "0"})
        if not r or "data" not in r: break
        items = r["data"].get("list", [])
        if not items: break
        for it in items:
            if int(it.get("se", 0)) == int(se): out[int(it.get("ep", 0))] = it
        if len(items) < 20: break
        ef += 20
    return out

def get_arabic_id(sid):
    data = mb_dub(sid)
    if not data or "data" not in data: return str(sid)
    for d in data["data"].get("dubs", []):
        if d.get("lanCode") == "ar" and d.get("type") == 1: return str(d["subjectId"])
    return str(sid)

# Telegram upload
def tg_upload(fp, chat_id, caption=""):
    sz = os.path.getsize(fp)
    log(f"    Uploading {sz/(1024*1024):.0f}MB to Telegram...")
    t0 = time.time()
    is_local = TG_API.startswith("http://localhost")
    if is_local:
        # Local Bot API: use file:// path (zero-copy, fastest possible)
        abs_path = os.path.abspath(fp)
        r = requests.post(f"{TG_API}/bot{TG_TOKEN}/sendVideo",
            json={"chat_id": chat_id, "video": f"file://{abs_path}",
                  "caption": caption, "supports_streaming": True},
            timeout=1800)
    else:
        # Standard API: multipart upload
        with open(fp, "rb") as f:
            r = requests.post(f"{TG_API}/bot{TG_TOKEN}/sendDocument",
                data={"chat_id": chat_id, "caption": caption},
                files={"document": (os.path.basename(fp), f)},
                timeout=1800)
    el = time.time() - t0
    if r.status_code == 200 and r.json().get("ok"):
        res = r.json()["result"]
        vid = res.get("video") or res.get("document", {})
        log(f"    Uploaded {el:.0f}s ({sz/el/(1024*1024):.1f}MB/s)")
        return vid.get("file_id", ""), vid.get("file_unique_id", "")
    log(f"    Upload FAILED: {r.text[:200]}")
    return None, None

def dl_file(url, dest):
    r = requests.get(url, headers={"User-Agent": "okhttp/4.12.0"}, stream=True, timeout=120)
    if r.status_code != 200: return False
    with open(dest, "wb") as f:
        for c in r.iter_content(524288): f.write(c)
    return True

# Discovery
def discover(max_pages=50):
    log("Discovering...")
    found = []; junk = 0
    for pg in range(1, max_pages + 1):
        data = mb_catalog(pg)
        if not data or "data" not in data: break
        items = data["data"].get("items", [])
        if not items: break
        for it in items:
            sid = str(it.get("subjectId", ""))
            st = it.get("subjectType", 0)
            if not sid: continue
            if st not in (1, 2): junk += 1; continue
            found.append(sid)
        log(f"  Page {pg}: {len(items)} items ({junk} junk)")
        if not data["data"].get("pager", {}).get("hasMore"): break
        time.sleep(0.5)
    if found:
        added = add_to_queue(found)
        log(f"Discovery: {len(found)} real, {junk} junk, {added} new")

# Process one title
def process_title(sid, claim_sha):
    log(f"Processing {sid}")
    detail = mb_detail(sid)
    dd = detail.get("data", {}) if detail else {}
    tname = dd.get("title", "Unknown")
    st = dd.get("subjectType", 0)
    if st not in (1, 2):
        log(f"  {tname}: not movie/series -- blacklisting")
        add_to_blacklist(sid, tname, f"type_{st}", "not_movie_or_series")
        _, s0 = gh_read(f"titles/{sid}.json")
        if s0: requests.delete(f"{GH}/repos/{DB_REPO}/contents/titles/{sid}.json",
            headers=GH_H, json={"message": f"{WORKER_ID} rejects {sid}", "sha": s0}, timeout=30)
        return
    ttype = "movie" if st == 1 else "series"
    arabic_id = get_arabic_id(sid)
    log(f"  {tname} ({ttype}) arabic={arabic_id}")

    eps = []; sc = {}
    if ttype == "series":
        sd = mb_seasons(arabic_id)
        if sd and "data" in sd:
            for s in sd["data"].get("seasons", [sd["data"]] if isinstance(sd["data"], dict) else []):
                se = s.get("se", 1)
                av = mb_resource_all(arabic_id, se, QUALITY)
                if not av: continue
                sc[se] = av
                for e in sorted(av.keys()): eps.append((se, e))
                log(f"  S{se}: {len(av)} episodes")
    else:
        probe = mb_resource_all(arabic_id, 1, QUALITY)
        if probe and 1 in probe: sc[1] = probe; eps = [(1, 1)]

    if not eps:
        log(f"  {tname}: no sources -- blacklisting")
        add_to_blacklist(sid, tname, ttype, "no_source")
        _, s0 = gh_read(f"titles/{sid}.json")
        if s0: requests.delete(f"{GH}/repos/{DB_REPO}/contents/titles/{sid}.json",
            headers=GH_H, json={"message": f"{WORKER_ID} drops {sid}", "sha": s0}, timeout=30)
        return

    chat_id = TG_CHATS[hash(sid) % len(TG_CHATS)]
    ep_dict = {f"S{se:02d}E{ep:02d}": {"season": se, "episode": ep, "status": "pending"} for se, ep in eps}
    td = {"subject_id": sid, "arabic_id": arabic_id, "title": tname, "type": ttype,
          "description": dd.get("description", ""), "genre": dd.get("genre", ""),
          "cover_url": dd.get("cover", {}).get("url", ""),
          "imdb_rating": dd.get("imdbRatingValue", ""),
          "chat_id": chat_id, "status": "processing", "worker": WORKER_ID,
          "episodes": ep_dict,
          "claimed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    save_title(sid, td, claim_sha)

    # Pipeline: download next episode while uploading current one
    import concurrent.futures, threading

    def do_episode(se, ep):
        ek = f"S{se:02d}E{ep:02d}"
        r0 = sc.get(se, {}).get(ep)
        if not r0: return
        dl_url = r0.get("resourceLink", "")
        fsize = r0.get("size", "0")
        rez = r0.get("resolution", 0)
        if not dl_url: return
        fn = f"{tname[:30]}_{ek}_{rez}p.mp4".replace(" ", "_").replace("/", "_")
        lp = WORK_DIR / fn
        log(f"  {ek}: dl {rez}p {int(fsize)/(1024*1024):.0f}MB")
        if not dl_file(dl_url, lp):
            log(f"  {ek}: dl failed"); return
        cap = f"{tname} - {ek} ({rez}p)"
        fid, fuid = tg_upload(str(lp), chat_id, cap)
        lp.unlink(missing_ok=True)
        if fid:
            for _ in range(5):
                data, sha = gh_read(f"titles/{sid}.json")
                if not data: break
                data["episodes"][ek]["status"] = "uploaded"
                data["episodes"][ek]["file_id"] = fid
                data["episodes"][ek]["file_unique_id"] = fuid
                data["episodes"][ek]["resolution"] = rez
                data["episodes"][ek]["size"] = int(fsize)
                if save_title(sid, data, sha): break
                time.sleep(random.uniform(0.5, 2))
            log(f"  {ek}: OK")
        else:
            log(f"  {ek}: upload failed")

    # Run 2 episodes in parallel (one downloading while other uploads)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(do_episode, se, ep) for se, ep in eps]
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except Exception as e: log(f"  Episode error: {e}")

    data, sha = gh_read(f"titles/{sid}.json")
    if data:
        ae = data.get("episodes", {})
        uploaded = [e for e in ae.values() if e.get("status") == "uploaded"]
        if not uploaded:
            log(f"  {tname}: nothing uploaded -- removing")
            add_to_blacklist(sid, tname, ttype, "no_uploads")
            requests.delete(f"{GH}/repos/{DB_REPO}/contents/titles/{sid}.json",
                headers=GH_H, json={"message": f"{WORKER_ID} drops {sid}", "sha": sha}, timeout=30)
        else:
            data["status"] = "done" if all(e.get("status") in ("uploaded", "no_source") for e in ae.values()) else "partial"
            data["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            save_title(sid, data, sha)
            log(f"  {tname} complete! ({len(uploaded)}/{len(ae)} eps)")

def main():
    log(f"Starting -- max={MAX_TITLES}, quality={QUALITY}p, chats={TG_CHATS}")
    r = requests.get(f"{TG_API}/bot{TG_TOKEN}/getMe", timeout=15)
    if r.json().get("ok"):
        log(f"Bot: @{r.json()['result']['username']}")
    else:
        log(f"Bot FAILED: {r.text[:200]}"); return
    discover()
    done = 0
    while done < MAX_TITLES:
        sid, cs = pick_from_queue()
        if not sid: log("Queue empty"); break
        try:
            process_title(sid, cs); done += 1
        except Exception as e:
            log(f"Error on {sid}: {e}")
            import traceback; traceback.print_exc()
            data, sha = gh_read(f"titles/{sid}.json")
            if data:
                data["status"] = "error"; data["error"] = str(e)[:300]
                save_title(sid, data, sha)
        time.sleep(1)
    log(f"Finished ({done} titles)")

if __name__ == "__main__":
    main()
