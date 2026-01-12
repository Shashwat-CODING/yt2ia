import asyncio
import aiohttp
import requests
import os
import time
import urllib.parse
from database import save_entry, get_all_video_ids, init_db, check_video_exists
from internetarchive import upload
import re
import logging

# S3 Config
IA_ACCESS_KEY = "YXzY5OrREXLL6XBh"
IA_SECRET_KEY = "m2XnL3X7xCB1pNGE"

# Status Tracking Structure
STATUS = {
    "active_jobs": {},
    "queue": [],
    "logs": [],
    "stats": {
        "processed": 0,
        "skipped": 0
    }
}

class ListHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        STATUS["logs"].append(msg)
        if len(STATUS["logs"]) > 100:
            STATUS["logs"].pop(0)

logger = logging.getLogger("yt2ia")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler("app.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
list_handler = ListHandler()
list_handler.setFormatter(formatter)
logger.addHandler(list_handler)

def set_status(video_id, status_msg, progress=0, state="running"):
    STATUS["active_jobs"][video_id] = {
        "status": status_msg,
        "progress": progress,
        "state": state
    }
    if state != "downloading": 
        logger.info(f"[{video_id}] {status_msg}")

def remove_status(video_id):
    if video_id in STATUS["active_jobs"]:
        del STATUS["active_jobs"][video_id]

YTIFY_API_BASE = "https://ytify-backend.vercel.app/api"
INVIDIOUS_INSTANCES = [
  "https://compilations.broquemonsieur.net",
  "https://invidious.frontendfriendly.xyz",
  "https://invidious.privacyfucking.rocks",
  "https://invidious.privacyredirect.com",
  "https://invidious.darkness.services",
  "https://invidious.technicalvoid.dev",
  "https://invidious.einfachzocken.eu",
  "https://invidious.tail5b365.ts.net",
  "https://invidious.hyperreal.coffee",
  "https://invidious.schenkel.eti.br",
  "https://invidious.nikkosphere.com",
  "https://invidious.projectsegfault",
  "https://invidious.perennialte.ch",
  "https://invidious.private.coffee",
  "https://invidious.incogniweb.net",
  "https://invidious.reallyaweso.me",
  "https://invidious.tiekoetter.com",
  "https://invidious.protokolla.fi",
  "https://inv.in.projectsegfau.lt",
  "https://inv.us.projectsegfau.lt",
  "https://invidious.materialio.us",
  "https://invidious.yourdevice.ch",
  "https://invidious.adminforge.de",
  "https://invidious.drgns.space",
  "https://invidious.no-logs.com",
  "https://invidious.neomode.net",
  "https://invidious.catspeed.cc",
  "https://invidious.nogafa.org",
  "https://invidious.nerdvpn.de",
  "https://anontube.lvkaszus.pl",
  "https://invidious.nirn.quest",
  "https://invidious.jing.rocks",
  "https://invidious.polido.pt",
  "https://invidious.ozzy.baby",
  "https://invi.susurrando.com",
  "https://invidious.lunar.icu",
  "https://invidious.baczek.me",
  "https://invidious.asir.dev",
  "https://invidious.danii.cc",
  "https://youtube.owacon.moe",
  "https://not-ytb.blocus.ch",
  "https://inv.instances.one",
  "https://iv.datura.network",
  "https://inv.makerlab.tech",
  "https://invidious.12a.app",
  "https://inv.everypizza.im",
  "https://inv.stealthy.club",
  "https://yt.fascinated.cc",
  "https://invidious.fdn.fr",
  "https://invidious.io.LOL",
  "https://yt.pwnwriter.xyz",
  "https://inv.perditum.com",
  "https://inv.clovius.club",
  "https://invidious.f5.si",
  "https://zerotube.p-e.kr",
  "https://iv.melmac.space",
  "https://echostreamz.com",
  "https://yt.floss.media",
  "https://iv.ggtyler.dev",
  "https://inv.nadeko.net",
  "https://yt.funami.tech",
  "https://inv.tux.pizza",
  "https://yt.drgnz.club",
  "https://invi.danii.cc",
  "https://inv.citw.lgbt",
  "https://vid.lilay.dev",
  "https://inv.oikei.net",
  "https://id.420129.xyz",
  "https://yt.omada.cafe",
  "https://yt.upgods.us",
  "https://iv.nboeck.de",
  "https://just4cats.tv",
  "https://inv.n8pjl.ca",
  "https://iv.zorby.top",
  "https://iv.fmac.xyz",
  "https://yt.cdaut.de",
  "https://iv.duti.dev",
  "https://inv.qilk.de",
  "https://inv.vern.cc",
  "https://yt.lsyy.cf",
  "https://lekker.gay",
  "https://yt.owo.si",
  "https://inv.vy.ci",
  "https://y.com.sb"
]

PIPED_INSTANCES = [
  "https://pipedapi.kavin.rocks",
  "https://pipedapi.tokhmi.xyz",
  "https://pipedapi.moomoo.me",
  "https://pipedapi.syncpundit.io",
  "https://api-piped.mha.fi",
  "https://piped-api.garudalinux.org",
  "https://pipedapi.rivo.lol",
  "https://pipedapi.leptons.xyz",
  "https://piped-api.lunar.icu",
  "https://ytapi.dc09.ru",
  "https://pipedapi.colinslegacy.com",
  "https://yapi.vyper.me",
  "https://api.looleh.xyz",
  "https://piped-api.cfe.re",
  "https://pipedapi.r4fo.com",
  "https://pipedapi.nosebs.ru",
  "https://pipedapi-libre.kavin.rocks",
  "https://pa.mint.lgbt",
  "https://pa.il.ax",
  "https://piped-api.privacy.com.de",
  "https://api.piped.projectsegfau.lt",
  "https://pipedapi.in.projectsegfau.lt",
  "https://pipedapi.us.projectsegfau.lt",
  "https://watchapi.whatever.social",
  "https://api.piped.privacydev.net",
  "https://pipedapi.palveluntarjoaja.eu",
  "https://pipedapi.smnz.de",
  "https://pipedapi.adminforge.de",
  "https://pipedapi.qdi.fi",
  "https://piped-api.hostux.net",
  "https://pdapi.vern.cc",
  "https://pipedapi.pfcd.me",
  "https://pipedapi.frontendfriendly.xyz",
  "https://api.piped.yt",
  "https://pipedapi.astartes.nl",
  "https://pipedapi.osphost.fi",
  "https://pipedapi.simpleprivacy.fr",
  "https://pipedapi.drgns.space",
  "https://piapi.ggtyler.dev",
  "https://api.watch.pluto.lat",
  "https://piped-backend.seitan-ayoub.lol",
  "https://pipedapi.owo.si",
  "https://api.piped.minionflo.net",
  "https://pipedapi.nezumi.party",
  "https://pipedapi.ducks.party",
  "https://pipedapi.ngn.tf",
  "https://pipedapi.coldforge.xyz",
  "https://piped-api.codespace.cz",
  "https://pipedapi.reallyaweso.me",
  "https://pipedapi.phoenixthrush.com",
  "https://api.piped.private.coffee",
  "https://schaunapi.ehwurscht.at",
  "https://pipedapi.darkness.services",
  "https://pipedapi.andreafortuna.org",
  "https://piped.wireway.ch",
  "https://piped-nextgen.xn--17b.net",
  "https://nuv3d-7iaaa-aaaan-qahma-cai.ic0.app",
  "https://piped.syncpundit.io",
  "https://piped.ezero.space",
  "https://pipedapi.orangenet.cc"
]

# ---------- HELPER FUNCTIONS ----------

async def fetch_from_instance(session, video_id, instance_url):
    try:
        url = f"{instance_url}/api/v1/videos/{video_id}"
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                return instance_url, data
    except Exception:
        pass
    return None

async def fetch_details_parallel(video_id):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch_from_instance(session, video_id, url)) 
                for url in INVIDIOUS_INSTANCES]
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                if result:
                    for t in tasks: t.cancel()
                    return result
            except: pass
    return None, None

async def fetch_from_piped(session, video_id, instance_url):
    try:
        url = f"{instance_url}/streams/{video_id}"
        async with session.get(url, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('audioStreams') or data.get('videoStreams'):
                    return data
    except: pass
    return None

async def fetch_piped_parallel(video_id):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_from_piped(session, video_id, inst) for inst in PIPED_INSTANCES]
        for future in asyncio.as_completed(tasks):
            result = await future
            if result:
                return result
    return None

def fetch_details_rapidapi(video_id):
    url = "https://yt-api.p.rapidapi.com/dl"
    headers = {
        "x-rapidapi-key": "eee55a9833msh8f2dbd8e2b7970bp194fefjsn646e78",
        "x-rapidapi-host": "yt-api.p.rapidapi.com"
    }
    try:
        response = requests.get(url, headers=headers, params={"id": video_id, "cgeo": "US"}, timeout=15)
        if response.status_code == 200:
            return response.json()
    except: pass
    return None

# ---------- SOURCE FINDERS ----------

def parse_artists(artist_string):
    if not artist_string: return []
    separators = [', ', ' & ', ' and ', ' x ', ' X ', ' feat. ', ' feat ', ' ft. ', ' ft ', ' featuring ', ' Featuring ']
    artists = [artist_string]
    for sep in separators:
        new_ar = []
        for a in artists: new_ar.extend(a.split(sep))
        artists = new_ar
    return [a.strip() for a in artists if a.strip()]

def find_jiosaavn(title, artist):
    if not title or not artist: return None, None
    try:
        # Simple clean info
        artist = parse_artists(artist)
        if not artist: return None, None
        
        q_title = urllib.parse.quote(title.strip())
        q_artist = urllib.parse.quote(','.join(artist))
        url = f"{YTIFY_API_BASE}/jiosaavn/search?title={q_title}&artist={q_artist}&debug=0"
        
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            d = resp.json()
            if d and d.get("downloadUrl"):
                return d.get("downloadUrl"), {'type': 'audio/mp4'}
    except: pass
    return None, None

async def find_invidious(video_id, current_metadata=None):
    # If we already have metadata from somewhere, good. But we need an instance to proxy.
    # So we always fetch
    instance, data = await fetch_details_parallel(video_id)
    if not data: return None, None, None

    # Metadata extraction
    meta = {
        'title': data.get('title', 'Unknown'),
        'artist': data.get('author', 'Unknown')
    }
    if meta['artist'].endswith(" - Topic"): meta['artist'] = meta['artist'].replace(" - Topic", "")

    # Format
    adaptive = data.get('adaptiveFormats', [])
    audio = [f for f in adaptive if f.get('type', '').startswith('audio')]
    if not audio: return None, None, meta

    audio.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
    best = audio[0]
    raw_url = best.get('url')
    
    if raw_url:
        # PROXY via Instance
        # Standard Invidious proxy: {instance}/videoplayback?{query}
        try:
            parsed = urllib.parse.urlparse(raw_url)
            proxy_url = f"{instance}/videoplayback?{parsed.query}"
            return proxy_url, best, meta
        except:
             return raw_url, best, meta # Fallback to raw

    return None, None, meta

async def find_piped(video_id):
    data = await fetch_piped_parallel(video_id)
    if not data: return None, None
    
    streams = data.get('audioStreams', [])
    if not streams: return None, None
    
    streams.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
    best = streams[0]
    return best.get('url'), {'type': best.get('mimeType', 'audio/mp4')}

def find_zeabur(video_id):
    try:
        url = f"https://hdtkfyf.zeabur.app/api/dl/{video_id}"
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            formats = data.get('audio_formats', [])
            formats.sort(key=lambda x: x.get('filesize', 0) or 0, reverse=True)
            if formats:
                best = formats[0]
                raw = best.get('url')
                if raw:
                    enc = urllib.parse.quote(raw)
                    return f"https://hdtkfyf.zeabur.app/api/proxy?url={enc}", best
    except: pass
    return None, None

def find_rapidapi(video_id):
    data = fetch_details_rapidapi(video_id)
    if not data: return None, None, None
    
    meta = {
        'title': data.get('title', 'Unknown'),
        'artist': data.get('channelTitle', 'Unknown')
    }
    adaptive = data.get('adaptiveFormats', [])
    audio = [f for f in adaptive if f.get('type', '').startswith('audio')]
    if audio:
        audio.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
        return audio[0].get('url'), audio[0], meta
    return None, None, meta

# ---------- MAIN PROCESSING ----------

async def process_video_async(video_id, metadata_override=None):
    # 0. Check Loop/DB Skip
    if check_video_exists(video_id):
         logger.info(f"[{video_id}] Skipped (Already in DB)")
         set_status(video_id, "Skipped (Already Exists)")
         STATUS["stats"]["skipped"] += 1
         remove_status(video_id)
         return

    title = metadata_override.get('title', 'Unknown') if metadata_override else "Unknown"
    author = metadata_override.get('artist', 'Unknown') if metadata_override else "Unknown"
    
    # 1. Resolve Metadata if missing
    if title == "Unknown":
        set_status(video_id, "Resolving Metadata...")
        # Brief check just for title/author
        _, i_data = await fetch_details_parallel(video_id)
        if i_data:
            title = i_data.get('title', 'Unknown')
            author = i_data.get('author', 'Unknown')
        else:
             r_data = fetch_details_rapidapi(video_id)
             if r_data:
                 title = r_data.get('title', 'Unknown')
                 author = r_data.get('channelTitle', 'Unknown')
        
        if author.endswith(" - Topic"): author = author.replace(" - Topic", "")
    
    logger.info(f"[{video_id}] Metadata: {title} - {author}")

    # 2. Try Download Chain
    # We define a list of steps. Each step returns (url, format_info) or None
    # If download fails, we continue loop.
    
    success_file = None
    
    # Define steps
    # Each step: name, async_func -> (url, format_data)
    
    # We wrap synchronous calls in async wrappers for uniformity
    async def step_jio(): return find_jiosaavn(title, author)
    async def step_inv(): 
        u, f, _ = await find_invidious(video_id)
        return u, f
    async def step_piped(): return await find_piped(video_id)
    async def step_zeabur(): return find_zeabur(video_id) # Sync but fast enough or threads
    async def step_rapid():
        u, f, _ = find_rapidapi(video_id)
        return u, f

    steps = [
        ("JioSaavn", step_jio),
        ("Invidious", step_inv),
        ("Piped", step_piped),
        ("Zeabur", step_zeabur),
        ("RapidAPI", step_rapid)
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.youtube.com/"
    }

    for name, func in steps:
        set_status(video_id, f"Checking {name}...")
        logger.info(f"[{video_id}] Trying {name}")
        
        try:
            url, fmt = await func()
        except:
            url, fmt = None, None
            
        if not url:
            logger.info(f"[{video_id}] {name} found no URL")
            continue
            
        # Try Download
        try:
            # Filename logic
            ext = "webm"
            if fmt:
                mime = fmt.get('type', '') or fmt.get('mimeType', '')
                if 'mp4' in mime or 'm4a' in mime: ext = "m4a"
            
            fname = f"{video_id}.{ext}"
            fpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), fname)
            
            logger.info(f"[{video_id}] Downloading from {name}: {url}")
            
            # Stream download
            with requests.get(url, headers=headers, stream=True, timeout=20) as r:
                if r.status_code != 200:
                    logger.warning(f"[{video_id}] {name} download failed: {r.status_code}")
                    continue # Try next source on HTTP error
                
                total = int(r.headers.get('content-length', 0))
                dl = 0
                with open(fpath, 'wb') as f:
                    for chunk in r.iter_content(1024*1024):
                        if chunk:
                            f.write(chunk)
                            dl += len(chunk)
                            if total:
                                pct = int(dl/total * 100)
                                set_status(video_id, f"Downloading ({name}): {pct}%", progress=pct, state="downloading")
            
            # If we get here, download finished
            logger.info(f"[{video_id}] Download success from {name}")
            success_file = fpath
            break # Exit loop
            
        except Exception as e:
            logger.warning(f"[{video_id}] {name} download exception: {e}")
            if os.path.exists(fpath): os.remove(fpath)
            continue # Try next source
            
    if not success_file:
         set_status(video_id, "ERROR: All sources failed")
         remove_status(video_id)
         return

    # 3. Upload
    set_status(video_id, "Uploading...")
    try:
        aid = metadata_override.get('artist_id') if metadata_override else None
        identifier = f"yt2ia-{aid}" if aid else "YTMBACKUP"
        
        md = {'title': title, 'creator': author, 'mediatype': 'audio', 'collection': 'opensource_audio'}
        r = upload(identifier, files={os.path.basename(success_file): success_file}, metadata=md, access_key=IA_ACCESS_KEY, secret_key=IA_SECRET_KEY)
        
        if r and r[0].status_code == 200:
            ia_url = f"https://archive.org/download/{identifier}/{os.path.basename(success_file)}"
            save_entry(f"{title} - {author}", ia_url)
            STATUS["stats"]["processed"] += 1
            logger.info(f"[{video_id}] Upload Complete")
        else:
             logger.error(f"[{video_id}] Upload failed")
             
    except Exception as e:
        logger.error(f"[{video_id}] Upload error: {e}")
    
    if os.path.exists(success_file): os.remove(success_file)
    remove_status(video_id)

def process_video_task(video_id, metadata_override=None):
    if video_id in STATUS["active_jobs"]: return
    set_status(video_id, "Starting...")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_video_async(video_id, metadata_override))
        loop.close()
    except Exception as e:
        logger.error(f"Task error: {e}")
        remove_status(video_id)

# Helper for other types (Artist/Playlist) - mostly same as before
def process_artist_task(aid):
    try:
        r = requests.get(f"{YTIFY_API_BASE}/artist/{aid}", timeout=10)
        if r.status_code==200:
            pid = r.json().get('playlistId')
            if pid: process_playlist_task(pid, artist_id=aid)
    except: pass

def process_playlist_task(pid, artist_id=None):
    try:
        r = requests.get(f"{YTIFY_API_BASE}/playlist/{pid}", timeout=10)
        if r.status_code==200:
            tracks = r.json().get('tracks', [])
            existing = get_all_video_ids()
            for t in tracks:
                vid = t.get('videoId')
                if vid and vid not in existing:
                    STATUS["queue"].append({"id": vid, "title": t.get('title')})
                    meta = {'title': t.get('title'), 'artist': t.get('artist') or t.get('author')}
                    if artist_id: meta['artist_id'] = artist_id
                    process_video_task(vid, meta)
    except: pass

def handle_submission(inp):
    if len(inp) == 11 and not inp.startswith("UC"): process_video_task(inp)
    elif inp.startswith("UC"): process_artist_task(inp)
    else: process_playlist_task(inp)
