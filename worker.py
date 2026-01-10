import asyncio
import aiohttp
import requests
import os
import time
import urllib.parse
from database import save_entry, get_all_video_ids, init_db
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

# Custom Handler to write to STATUS["logs"]
class ListHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        STATUS["logs"].append(msg)
        if len(STATUS["logs"]) > 100:
            STATUS["logs"].pop(0)

# Setup Logger
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
  "https://vid.selbsthilfegruppe.wtf",
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
  "https://sklempin.ddns.net:9001",
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
  "https://abura53-test.com",
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

YTIFY_API_BASE = "https://ytify-backend.vercel.app/api"

async def fetch_from_instance(session, video_id, instance_url):
    try:
        url = f"{instance_url}/api/v1/videos/{video_id}"
        async with session.get(url, timeout=100) as response:
            if response.status == 200:
                data = await response.json()
                return instance_url, data
    except Exception as e:
        logger.debug(f"Instance {instance_url} failed: {e}")
    return None

async def fetch_details_parallel(video_id):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch_from_instance(session, video_id, url)) 
                for url in INVIDIOUS_INSTANCES]
        
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                if result:
                    for t in tasks:
                        t.cancel()
                    return result
            except Exception:
                pass
                
    return None, None

async def fetch_from_piped(session, video_id, instance_url):
    try:
        url = f"{instance_url}/streams/{video_id}"
        async with session.get(url, timeout=100) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('audioStreams') or data.get('videoStreams'):
                    return data
    except Exception as e:
        logger.debug(f"Piped instance {instance_url} failed: {e}")
    return None

async def fetch_piped_parallel(video_id):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_from_piped(session, video_id, inst) for inst in PIPED_INSTANCES]
        for future in asyncio.as_completed(tasks):
            result = await future
            if result:
                return result
        return None

def parse_artists(artist_string):
    """Parse artist string and extract individual artist names"""
    if not artist_string:
        return []
    
    # Common separators for multiple artists
    separators = [', ', ' & ', ' and ', ' x ', ' X ', ' feat. ', ' feat ', ' ft. ', ' ft ', ' featuring ', ' Featuring ']
    
    artists = [artist_string]
    for sep in separators:
        new_artists = []
        for a in artists:
            new_artists.extend(a.split(sep))
        artists = new_artists
    
    # Clean up each artist name
    artists = [a.strip() for a in artists if a.strip()]
    return artists

def fetch_jiosaavn_url(title, artist):
    try:
        if not title or not artist:
            return None
        
        # Use full title - don't strip parentheses content as it may be important
        # (e.g., "Slowed Version", "Remix", etc.)
        clean_title = title.strip()
        
        # Parse multiple artists and join with comma
        artists_list = parse_artists(artist)
        if not artists_list:
            return None
        
        # Join artists with comma for API
        artists_str = ','.join(artists_list)
        
        query_title = urllib.parse.quote(clean_title)
        query_artist = urllib.parse.quote(artists_str)
        
        url = f"{YTIFY_API_BASE}/jiosaavn/search?title={query_title}&artist={query_artist}&debug=0"
        logger.info(f"Searching JioSaavn: {clean_title} - {artists_str}")
        logger.info(f"JioSaavn API Request: {url}")
        
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict) and data.get("downloadUrl"):
                d_url = data.get("downloadUrl")
                logger.info(f"Found JioSaavn URL")
                return d_url
    except Exception as e:
        logger.warning(f"JioSaavn Search Failed: {e}")
    return None

def fetch_details_rapidapi(video_id):
    url = "https://yt-api.p.rapidapi.com/dl"
    querystring = {"id": video_id, "cgeo": "US"}
    headers = {
        "x-rapidapi-key": "eee55a9833msh8f2dbd8e2b7970bp194fefjsne09ddc646e78",
        "x-rapidapi-host": "yt-api.p.rapidapi.com"
    }
    try:
        logger.info(f"Attempting RapidAPI for {video_id}")
        response = requests.get(url, headers=headers, params=querystring, timeout=15)
        if response.status_code == 200:
            data = response.json()
            data['author'] = data.get('channelTitle', 'Unknown')
            
            if 'adaptiveFormats' in data:
                for f in data['adaptiveFormats']:
                    if 'mimeType' in f:
                        f['type'] = f['mimeType']
                        
            return data
    except Exception as e:
        logger.error(f"RapidAPI error: {e}")
    return None

def sanitize_name(name):
    # Remove invalid filename characters, replace spaces with underscores
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = name.replace(" ", "_")
    name = re.sub(r'_+', '_', name)  # Remove multiple underscores
    name = name.strip('_')  # Remove leading/trailing underscores
    return name[:200]  # Limit length

def process_video_task(video_id, metadata_override=None):
    if video_id in STATUS["active_jobs"]:
        return
        
    set_status(video_id, "Queued / Starting")
    logger.info(f"Starting processing for video: {video_id}")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_video_async(video_id, metadata_override))
        loop.close()
    except Exception as e:
        set_status(video_id, f"Error: {str(e)}")
        logger.error(f"Error processing {video_id}: {e}", exc_info=True)

async def process_video_async(video_id, metadata_override=None):
    instance = None
    data = None
    title = "Unknown"
    author = "Unknown"
    
    if metadata_override:
        title = metadata_override.get('title', 'Unknown')
        author = metadata_override.get('artist', 'Unknown')
        logger.info(f"[{video_id}] Metadata provided: {title} - {author}")
    else:
        set_status(video_id, "Resolving Metadata...")
        instance, data = await fetch_details_parallel(video_id)
        
        if not data:
            set_status(video_id, "Trying RapidAPI for metadata...")
            data = fetch_details_rapidapi(video_id)
            instance = "RAPIDAPI"
            
        if data:
            title = data.get('title', 'Unknown')
            author = data.get('author', 'Unknown')
            set_status(video_id, f"Metadata: {title}")
        else:
            set_status(video_id, "ERROR: No metadata found")
            logger.error(f"[{video_id}] No metadata available")
            remove_status(video_id)
            return

    if author and " - Topic" in author:
        author = author.replace(" - Topic", "")

    final_stream_url = None
    best_audio = {}
    source_used = None

    set_status(video_id, "Finding stream source...")

    # Priority 1: JioSaavn (best quality for Indian music)
    set_status(video_id, "Checking JioSaavn...")
    jio_url = None
    if title and author and author != "Unknown":
        jio_url = fetch_jiosaavn_url(title, author)
    
    if jio_url:
        final_stream_url = jio_url
        best_audio['type'] = 'audio/mp4'
        source_used = "JioSaavn"
        logger.info(f"[{video_id}] Source: JioSaavn")
    
    # Priority 2: Invidious (with proxy)
    if not final_stream_url:
        set_status(video_id, "Checking Invidious...")
        if not data:
            instance, data = await fetch_details_parallel(video_id)
        
        if data and data.get('adaptiveFormats'):
            adaptive_formats = data.get('adaptiveFormats', [])
            audio_formats = [f for f in adaptive_formats if f.get('type', '').startswith('audio')]
            if audio_formats:
                audio_formats.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
                best_audio = audio_formats[0]
                raw_url = best_audio.get('url')
                
                # Proxy the URL through the Invidious instance
                if raw_url and instance:
                    # Use the /latest_version endpoint to get proxied URL
                    proxy_url = f"{instance}/latest_version?id={video_id}&itag={best_audio.get('itag', '')}&local=true"
                    final_stream_url = proxy_url
                    source_used = "Invidious (Proxied)"
                    logger.info(f"[{video_id}] Source: Invidious (Proxied via {instance})")

    # Priority 3: Piped
    if not final_stream_url:
        set_status(video_id, "Checking Piped...")
        piped_data = await fetch_piped_parallel(video_id)
        if piped_data:
            p_streams = piped_data.get('audioStreams', [])
            if p_streams:
                p_streams.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
                best_stream = p_streams[0]
                final_stream_url = best_stream.get('url')
                if final_stream_url:
                    source_used = "Piped"
                    logger.info(f"[{video_id}] Source: Piped")
                    mime = best_stream.get('mimeType', '')
                    if 'mp4' in mime:
                        best_audio['type'] = 'audio/mp4'
                    elif 'webm' in mime or 'opus' in best_stream.get('codec', ''):
                        best_audio['type'] = 'audio/webm; codecs="opus"'
                    else:
                        best_audio['type'] = mime

    # Priority 4: RapidAPI (fallback)
    if not final_stream_url:
        set_status(video_id, "Checking RapidAPI...")
        rapid_data = fetch_details_rapidapi(video_id)
        if rapid_data:
            r_adaptive = rapid_data.get('adaptiveFormats', [])
            r_audio = [f for f in r_adaptive if f.get('type', '').startswith('audio')]
            if r_audio:
                r_audio.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
                final_stream_url = r_audio[0].get('url')
                best_audio = r_audio[0]
                source_used = "RapidAPI"
                logger.info(f"[{video_id}] Source: RapidAPI")

    if source_used:
        set_status(video_id, f"Source: {source_used}")

    if not final_stream_url:
        set_status(video_id, "ERROR: No stream source available")
        logger.error(f"[{video_id}] All sources exhausted")
        remove_status(video_id)
        return

    # Use video_id as filename to ensure uniqueness
    extension = "webm" if "opus" in best_audio.get('type', '') else "m4a"
    filename = f"{video_id}.{extension}"
    
    # Use absolute path for file operations
    download_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(download_dir, filename)
    
    logger.info(f"[{video_id}] Filename: {filename} (Title: {title} - {author})")
    set_status(video_id, f"Downloading...")
    
    try:
        # Download audio
        response = requests.get(final_stream_url, stream=True, timeout=180)
        
        # Handle 403 with RapidAPI fallback
        if response.status_code == 403:
            logger.warning(f"[{video_id}] 403 error, trying RapidAPI...")
            rapid_data = fetch_details_rapidapi(video_id)
            if rapid_data:
                r_adaptive = rapid_data.get('adaptiveFormats', [])
                r_audio = [f for f in r_adaptive if f.get('type', '').startswith('audio')]
                if r_audio:
                    r_audio.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
                    final_stream_url = r_audio[0].get('url')
                    response = requests.get(final_stream_url, stream=True, timeout=180)
        
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = int((downloaded / total_size) * 100)
                        set_status(video_id, f"Downloading: {percent}%", progress=percent, state="downloading")
            
            logger.info(f"[{video_id}] Download complete")
            set_status(video_id, "Uploading to Archive.org...", progress=100, state="processing")
            
            identifier = "YTMBACKUP"
            md = {
                'title': title,
                'creator': author,
                'mediatype': 'audio',
                'collection': 'opensource_audio'
            }
            
            logger.info(f"[{video_id}] Uploading to IA: {identifier}")
            
            # Retry logic for upload (handle connection refused/busy)
            max_retries = 10
            base_delay = 5  # Start with 5s delay
            
            for attempt in range(max_retries):
                try:
                    r_ia = upload(
                        identifier, 
                        files={filename: filepath},  # key is remote name, value is local path
                        metadata=md, 
                        access_key=IA_ACCESS_KEY,
                        secret_key=IA_SECRET_KEY,
                        verbose=True
                    )
                    
                    if r_ia and len(r_ia) > 0 and r_ia[0].status_code == 200:
                        logger.info(f"[{video_id}] Upload success")
                        ia_download_url = f"https://archive.org/download/{identifier}/{filename}"
                        # Save with song name format: "title - artist1, artist2"
                        song_name = f"{title} - {author}"
                        save_entry(song_name, ia_download_url)
                        STATUS["stats"]["processed"] += 1
                        remove_status(video_id)
                        break
                    else:
                        raise Exception(f"Upload returned status {r_ia[0].status_code if r_ia else 'unknown'}")
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = base_delay * (attempt + 1) + (time.time() % 5) # Add jitter
                        logger.warning(f"[{video_id}] Upload attempt {attempt+1} failed ({str(e)}). Retrying in {wait:.1f}s...")
                        set_status(video_id, f"Upload retry {attempt+1}/{max_retries}...", state="processing")
                        time.sleep(wait)
                    else:
                        set_status(video_id, "ERROR: Upload failed after retries")
                        logger.error(f"[{video_id}] Upload failed: {e}")
                        remove_status(video_id)
                        # Don't save to DB on failure
                # Don't save to DB on failure
                
            if os.path.exists(filepath):
                os.remove(filepath)
    except Exception as e:
        set_status(video_id, f"ERROR: {str(e)}")
        logger.error(f"[{video_id}] Processing error: {e}", exc_info=True)
        remove_status(video_id)
        if os.path.exists(filepath):
            os.remove(filepath)

def process_artist_task(artist_id):
    try:
        logger.info(f"Fetching Artist: {artist_id}")
        url = f"{YTIFY_API_BASE}/artist/{artist_id}"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch artist {artist_id}: {resp.status_code}")
            return
        
        data = resp.json()
        playlist_id = data.get('playlistId')
        
        if playlist_id:
            logger.info(f"Artist {artist_id} -> Playlist {playlist_id}")
            process_playlist_task(playlist_id)
        else:
            logger.warning(f"No playlist for artist {artist_id}")

    except Exception as e:
        logger.error(f"Error artist {artist_id}: {e}", exc_info=True)

def process_playlist_task(playlist_id):
    try:
        logger.info(f"Fetching Playlist: {playlist_id}")
        url = f"{YTIFY_API_BASE}/playlist/{playlist_id}"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch playlist {playlist_id}: {resp.status_code}")
            return
        
        data = resp.json()
        tracks = data.get('tracks', [])
        
        logger.info(f"Found {len(tracks)} tracks in {playlist_id}")
        
        existing_ids = get_all_video_ids()
        logger.info(f"Loaded {len(existing_ids)} existing IDs from DB")
        
        valid_tracks = []
        for track in tracks:
            vid = track.get('videoId')
            if vid and vid not in existing_ids:
                artist = track.get('artist') or track.get('author')
                if not artist:
                    logger.info(f"Skipping {vid}: No artist metadata")
                    STATUS["stats"]["skipped"] += 1
                    continue
                    
                valid_tracks.append(track)
            elif vid:
                logger.info(f"Skipping {vid}: Already archived")
                STATUS["stats"]["skipped"] += 1

        logger.info(f"Queueing {len(valid_tracks)} valid tracks")
        
        for t in valid_tracks:
            STATUS["queue"].append({
                "id": t.get('videoId'),
                "title": t.get('title', 'Unknown')
            })

        for track in valid_tracks:
            vid = track.get('videoId')
            STATUS["queue"] = [q for q in STATUS["queue"] if q["id"] != vid]
            
            meta = {
                'title': track.get('title'),
                'artist': track.get('artist') or track.get('author') 
            }
            process_video_task(vid, metadata_override=meta)
                
    except Exception as e:
        logger.error(f"Error playlist {playlist_id}: {e}", exc_info=True)

def handle_submission(input_id):
    if len(input_id) == 11:
        logger.warning(f"Direct video ID not supported: {input_id}")
        return
        
    elif input_id.startswith("UC"):
        process_artist_task(input_id)
    elif input_id.startswith("PL") or input_id.startswith("OLAK") or input_id.startswith("VLR"):
        process_playlist_task(input_id)
    else:
        if len(input_id) > 11:
            process_artist_task(input_id)
        else:
            logger.warning(f"Unknown ID format: {input_id}")
