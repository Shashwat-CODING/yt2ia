import asyncio
import aiohttp
import requests
import os
import sys
import time
import urllib.parse
from database import save_entry, get_all_video_ids, init_db
from queue_db import add_to_queue, remove_from_queue
from internetarchive import upload
import re
import logging

# S3 Config
IA_ACCESS_KEY = "YXzY5OrREXLL6XBh"
IA_SECRET_KEY = "m2XnL3X7xCB1pNGE"
UPLOAD_DELAY = 0

# Status Tracking Structure
STATUS = {
    "active_jobs": {},
    "queue": [],
    "logs": [],
    "stats": {
        "processed": 0,
        "skipped": 0,
        "failed": 0
    }
}

VIDEO_ID_CACHE = set()

last_upload_time = 0

def initialize_cache():
    global VIDEO_ID_CACHE
    VIDEO_ID_CACHE = get_all_video_ids()
    logger.info(f"Cache initialized with {len(VIDEO_ID_CACHE)} IDs")

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

YTIFY_API_BASE = "https://ytify-backend.zeabur.app/api"
INVIDIOUS_INSTANCES = [
    "https://cursorpro-streamion.hf.space",
    "https://cursorpro-streamion-2.hf.space",
    "https://shashwatidr-we.hf.space",
    "https://veltrixcode-stream.hf.space"
]

# ---------- HELPER FUNCTIONS ----------

async def fetch_from_instance(session, video_id, instance_url):
    try:
        url = f"{instance_url}/api/v1/videos/{video_id}"
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                return instance_url, data
            else:
                try:
                    err_data = await response.text()
                    logger.warning(f"[{video_id}] Invidious instance {instance_url} returned {response.status}: {err_data}")
                except Exception:
                    logger.warning(f"[{video_id}] Invidious instance {instance_url} returned {response.status}")
    except Exception as e:
        logger.warning(f"[{video_id}] Invidious instance {instance_url} connection error: {e}")
    return None

current_inv_index = 0

async def fetch_details_sequential(video_id):
    global current_inv_index
    async with aiohttp.ClientSession() as session:
        num_instances = len(INVIDIOUS_INSTANCES)
        for i in range(num_instances):
            idx = (current_inv_index + i) % num_instances
            url = INVIDIOUS_INSTANCES[idx]
            result = await fetch_from_instance(session, video_id, url)
            if result:
                current_inv_index = idx  # Remember the working instance for the next video
                return result
    return None, None



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
    if not artist_string:
        return []
    separators = [', ', ' & ', ' and ', ' x ', ' X ', ' feat. ', ' feat ', ' ft. ', ' ft ', ' featuring ', ' Featuring ']
    artists = [artist_string]
    for sep in separators:
        new_ar = []
        for a in artists:
            new_ar.extend(a.split(sep))
        artists = new_ar
    return [a.strip() for a in artists if a.strip()]

def find_jiosaavn(title, artist):
    if not title or not artist:
        return None, None
    try:
        artist = parse_artists(artist)
        if not artist:
            return None, None
        q_title = urllib.parse.quote(title.strip())
        q_artist = urllib.parse.quote(','.join(artist))
        url = f"{YTIFY_API_BASE}/jiosaavn/search?title={q_title}&artist={q_artist}&debug=0"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            d = resp.json()
            if d and d.get("downloadUrl"):
                return d.get("downloadUrl"), {'type': 'audio/mp4'}
    except:
        pass
    return None, None

async def find_invidious(video_id, current_metadata=None):
    instance, data = await fetch_details_sequential(video_id)
    if not data:
        return None, None, None, None

    meta = {
        'title': data.get('title', 'Unknown'),
        'artist': data.get('author', 'Unknown')
    }
    if meta['artist'].endswith(" - Topic"):
        meta['artist'] = meta['artist'].replace(" - Topic", "")

    adaptive = data.get('adaptiveFormats', [])
    audio = [f for f in adaptive if f.get('type', '').startswith('audio')]
    if not audio:
        return None, None, meta, None
    
    audio.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
    best = audio[0]
    raw_url = best.get('url')
    
    if raw_url:
        try:
            parsed = urllib.parse.urlparse(raw_url)
            proxy_url = f"{instance}/videoplayback?{parsed.query}"
            # Return both proxy URL and direct URL as fallback
            return proxy_url, best, meta, raw_url
        except:
            return raw_url, best, meta, None
    
    return None, None, meta, None

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
                    enc = (raw)
                    return f"{enc}", best
    except:
        pass
    return None, None

def find_rapidapi(video_id):
    data = fetch_details_rapidapi(video_id)
    if not data:
        return None, None, None
    
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

# ---------- RATE-LIMITED UPLOAD ----------

def wait_for_rate_limit():
    """Ensure minimum delay between uploads"""
    global last_upload_time
    
    if last_upload_time:
        elapsed = time.time() - last_upload_time
        if elapsed < UPLOAD_DELAY:
            wait_time = UPLOAD_DELAY - elapsed
            logger.info(f"Rate limiting: waiting {wait_time:.1f}s before next upload")
            time.sleep(wait_time)
    
    last_upload_time = time.time()

def upload_to_ia_with_retry(identifier, input_files, metadata, max_retries=3):
    """Upload with exponential backoff retry logic. 
    `input_files` can be a single string filepath, or a dict of {filename: filepath} for bulk upload."""
    
    # Normalize input to always be a dict
    if isinstance(input_files, str):
        files_dict = {os.path.basename(input_files): input_files}
    else:
        files_dict = input_files
        
    for attempt in range(max_retries):
        try:
            # Wait for rate limit before checking/uploading
            wait_for_rate_limit()
            
            # We skip individual file check for bulk uploads here right now to optimize
            
            # Attempt upload with minimal queue load
            logger.info(f"Uploading {len(files_dict)} files to IA (attempt {attempt + 1}/{max_retries})...")
            
            r = upload(
                identifier,
                files=files_dict,
                metadata=metadata,
                access_key=IA_ACCESS_KEY,
                secret_key=IA_SECRET_KEY,
                queue_derive=0,  # Don't queue derive tasks
                verify=True,
                verbose=False,  # Reduce verbosity
                retries=1,
                retries_sleep=10
            )
            
            if r and len(r) > 0 and r[0].status_code in [200, 201]:
                # Return list of final IA URLs if single or dict
                if isinstance(input_files, str):
                    ia_url = f"https://archive.org/download/{identifier}/{list(files_dict.keys())[0]}"
                    logger.info(f"Upload successful: {ia_url}")
                    return True, ia_url
                else:
                    ia_urls = {filename: f"https://archive.org/download/{identifier}/{filename}" for filename in files_dict.keys()}
                    logger.info(f"Bulk Upload successful: {len(ia_urls)} files")
                    return True, ia_urls
            else:
                status = r[0].status_code if (r and len(r) > 0) else 'Unknown'
                logger.warning(f"Upload returned status: {status}")
                if attempt < max_retries - 1:
                    time.sleep(10 * (attempt + 1))
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Upload attempt {attempt + 1} failed: {error_msg}")
            
            # If rate limit error, wait much longer
            if "request rate" in error_msg.lower() or "total_tasks_queued" in error_msg.lower() or "queued" in error_msg.lower():
                wait_time = 60 * (1 ** attempt)  # 60s, 120s, 240s
                logger.info(f"Rate limit detected, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            elif attempt < max_retries - 1:
                time.sleep(15 * (attempt + 1))
    
    return False, None

# ---------- MAIN PROCESSING ----------

async def download_video_async(video_id, metadata_override=None):
    title = metadata_override.get('title', 'Unknown') if metadata_override else "Unknown"
    author = metadata_override.get('artist', 'Unknown') if metadata_override else "Unknown"

    # 1. Resolve Metadata if missing
    if title == "Unknown":
        set_status(video_id, "Resolving Metadata...")
        _, i_data = await fetch_details_sequential(video_id)
        if i_data:
            title = i_data.get('title', 'Unknown')
            author = i_data.get('author', 'Unknown')
        else:
            r_data = fetch_details_rapidapi(video_id)
            if r_data:
                title = r_data.get('title', 'Unknown')
                author = r_data.get('channelTitle', 'Unknown')
        
        if author.endswith(" - Topic"):
            author = author.replace(" - Topic", "")
    
    logger.info(f"[{video_id}] Metadata: {title} - {author}")

    # 2. Try Download Chain
    success_file = None
    invidious_direct_url = None  # Fallback direct URL from Invidious

    async def step_jio():
        return find_jiosaavn(title, author)

    async def step_inv():
        nonlocal invidious_direct_url
        u, f, _, direct = await find_invidious(video_id)
        invidious_direct_url = direct  # Store direct URL for fallback
        return u, f

    async def step_zeabur():
        return find_zeabur(video_id)

    async def step_rapid():
        u, f, _ = find_rapidapi(video_id)
        return u, f

    steps = [
        ("JioSaavn", step_jio),
        ("Invidious", step_inv),
        ("Zeabur", step_zeabur),
        ("RapidAPI", step_rapid)
    ]

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.youtube.com/"
    }

    async def try_download(video_id, name, url, fmt, headers, download_session):
        """Attempt to download audio from the given URL. Returns filepath on success, None on failure."""
        ext = "webm"
        if fmt:
            mime = fmt.get('type', '') or fmt.get('mimeType', '')
            if 'mp4' in mime or 'm4a' in mime:
                ext = "m4a"
        
        fname = f"{video_id}.{ext}"
        fpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), fname)
        
        logger.info(f"[{video_id}] Downloading from {name}: {url[:100]}...")
        
        try:
            async with download_session.get(url, headers=headers, timeout=30, max_redirects=10) as r:
                if r.status != 200:
                    logger.warning(f"[{video_id}] {name} download failed: {r.status}")
                    return None
                
                total = int(r.headers.get('content-length', 0))
                dl = 0
                
                with open(fpath, 'wb') as f:
                    async for chunk in r.content.iter_chunked(1024*1024):
                        if chunk:
                            f.write(chunk)
                            dl += len(chunk)
                            if total:
                                pct = int(dl/total * 100)
                                set_status(video_id, f"Downloading ({name}): {pct}%", progress=pct, state="downloading")
            
            # Verify file is not empty
            if os.path.exists(fpath) and os.path.getsize(fpath) > 0:
                logger.info(f"[{video_id}] Download success from {name}")
                return fpath
            else:
                logger.warning(f"[{video_id}] {name} downloaded empty file")
                if os.path.exists(fpath):
                    os.remove(fpath)
                return None
                
        except RecursionError:
            logger.warning(f"[{video_id}] {name} hit recursion depth limit (redirect loop or SSL issue)")
            if os.path.exists(fpath):
                os.remove(fpath)
            return None
        except aiohttp.TooManyRedirects:
            logger.warning(f"[{video_id}] {name} too many redirects")
            if os.path.exists(fpath):
                os.remove(fpath)
            return None
        except Exception as e:
            logger.warning(f"[{video_id}] {name} download exception: {e}")
            if os.path.exists(fpath):
                os.remove(fpath)
            return None

    async with aiohttp.ClientSession() as download_session:
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

            if url:
                stream_found = True

            # Try download with primary URL
            result = await try_download(video_id, name, url, fmt, headers, download_session)
            
            # If Invidious proxy URL failed, try the direct YouTube URL as fallback
            if result is None and name == "Invidious" and invidious_direct_url:
                logger.info(f"[{video_id}] Invidious proxy failed, trying direct URL fallback")
                result = await try_download(video_id, f"{name}-Direct", invidious_direct_url, fmt, headers, download_session)
            
            if result:
                success_file = result
                break

    if not success_file:
        set_status(video_id, "ERROR: All sources failed")
        STATUS["stats"]["failed"] += 1
        return None, title, author, stream_found

    return success_file, title, author, stream_found

async def process_video_async(video_id, metadata_override=None):
    success_file, title, author, _ = await download_video_async(video_id, metadata_override)
    if not success_file:
        remove_status(video_id)
        return

    # 3. Upload with rate limiting
    set_status(video_id, "Uploading to Internet Archive...")
    
    try:
        aid = metadata_override.get('artist_id') if metadata_override else None
        identifier = f"yt2ia-{aid}" if aid else "YTMBACKUP"
        
        md = {
            'title': title,
            'creator': author,
            'mediatype': 'audio',
            'collection': 'opensource_audio',
            'description': f'Audio from YouTube video {video_id}',
            'subject': ['youtube', 'audio', 'music']
        }
        
        success, ia_url = upload_to_ia_with_retry(identifier, success_file, md)
        
        if success:
            save_entry(f"{title} - {author}", ia_url)
            logger.info(f"[{video_id}] Added song to Main DB: {title} - {author}")
            
            VIDEO_ID_CACHE.add(video_id)
            logger.info(f"[{video_id}] Added ID to Cache")
            
            STATUS["stats"]["processed"] += 1
            logger.info(f"[{video_id}] Upload Complete: {ia_url}")
            set_status(video_id, "Complete!")
        else:
            STATUS["stats"]["failed"] += 1
            logger.error(f"[{video_id}] Upload failed after all retries")
            set_status(video_id, "ERROR: Upload failed")
            
    except Exception as e:
        STATUS["stats"]["failed"] += 1
        logger.error(f"[{video_id}] Upload error: {e}")
        set_status(video_id, f"ERROR: {str(e)}")
    finally:
        if os.path.exists(success_file):
            os.remove(success_file)
        remove_status(video_id)

def process_video_task(video_id, metadata_override=None):
    if video_id in STATUS["active_jobs"]:
        return
    
    set_status(video_id, "Starting...")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_video_async(video_id, metadata_override))
        loop.close()
    except Exception as e:
        logger.error(f"Task error: {e}")
        remove_status(video_id)

async def batch_process_videos(videos_list, artist_id=None):
    """
    Process a list of videos concurrently in chunks.
    Uploads them to Internet Archive in bulk.
    """
    chunk_size = 10
    
    for i in range(0, len(videos_list), chunk_size):
        chunk = videos_list[i:i+chunk_size]
        
        # Initialize batch matrix for this chunk
        vids = [item['vid'] for item in chunk]
        STATUS["batch_matrix"] = {
            "vids": vids,
            "stream": {vid: False for vid in vids},
            "download": {vid: False for vid in vids},
            "upload": {vid: False for vid in vids},
            "url": {vid: '-' for vid in vids}
        }
        
        # Initialize status for all videos in chunk
        for item in chunk:
            set_status(item['vid'], "Starting...")

        logger.info(f"Starting batch download for {len(chunk)} videos...")
        
        # Concurrently download videos
        download_tasks = [download_video_async(item['vid'], item['meta']) for item in chunk]
        results = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Process results and prepare files for bulk upload
        files_to_upload = {}
        success_items = []
        
        for k, result in enumerate(results):
            item_data = chunk[k]
            vid = item_data['vid']
            meta = item_data['meta']
            
            if isinstance(result, tuple) and len(result) == 4:
                filepath, title, author, stream_found = result
                STATUS["batch_matrix"]["stream"][vid] = stream_found
                if filepath:
                    STATUS["batch_matrix"]["download"][vid] = True
                    filename = os.path.basename(filepath)
                    files_to_upload[filename] = filepath
                    
                    # Update metadata with final retrieved ones
                    meta['final_title'] = title
                meta['final_author'] = author
                
                success_items.append({
                    "vid": vid,
                    "filepath": filepath,
                    "filename": filename,
                    "meta": meta
                })
                set_status(vid, "Uploading to Internet Archive in batch...")
            else:
                # Failed video
                logger.error(f"[{vid}] Failed to download in batch.")
                remove_status(vid)
                STATUS["queue"] = [q_item for q_item in STATUS["queue"] if q_item["id"] != vid]

        if not files_to_upload:
            logger.warning("No files downloaded successfully in this batch.")
        else:
            # Bulk upload to IA
            logger.info(f"Bulk uploading {len(files_to_upload)} files...")
            aid = artist_id if artist_id else None
            
            # If all items are from the same artist, use their aid. Otherwise, a generic backup name.
            if aid is None and len(success_items) > 0 and 'artist_id' in success_items[0]['meta']:
                 aid = success_items[0]['meta']['artist_id']
    
            identifier = f"yt2ia-{aid}" if aid else "YTMBACKUP"
            
            md = {
                'title': "Batch Upload" if aid else "Mixed Uploads",
                'creator': success_items[0]['meta']['final_author'] if aid else "yt2ia",
                'mediatype': 'audio',
                'collection': 'opensource_audio',
                'description': f'Batch audio uploads from YouTube',
                'subject': ['youtube', 'audio', 'music']
            }
            
            success, ia_urls_dict = upload_to_ia_with_retry(identifier, files_to_upload, md)

        
        if success and ia_urls_dict:
            for item in success_items:
                vid = item['vid']
                filepath = item['filepath']
                filename = item['filename']
                meta = item['meta']
                
                ia_url = ia_urls_dict.get(filename)
                title = meta.get('final_title', 'Unknown')
                author = meta.get('final_author', 'Unknown')
                
                if ia_url:
                    STATUS["batch_matrix"]["upload"][vid] = True
                    STATUS["batch_matrix"]["url"][vid] = ia_url
                    save_entry(f"{title} - {author}", ia_url)
                    logger.info(f"[{vid}] Added song to Main DB: {title} - {author}")
                    
                    VIDEO_ID_CACHE.add(vid)
                    STATUS["stats"]["processed"] += 1
                    STATUS["stats"]["failed"] -= 1 # adjust back if it previously failed (not needed here usually but fine)
                    
                    logger.info(f"[{vid}] Upload Complete: {ia_url}")
                    set_status(vid, "Complete!")
                else:
                    logger.warning(f"[{vid}] URL not returned from bulk upload")
                    set_status(vid, "ERROR: URL mapping missing in bulk upload")
                
                # Cleanup and remove status
                if os.path.exists(filepath):
                    os.remove(filepath)
                remove_status(vid)
                STATUS["queue"] = [q_item for q_item in STATUS["queue"] if q_item["id"] != vid]
        elif files_to_upload:
            # Entire batch upload failed
            logger.error(f"Bulk upload failed for {len(files_to_upload)} files.")
            for item in success_items:
                vid = item['vid']
                filepath = item['filepath']
                set_status(vid, "ERROR: Batch Upload failed")
                if os.path.exists(filepath):
                    os.remove(filepath)
                remove_status(vid)
                STATUS["queue"] = [q_item for q_item in STATUS["queue"] if q_item["id"] != vid]

        # Log ASCII matrix to terminal at the end of the batch
        b = STATUS["batch_matrix"]
        if b and b.get("vids"):
            def get_ico(val):
                return '✅' if val is True else ('❌' if val is False else '⏳')
            
            # Print table
            logger.info("\n" + "="*80)
            logger.info("BATCH COMPLETION MATRIX")
            logger.info("="*80)
            
            vids = b["vids"]
            col_width = 15
            header_row = "".ljust(15) + "".join([str(v)[:col_width].ljust(col_width) for v in vids])
            logger.info(header_row)
            logger.info("-" * len(header_row))
            
            stream_row = "Stream".ljust(15) + "".join([get_ico(b["stream"].get(v)).ljust(col_width - 1) + " " for v in vids])
            logger.info(stream_row)
            
            dl_row = "Download".ljust(15) + "".join([get_ico(b["download"].get(v)).ljust(col_width - 1) + " " for v in vids])
            logger.info(dl_row)
            
            ul_row = "Upload".ljust(15) + "".join([get_ico(b["upload"].get(v)).ljust(col_width - 1) + " " for v in vids])
            logger.info(ul_row)
            
            logger.info("-" * len(header_row))
            for v in vids:
                url = b["url"].get(v, "-")
                if url != "-":
                    logger.info(f"{v.ljust(15)} {url}")
            logger.info("="*80 + "\n")



def process_artist_task(aid):
    try:
        r = requests.get(f"{YTIFY_API_BASE}/artist/{aid}", timeout=10)
        if r.status_code==200:
            pid = r.json().get('playlistId')
            if pid:
                process_playlist_task(pid, artist_id=aid)
    except:
        pass

def process_playlist_task(pid, artist_id=None):
    try:
        r = requests.get(f"{YTIFY_API_BASE}/playlist/{pid}", timeout=10)
        if r.status_code==200:
            tracks = r.json().get('tracks', [])
            existing = VIDEO_ID_CACHE
            logger.info(f"Deduplication: Found {len(existing)} existing video IDs in Cache")
            
            # Filter tracks we actually need to process
            to_process = []
            for t in tracks:
                vid = t.get('videoId')
                if vid and vid not in existing:
                    STATUS["queue"].append({"id": vid, "title": t.get('title')})
                    meta = {'title': t.get('title'), 'artist': t.get('artist') or t.get('author')}
                    if artist_id:
                        meta['artist_id'] = artist_id
                    
                    to_process.append({"vid": vid, "meta": meta})
                    
            if to_process:
                logger.info(f"Processing {len(to_process)} playlist tracks in bulk...")
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(batch_process_videos(to_process, artist_id))
                    loop.close()
                except Exception as e:
                    logger.error(f"Batch playlist processing error: {e}")
                    
    except Exception as e:
        logger.error(f"Playlist task error: {e}")
        pass

def handle_submission(inp):
    if len(inp) == 11 and not inp.startswith("UC"):
        process_video_task(inp)
    elif inp.startswith("UC"):
        process_artist_task(inp)
    else:
        process_playlist_task(inp)
