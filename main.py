from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uuid
import os
import threading
import time

from database import init_db, get_all_entries, clear_all_entries
from worker import handle_submission, STATUS
from queue_db import init_queue_db, add_to_queue, get_next_from_queue, remove_from_queue, get_all_queue, get_queue_count, clear_queue

app = FastAPI()

# Track state
last_activity_time = time.time()
is_processing = False
current_processing_id = None
processing_lock = threading.Lock()
current_queue_index = 0
pause_until = 0

def update_activity():
    global last_activity_time
    last_activity_time = time.time()

# Start processing immediately (used when items added while idle)
def trigger_processing():
    global is_processing
    if not is_processing:
        thread = threading.Thread(target=process_one_item, daemon=True)
        thread.start()

def process_one_item():
    global is_processing, current_processing_id, current_queue_index
    
    with processing_lock:
        if is_processing:
            return
        is_processing = True
    
    try:
        # Get all items to process in circle
        items = get_all_queue()
        if items:
            # Ensure index is within bounds (circular)
            current_queue_index = current_queue_index % len(items)
            next_id = items[current_queue_index]
            
            current_processing_id = next_id
            print(f"Processing: {next_id} (Index: {current_queue_index}/{len(items)})")
            
            # Increment for next time immediately
            current_queue_index += 1
            
            try:
                # Don't remove from queue!
                handle_submission(next_id)
                print(f"Completed: {next_id}")
            except Exception as e:
                print(f"Error processing {next_id}: {e}")
                
            # Update activity time so we wait 1 min before next item
            update_activity()
        else:
             print("Queue empty")

    finally:
        current_processing_id = None
        is_processing = False

# Database API Endpoints
@app.get("/api/data")
async def get_all_data():
    entries = get_all_entries()
    return {"count": len(entries), "data": entries}

@app.get("/api/cleardb")
async def clear_database():
    deleted_count = clear_all_entries()
    return {"message": "Database cleared", "deleted_count": deleted_count}

@app.get("/api/clrq")
async def clear_queue_endpoint():
    deleted_count = clear_queue()
    return {"message": "Queue cleared", "deleted_count": deleted_count}

@app.get("/api/queue")
async def get_queue():
    items = get_all_queue()
    return {"count": len(items), "items": items}

@app.get("/api/start")
async def start_processing():
    """Manually trigger processing for very first ID"""
    global current_queue_index, pause_until
    if not is_processing:
        current_queue_index = 0  # Reset to start
        pause_until = 0 # Force resume
        trigger_processing()
        return {"message": "Processing started from beginning"}
    return {"message": "Already processing"}

@app.get("/api/pause")
async def pause_processing(duration: int = 3600):
    global pause_until
    pause_until = time.time() + duration
    return {"message": f"Paused for {duration} seconds", "paused_until": pause_until}

@app.get("/api/resume")
async def resume_processing():
    global pause_until
    pause_until = 0
    update_activity() # Reset activity timer
    trigger_processing()
    return {"message": "Resumed processing"}

# Search API
import requests as http_requests

@app.get("/api/search")
async def search_items(q: str, filter: str = "artists"):
    try:
        url = f"https://ytify-backend.vercel.app/api/search?q={q}&filter={filter}"
        response = http_requests.get(url, timeout=15)
        if response.status_code == 200:
            return response.json()
        return {"error": "Search failed"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/meta")
async def get_meta(id: str):
    try:
        url = f"https://ytify-backend.vercel.app/api/artist/{id}"
        response = http_requests.get(url, timeout=15)
        if response.status_code == 200:
            return response.json()
        return {"error": "Meta fetch failed"}
    except Exception as e:
        return {"error": str(e)}

# Submit artist
class SubmitArtistRequest(BaseModel):
    browse_id: str
    name: str = ""

@app.post("/submitartist")
async def submit_artist(req: SubmitArtistRequest):
    update_activity()
    if add_to_queue(req.browse_id):
        # If system was idle (1+ min) and not processing, start immediately
        idle_time = time.time() - last_activity_time
        if idle_time >= 60 and not is_processing:
            trigger_processing()
        return {"message": "Added to queue", "id": req.browse_id}
    else:
        return {"message": "Already added", "id": req.browse_id, "duplicate": True}

# Submit via manual input
class SubmitRequest(BaseModel):
    input_id: str

@app.post("/submit")
async def submit_job(req: SubmitRequest):
    update_activity()
    if add_to_queue(req.input_id):
        # If system was idle and not processing, start immediately
        idle_time = time.time() - last_activity_time
        if idle_time >= 60 and not is_processing:
            trigger_processing()
        return {"message": "Added to queue", "id": req.input_id}
    else:
        return {"message": "Already added", "id": req.input_id, "duplicate": True}

# Setup Templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.on_event("startup")
def on_startup():
    init_db()
    init_queue_db()
    start_queue_processor()

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/status")
async def get_status():
    global last_activity_time, is_processing, current_processing_id
    idle_seconds = int(time.time() - last_activity_time)
    queue_count = get_queue_count()
    return {
        "idle_seconds": idle_seconds,
        "queue_count": queue_count,
        "is_processing": is_processing,
        "current_id": current_processing_id,
        "active_jobs": STATUS.get("active_jobs", {}),
        "stats": STATUS.get("stats", {}),
        "logs": STATUS.get("logs", [])[-10:],  # Last 10 logs
        "current_index": current_queue_index,
        "paused_until": pause_until
    }

# Queue Processor - 1 min idle time
def queue_processor():
    global is_processing, last_activity_time
    print("Queue processor started")
    
    while True:
        try:
            time.sleep(5)  # Check every 5 seconds
            
            idle_time = time.time() - last_activity_time
            
            # Check pause
            if time.time() < pause_until:
                continue

            # Process if idle for 1+ min and not already processing
            if idle_time >= 60 and not is_processing:
                if get_queue_count() > 0:
                    process_one_item()
                    
        except Exception as e:
            print(f"Queue processor error: {e}")

def start_queue_processor():
    thread = threading.Thread(target=queue_processor, daemon=True)
    thread.start()
    print("Queue processor thread started")

# SSE Stream
import asyncio
import json
from fastapi.responses import StreamingResponse

async def event_generator():
    while True:
        data = {
            "idle_seconds": int(time.time() - last_activity_time),
            "queue_count": get_queue_count(),
            "is_processing": is_processing,
            "current_id": current_processing_id,
            "active_jobs": STATUS.get("active_jobs", {}),
            "stats": STATUS.get("stats", {}),
            "active_jobs": STATUS.get("active_jobs", {}),
            "stats": STATUS.get("stats", {}),
            "logs": STATUS.get("logs", [])[-10:],
            "current_index": current_queue_index,
            "paused_until": pause_until
        }
        yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(1)

@app.get("/stream")
async def run_stream():
    return StreamingResponse(event_generator(), media_type="text/event-stream")
