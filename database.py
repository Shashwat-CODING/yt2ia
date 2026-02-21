import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
import re

# DB Config
DB_CONFIG = {
    'user': '2H65kCXGzjYP3va.root',
    'password': 'l0ztMMj9ZJJWhQtI',
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'port': 4000,
    'database': 'test',
    'database': 'test',
    'pool_name': 'main_pool',
    'pool_size': 5
}

main_pool = None

def init_pool():
    global main_pool
    try:
        main_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
        print("Main DB Connection Pool initialized.")
    except Error as e:
        print(f"Error initializing Main DB pool: {e}")

def get_connection():
    global main_pool
    if not main_pool:
        init_pool()
    try:
        connection = main_pool.get_connection()
        return connection
    except Error as e:
        print(f"Error connecting to MySQL Platform: {e}")
        return None

def init_db():
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # Store song_name (title - artists) and ia_url
            # video_id can be extracted from the URL filename
            create_table_query = """
            CREATE TABLE IF NOT EXISTS yt2ia_records (
                song_name VARCHAR(500) NOT NULL,
                ia_url TEXT NOT NULL,
                PRIMARY KEY (song_name(255))
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            print("Database initialized successfully.")
        except Error as e:
            print(f"Error initializing DB: {e}")
        finally:
            conn.close()

def extract_video_id_from_url(ia_url):
    """Extract video_id from IA URL like https://archive.org/download/YTMBACKUP/abc123.m4a"""
    try:
        # Get filename from URL
        filename = ia_url.split('/')[-1]
        # Remove extension
        video_id = filename.rsplit('.', 1)[0]
        return video_id
    except:
        return None

def save_entry(song_name, ia_url):
    """Save entry with song_name (title - artists) and ia_url"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            query = "INSERT INTO yt2ia_records (song_name, ia_url) VALUES (%s, %s) ON DUPLICATE KEY UPDATE ia_url=%s"
            cursor.execute(query, (song_name, ia_url, ia_url))
            conn.commit()
            print(f"Saved entry: {song_name}")
        except Error as e:
            print(f"Error saving to DB: {e}")
        finally:
            conn.close()

def get_all_video_ids():
    """Get all video IDs by extracting from URLs"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            query = "SELECT ia_url FROM yt2ia_records"
            cursor.execute(query)
            result = set()
            for row in cursor.fetchall():
                video_id = extract_video_id_from_url(row[0])
                if video_id:
                    result.add(video_id)
            print(f"DB Debug: Loaded {len(result)} IDs from {len(cursor.fetchall()) + len(result)} rows")
            return result
        except Error as e:
            print(f"Error fetching all IDs: {e}")
            return set()
        finally:
            conn.close()
    return set()

def get_all_entries():
    """Get all entries with song_name and ia_url"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            query = "SELECT song_name, ia_url FROM yt2ia_records"
            cursor.execute(query)
            result = []
            for row in cursor.fetchall():
                result.append({
                    "name": row[0],
                    "url": row[1]
                })
            return result
        except Error as e:
            print(f"Error fetching all entries: {e}")
            return []
        finally:
            conn.close()
    return []

def clear_all_entries():
    """Delete all entries from the database"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            query = "DELETE FROM yt2ia_records"
            cursor.execute(query)
            conn.commit()
            deleted_count = cursor.rowcount
            print(f"Cleared {deleted_count} entries from database")
            return deleted_count
        except Error as e:
            print(f"Error clearing database: {e}")
            return -1
        finally:
            conn.close()
    return -1