import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling

# TiDB Connection Config
DB_CONFIG = {
    'user': '3vCmuofaMqYGzHG.root',
    'password': '0DH9los7ze1bkBKk',
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'port': 4000,
    'database': 'test',
    'pool_name': 'queue_pool',
    'pool_size': 5
}

queue_pool = None

def init_pool():
    global queue_pool
    try:
        queue_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
        print("Queue DB Connection Pool initialized.")
    except Error as e:
        print(f"Error initializing Queue DB pool: {e}")

def get_connection():
    global queue_pool
    if not queue_pool:
        init_pool()
    try:
        connection = queue_pool.get_connection()
        return connection
    except Error as e:
        print(f"Error getting connection from pool: {e}")
        return None

def init_queue_db():
    """Initialize simple queue table - only stores IDs"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # Super simple - just IDs
            create_table_query = """
            CREATE TABLE IF NOT EXISTS queue (
                id VARCHAR(255) PRIMARY KEY
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            print("Queue table initialized.")
        except Error as e:
            print(f"Error initializing queue: {e}")
        finally:
            conn.close()

def add_to_queue(submission_id):
    """Add ID to queue (ignore if exists)"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            query = "INSERT IGNORE INTO queue (id) VALUES (%s)"
            # Execute once
            cursor.execute(query, (submission_id,))
            conn.commit()
            if cursor.rowcount > 0:
                print(f"Queued: {submission_id}")
                return True
            else:
                print(f"Duplicate skipped: {submission_id}")
                return False
        except Error as e:
            print(f"Error adding to queue: {e}")
            return False
        finally:
            conn.close()
    return False

def get_next_from_queue():
    """Get one ID from queue (FIFO style)"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM queue LIMIT 1")
            row = cursor.fetchone()
            if row:
                return row[0]
            return None
        except Error as e:
            print(f"Error getting from queue: {e}")
            return None
        finally:
            conn.close()
    return None

def remove_from_queue(submission_id):
    """Remove ID from queue after processing"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM queue WHERE id = %s", (submission_id,))
            conn.commit()
            print(f"Removed from queue: {submission_id}")
        except Error as e:
            print(f"Error removing from queue: {e}")
        finally:
            conn.close()

def get_queue_count():
    """Get count of items in queue"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM queue")
            row = cursor.fetchone()
            return row[0] if row else 0
        except Error as e:
            print(f"Error counting queue: {e}")
            return 0
        finally:
            conn.close()
    return 0

def get_all_queue():
    """Get all IDs in queue"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM queue")
            return [row[0] for row in cursor.fetchall()]
        except Error as e:
            print(f"Error fetching queue: {e}")
            return []
        finally:
            conn.close()
    return []

def clear_queue():
    """Clear all from queue"""
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM queue")
            conn.commit()
            count = cursor.rowcount
            print(f"Cleared {count} items from queue")
            return count
        except Error as e:
            print(f"Error clearing queue: {e}")
            return -1
        finally:
            conn.close()
    return -1