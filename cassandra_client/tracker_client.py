import threading
import queue
import time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime

LOG_FILE = "bird_locations.log"
TRACE_LOG_FILE = "query_traces.log"

# Cassandra connection
print("Connecting to Cassandra cluster...")
try:
    cluster = Cluster(['cassandra-1', 'cassandra-2', 'cassandra-3', 'cassandra-4'], port=9042, connect_timeout=10)
    session = cluster.connect()
    print("Connected to Cassandra cluster successfully")
except Exception as e:
    print(f"Error connecting to Cassandra: {e}")
    print("Check if your Cassandra nodes are running and accessible")
    print("You may need to modify the connection settings in the code")
    exit(1)

KEYSPACE = "trackbirds"
# Create keyspace if it doesn't exist
session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '3'}}
""")
session.set_keyspace(KEYSPACE)

def query_worker(q, log_file, trace_log_file):
    while True:
        bird_id, date = q.get()
        if bird_id is None:
            q.task_done()
            break
            
        try:
            # Query for ALL locations for this bird, ordered by timestamp
            query = """
                SELECT timestamp, species, latitude, longitude 
                FROM birds_tracking 
                WHERE bird_id = %s AND date = %s
                ORDER BY timestamp ASC
                ALLOW FILTERING
            """
            
            # Execute the query with tracing
            result = list(session.execute(query, [bird_id, date], trace=True, timeout=10.0))
            
            # Get the trace information
            print(f"Getting query trace for {bird_id}...")
            trace = result.get_query_trace() if hasattr(result, 'get_query_trace') else None
            
            # Process the results
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"\n--- Query for {bird_id} at {datetime.now()} ---\n")
                
                if not result:
                    f.write("No location data found\n")
                else:
                    # Get all timestamps for this bird as strings
                    all_timestamps = {str(row.timestamp) for row in result if hasattr(row, 'timestamp')}
                    
                    # Find new timestamps since last query
                    last_known = last_seen.get(bird_id, set())
                    # Ensure last_known is a set to avoid NoneType errors
                    last_known = set() if last_known is None else set(str(t) for t in last_known) if isinstance(last_known, (set, list, tuple)) else set()
                    new_timestamps = all_timestamps - last_known
                    
                    # Update last seen with current timestamps as strings
                    last_seen[bird_id] = all_timestamps
                    
                    # Log all locations
                    f.write(f"\nAll locations for {bird_id} (total: {len(result)}):\n")
                    for i, row in enumerate(result, 1):
                        timestamp = getattr(row, 'timestamp', 'N/A')
                        species = getattr(row, 'species', 'N/A')
                        lat = getattr(row, 'latitude', 'N/A')
                        lon = getattr(row, 'longitude', 'N/A')
                        
                        loc_str = f"{i}. {timestamp} | {species}: {lat}, {lon}"
                        if timestamp in new_timestamps:
                            loc_str = "NEW: " + loc_str
                        f.write(loc_str + "\n")
                    
                    # Write summary
                    f.write(f"\nSummary for {bird_id}:")
                    f.write(f"\n- Total locations in database: {len(result)}")
                    f.write(f"\n- New locations this query: {len(new_timestamps)}")
                    
                    if result:
                        last = result[-1]
                        f.write("\n\nLast known location:")
                        f.write(f"\n- Timestamp: {getattr(last, 'timestamp', 'N/A')}")
                        f.write(f"\n- Species: {getattr(last, 'species', 'N/A')}")
                        f.write(f"\n- Coordinates: {getattr(last, 'latitude', 'N/A')}, {getattr(last, 'longitude', 'N/A')}")
                    
                    f.write("\n")
            
            # Write trace information if available
            if trace:
                try:
                    with open(trace_log_file, 'a', encoding='utf-8') as tf:
                        tf.write(f"\n{'='*80}\n")
                        tf.write(f"TRACE SUMMARY for {bird_id} on {date}\n")
                        tf.write(f"Trace ID: {getattr(trace, 'trace_id', 'N/A')}\n")
                        tf.write(f"Coordinator: {getattr(trace, 'coordinator', 'N/A')}\n")
                        tf.write(f"Duration: {getattr(getattr(trace, 'duration', None), 'microseconds', 'N/A')} us\n")
                        
                        # Count events by source
                        if hasattr(trace, 'events'):
                            sources = {}
                            for event in trace.events:
                                source = getattr(event, 'source', 'unknown')
                                sources[source] = sources.get(source, 0) + 1
                            
                            tf.write("\nEvents by source:\n")
                            for source, count in sources.items():
                                tf.write(f"  - {source}: {count} events\n")
                except Exception as e:
                    print(f"Error writing trace log: {e}")
            
        except Exception as e:
            print(f"Error processing {bird_id}: {e}")
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"Error processing {bird_id}: {e}\n")
        
        # Add a small delay between queries to avoid overwhelming the cluster
        time.sleep(1)
        q.task_done()

# Track last seen timestamp for each bird (initialize empty, will be updated dynamically)
last_seen = {}
# Track consecutive empty results for backoff
consecutive_empty = {}

def get_all_bird_ids():
    """Fetch all unique bird_ids from the database, deduplicating in Python."""
    try:
        rows = session.execute("SELECT bird_id, date FROM birds_tracking")
        bird_ids = set()
        for row in rows:
            bird_ids.add(row.bird_id)
        return list(bird_ids)
    except Exception as e:
        print(f"Error fetching bird ids: {e}")
        return []

# Clear log files before starting
with open(LOG_FILE, 'w') as f:
    f.write("=== Bird Tracking Log ===\n")
    f.write("Format: bird_id | date | timestamp | species | latitude, longitude\n\n")
    
with open(TRACE_LOG_FILE, 'w') as f:
    f.write("=== Query Trace Log ===\n\n")

def get_bird_updates(bird_id, last_timestamp):
    """Query for new updates for a specific bird since last_timestamp"""
    try:
        # First, get the most recent timestamp for this bird
        query = """
            SELECT timestamp, species, latitude, longitude 
            FROM birds_tracking 
            WHERE bird_id = %s AND date = %s
            ORDER BY timestamp ASC
            ALLOW FILTERING
        """
        
        # Execute with tracing disabled initially to avoid trace-related issues
        result = list(session.execute(query, [bird_id, base_date]))
        
        # If we have a last_timestamp, filter the results client-side
        if last_timestamp is not None:
            try:
                # Convert last_timestamp to string for comparison if it's a datetime object
                if hasattr(last_timestamp, 'strftime'):
                    last_timestamp_str = last_timestamp.strftime('%Y-%m-%d %H:%M:%S%z')
                else:
                    last_timestamp_str = str(last_timestamp)
                
                # Filter results to only include those after last_timestamp
                result = [row for row in result if str(row.timestamp) > last_timestamp_str]
            except Exception as e:
                print(f"Error filtering timestamps for {bird_id}: {e}")
                return []
        
        return result
        
    except Exception as e:
        print(f"Error querying {bird_id}: {e}")
        return []

def calculate_backoff(empty_count):
    """Calculate delay based on number of consecutive empty results"""
    # Start with 1s, max out at 5s
    return min(1.0 + (empty_count * 0.5), 5.0)

print(f"Starting tracker client at {datetime.now()}")
print(f"Logs will be written to {LOG_FILE} and {TRACE_LOG_FILE}")

task_queue = queue.Queue()
num_birds = 10
base_date = datetime.now().date()

# First round of queries - get initial locations
print("Querying initial bird locations...")
num_birds = 10  # Track 10 birds
for i in range(num_birds):
    bird_id = f"bird_{i}"
    task_queue.put((bird_id, base_date))

# Create worker threads (one per bird for better concurrency)
threads = []
num_workers = min(10, num_birds)  # Up to 10 worker threads
for _ in range(num_workers):
    t = threading.Thread(target=query_worker, args=(task_queue, LOG_FILE, TRACE_LOG_FILE))
    t.daemon = True  # Allow program to exit even if workers are running
    t.start()
    threads.append(t)

# Wait for initial queries to complete
task_queue.join()

# Simple endless loop tracking
print("Starting tracker...")
print("Press Ctrl+C to stop tracking\n")

try:
    while True:
        updates_found = 0
        print(f"\n--- Checking for updates at {datetime.now()} ---")
        
        # Dynamically discover all birds in the database
        all_bird_ids = get_all_bird_ids()
        for bird_id in all_bird_ids:
            if bird_id not in last_seen:
                print(f"Discovered new bird: {bird_id}")
                last_seen[bird_id] = None
        
        # Check each bird for updates
        for bird_id in last_seen.keys():
            # Ensure last_seen[bird_id] is a scalar (latest timestamp or None)
            if isinstance(last_seen[bird_id], set):
                if last_seen[bird_id]:
                    # Use the max timestamp from the set
                    last_seen[bird_id] = max(last_seen[bird_id])
                else:
                    last_seen[bird_id] = None
            updates = get_bird_updates(bird_id, last_seen[bird_id])
            

            # Ensure last_seen[bird_id] is a datetime or None (convert from string if needed)
       
            if isinstance(last_seen[bird_id], str):
                try:
                    last_seen[bird_id] = datetime.fromisoformat(last_seen[bird_id])
                except Exception:
                    last_seen[bird_id] = None
            # Filter updates to only those with timestamp > last_seen[bird_id]
            if last_seen[bird_id] is not None:
                updates = [u for u in updates if u.timestamp > last_seen[bird_id]]
            if updates:
                print(f"DEBUG: New updates for {bird_id}: {[row.timestamp for row in updates]}")
                last_seen[bird_id] = max(u.timestamp for u in updates)
                updates_found += len(updates)
                # Write updates to log and console
                with open(LOG_FILE, 'a', encoding='utf-8') as f:
                    print(f"\nFound {len(updates)} new locations for {bird_id}:")
                    
                    for i, update in enumerate(updates, 1):
                        # Format the update string
                        update_str = (f"{update.timestamp} | {getattr(update, 'species', 'N/A')}:" 
                                    f" {getattr(update, 'latitude', 'N/A')}, {getattr(update, 'longitude', 'N/A')}")
                        
                        # Print to console with numbering
                        print(f"  {i}. {update_str}")
                        
                        # Write to file with bird_id for clarity
                        f.write(f"{bird_id} | {update_str}\n")
                    
                    # Write summary after updates
                    f.write(f"\nSummary for {bird_id}:")
                    f.write(f"\n- Total locations in this batch: {len(updates)}")
                    f.write(f"\n- Last known timestamp: {updates[-1].timestamp if updates else 'N/A'}")
                    f.write(f"\n- Last known coordinates: {getattr(updates[-1], 'latitude', 'N/A')}, {getattr(updates[-1], 'longitude', 'N/A')}\n")
                    f.flush()  # Ensure all updates are written to disk
        
        print(f"Found {updates_found} new updates")
        print("Waiting 2 seconds before next check...")
        time.sleep(2)  # Fixed 2-second delay between checks
        
except KeyboardInterrupt:
    print("\nStopping tracker...")

# Clean up threads
for _ in threads:
    task_queue.put((None, None))
for t in threads:
    t.join()

print(f"\nAll queries completed at {datetime.now()}")
print(f"Bird locations logged to {LOG_FILE}")
print(f"Query traces logged to {TRACE_LOG_FILE}")
