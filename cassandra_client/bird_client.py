import threading
import queue
import time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

# Basic configuration
print("Starting bird client...")
KEYSPACE = "trackbirds"
num_birds = 10  # 10 birds as per requirements
species_list = ['sparrow', 'eagle', 'hawk', 'owl', 'pigeon', 'seagull', 'robin', 'swan', 'falcon', 'woodpecker']
base_date = datetime.now().date()

# Connect to Cassandra
print("\nConnecting to Cassandra...")
try:
    cluster = Cluster(['cassandra-1', 'cassandra-2', 'cassandra-3', 'cassandra-4'], port=9042, connect_timeout=10)
    session = cluster.connect()
    print("Connected to Cassandra")
except Exception as e:
    print(f"Connection error: {e}")
    exit(1)

# Create keyspace and table
print("\nSetting up database...")
try:
    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)
    
    session.execute("""
    CREATE TABLE IF NOT EXISTS birds_tracking (
        bird_id TEXT,
        date DATE,
        timestamp TIMESTAMP,
        species TEXT,
        latitude DOUBLE,
        longitude DOUBLE,
        PRIMARY KEY ((bird_id, date), timestamp)
    )""")
    print("Database ready")
except Exception as e:
    print(f"Setup error: {e}")
    exit(1)

# Worker function
def worker(q, worker_id):
    print(f"Worker {worker_id} started")
    while True:
        try:
            task = q.get()
            if task is None:
                break
                
            bird_id, date, timestamp, species, lat, lon = task
            print(f"Worker {worker_id}: Processing {bird_id} at {timestamp}")
            
            # Insert with tracing for initial inserts only
            if 'update' not in str(timestamp):
                print(f"Worker {worker_id}: Sending initial insert with tracing...")
                try:
                    result = session.execute(
                        "INSERT INTO birds_tracking (bird_id, date, timestamp, species, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s)",
                        (bird_id, date, timestamp, species, lat, lon),
                        trace=True
                    )
                    trace = result.get_query_trace()
                    if trace:
                        print(f"Worker {worker_id}: Trace ID: {trace.trace_id}")
                        print(f"Worker {worker_id}: Coordinator: {trace.coordinator}")
                        print(f"Worker {worker_id}: Duration: {trace.duration} us")
                    print(f"Worker {worker_id}: Inserted initial data for {bird_id}")
                except Exception as e:
                    print(f"Worker {worker_id}: Error during trace: {e}")
            else:
                # Regular insert for updates (no tracing)
                session.execute(
                    "INSERT INTO birds_tracking (bird_id, date, timestamp, species, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s)",
                    (bird_id, date, timestamp, species, lat, lon)
                )
                print(f"Worker {worker_id}: Updated data for {bird_id}")
            
            q.task_done()
            
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")
    
    print(f"Worker {worker_id} shutting down")

# Create queue and workers
task_queue = queue.Queue()
num_workers = 2
workers = []

# Start worker threads
print("\nStarting worker threads...")
for i in range(num_workers):
    t = threading.Thread(target=worker, args=(task_queue, i))
    t.start()
    workers.append(t)

# Add initial locations
print("\nAdding initial bird locations...")
for i in range(num_birds):
    bird_id = f"bird_{i}"
    species = species_list[i % len(species_list)]
    timestamp = datetime.now()
    lat = 40.0 + i
    lon = -70.0 - i
    
    print(f"Queueing initial location for {bird_id}")
    task_queue.put((bird_id, base_date, timestamp, species, lat, lon))

# Wait for initial inserts
print("\nWaiting for initial inserts to complete...")
task_queue.join()
print("Initial inserts completed")

# Add some updates
print("\nAdding updates...")
for i in range(num_birds):
    bird_id = f"bird_{i}"
    species = species_list[i % len(species_list)]
    
    for update in range(20):  # 20 updates per bird as per requirements
        # Use current time for each update
        timestamp = datetime.now()
        # Simulate some movement
        lat = 40.0 + i + (update * 0.01)  # Small changes in latitude
        lon = -70.0 - i - (update * 0.01)  # Small changes in longitude
        
        print(f"Queueing update {update+1} for {bird_id} at {timestamp}")
        task_queue.put((bird_id, base_date, timestamp, species, lat, lon))
        
        # Add 5-second delay between updates
        if update < 19:  # No need to wait after the last update
            time.sleep(5)  # 5-second delay between updates

# Wait for updates to complete
print("\nWaiting for updates to complete...")
task_queue.join()

# Shutdown workers
print("\nShutting down workers...")
for _ in range(num_workers):
    task_queue.put(None)
for t in workers:
    t.join()

print("\nAll done!")