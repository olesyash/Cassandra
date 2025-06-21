import threading
import queue
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Cassandra connection
cluster = Cluster(['cassandra-1', 'cassandra-2', 'cassandra-3', 'cassandra-4'], port=9042)
session = cluster.connect()

KEYSPACE = "trackbirds"
# Create keyspace if it doesn't exist
session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '3'}}
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
) WITH CLUSTERING ORDER BY (timestamp ASC);
""")

def insert_worker(q):
    while True:
        item = q.get()
        if item is None:
            break
        bird_id, date, timestamp, species, lat, lon = item
        result = session.execute(
            "INSERT INTO birds_tracking (bird_id, date, timestamp, species, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s)",
            (bird_id, date, timestamp, species, lat, lon),
            trace=True
        )
        trace = result.get_query_trace()
        if trace:
            print(f"Trace for {bird_id} at {timestamp}: {trace.trace_id}")
        q.task_done()

task_queue = queue.Queue()
num_birds = 10
species_list = ['sparrow', 'eagle', 'stork', 'owl', 'hawk']
base_date = datetime.now().date()

for i in range(num_birds):
    bird_id = f"bird_{i}"
    species = random.choice(species_list)
    for minute in range(60):  # 1 hour of per-minute locations
        timestamp = datetime.now().replace(second=0, microsecond=0) + timedelta(minutes=minute)
        lat = random.uniform(-90, 90)
        lon = random.uniform(-180, 180)
        task_queue.put((bird_id, base_date, timestamp, species, lat, lon))

threads = []
for _ in range(4):
    t = threading.Thread(target=insert_worker, args=(task_queue,))
    t.start()
    threads.append(t)

task_queue.join()
for _ in threads:
    task_queue.put(None)
for t in threads:
    t.join()

print("All bird data inserted.")
