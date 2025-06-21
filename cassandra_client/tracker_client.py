import threading
import queue
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime

LOG_FILE = "bird_locations.log"

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

def query_worker(q, log_file):
    while True:
        bird_id, date = q.get()
        if bird_id is None:
            break
        result = session.execute(
            "SELECT timestamp, species, latitude, longitude FROM birds_tracking WHERE bird_id=%s AND date=%s",
            (bird_id, date),
            trace=True
        )
        trace = result.get_query_trace()
        with open(log_file, 'a') as f:
            for row in result:
                f.write(f"{bird_id},{date},{row.timestamp},{row.species},{row.latitude},{row.longitude}\n")
            if trace:
                f.write(f"TRACE for {bird_id} on {date}: {trace.trace_id}\n")
                for event in trace.events:
                    f.write(f"  {event.description} @ {event.source_elapsed}us from {event.source}\n")
        q.task_done()

task_queue = queue.Queue()
num_birds = 10
base_date = datetime.now().date()

for i in range(num_birds):
    bird_id = f"bird_{i}"
    task_queue.put((bird_id, base_date))

threads = []
for _ in range(4):
    t = threading.Thread(target=query_worker, args=(task_queue, LOG_FILE))
    t.start()
    threads.append(t)

task_queue.join()
for _ in threads:
    task_queue.put((None, None))
for t in threads:
    t.join()

print("All queries completed and logged.")
