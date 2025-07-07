#!/usr/bin/env python3
"""
Combined Threaded Client for Cassandra
Uses queues and threads where each thread runs different workloads:
- Bird thread: Handles bird location inserts and updates
- Tracker thread: Handles periodic queries and logging
"""

import threading
import queue
import time
import os
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class CombinedThreadedClient:
    def __init__(self):
        # Configuration
        self.cluster_hosts = os.getenv(
            "CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3,cassandra-4"
        ).split(",")
        self.port = 9042
        self.keyspace = "birds_tracking"
        self.table = "bird_locations"

        # Bird configuration
        self.num_birds = 10
        self.updates_per_bird = 20
        self.bird_update_interval = 60  # 1 minute between bird updates

        # Tracker configuration
        self.tracker_query_interval = 30  # 30 seconds between tracker queries
        self.log_file = "combined_bird_tracking_log.txt"

        # Species list
        self.species_list = [
            "Sparrow",
            "Eagle",
            "Hawk",
            "Owl",
            "Pigeon",
            "Seagull",
            "Robin",
            "Swan",
            "Falcon",
            "Woodpecker",
        ]

        # Threading components
        self.bird_queue = queue.Queue()
        self.tracker_queue = queue.Queue()
        self.shutdown_event = threading.Event()

        # Shared session (thread-safe)
        self.cluster = None
        self.session = None
        self.connect_to_cassandra()

        # Initialize log file
        self.initialize_log_file()

    def connect_to_cassandra(self):
        """Connect to Cassandra cluster"""
        try:
            print("Connecting to Cassandra cluster...")
            self.cluster = Cluster(
                contact_points=self.cluster_hosts, port=self.port, connect_timeout=10
            )
            self.session = self.cluster.connect()
            print(f"✓ Connected to Cassandra cluster at {self.cluster_hosts}")
        except Exception as e:
            print(f"✗ Failed to connect to Cassandra: {e}")
            raise

    def setup_keyspace_and_table(self):
        """Create keyspace and table if they don't exist"""
        try:
            print(f"Setting up keyspace '{self.keyspace}' and table '{self.table}'...")

            # Create keyspace
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 2}}
            """)

            # Set keyspace
            self.session.set_keyspace(self.keyspace)

            # Create table
            self.session.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    bird_id TEXT,
                    timestamp TIMESTAMP,
                    species TEXT,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    PRIMARY KEY (bird_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """)

            print("✓ Keyspace and table setup completed")

        except Exception as e:
            print(f"✗ Error setting up keyspace/table: {e}")
            raise

    def initialize_log_file(self):
        """Initialize the log file with headers"""
        try:
            with open(self.log_file, "w", encoding="utf-8") as f:
                f.write("=== Combined Bird Tracking Log ===\n")
                f.write(
                    f"Log started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                )
                f.write("Format: [Timestamp] Bird Operations | Tracker Queries\n")
                f.write("=" * 80 + "\n\n")
            print(f"✓ Log file initialized: {self.log_file}")
        except Exception as e:
            print(f"✗ Failed to initialize log file: {e}")
            raise

    def bird_worker(self):
        """Worker thread for bird operations (inserts and updates)"""
        print("🐦 Bird worker thread started")

        # Prepare statements
        insert_stmt = self.session.prepare(f"""
            INSERT INTO {self.table} (bird_id, timestamp, species, latitude, longitude)
            VALUES (?, ?, ?, ?, ?)
        """)

        base_lat = 40.7128  # NYC base location
        base_lon = -74.0060

        try:
            while not self.shutdown_event.is_set():
                try:
                    # Get task from queue (with timeout)
                    task = self.bird_queue.get(timeout=1)

                    if task is None:  # Shutdown signal
                        break

                    operation, bird_id, update_round = task
                    species = self.species_list[
                        int(bird_id.split("_")[1]) % len(self.species_list)
                    ]
                    timestamp = datetime.now()

                    # Calculate location based on bird and update round
                    bird_index = int(bird_id.split("_")[1])
                    if operation == "initial":
                        latitude = base_lat + (bird_index * 0.1)
                        longitude = base_lon + (bird_index * 0.1)
                    else:  # update
                        movement_factor = update_round * 0.01
                        latitude = (
                            base_lat
                            + (bird_index * 0.1)
                            + (movement_factor * (1 if bird_index % 2 == 0 else -1))
                        )
                        longitude = (
                            base_lon
                            + (bird_index * 0.1)
                            + (movement_factor * (1 if bird_index % 3 == 0 else -1))
                        )

                    # Execute insert
                    self.session.execute(
                        insert_stmt, (bird_id, timestamp, species, latitude, longitude)
                    )

                    # Log to file
                    with open(self.log_file, "a", encoding="utf-8") as f:
                        f.write(
                            f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] BIRD: {operation} - {bird_id} ({species}) at ({latitude:.4f}, {longitude:.4f})\n"
                        )

                    print(
                        f"🐦 {operation.capitalize()} {bird_id} at ({latitude:.4f}, {longitude:.4f})"
                    )

                    self.bird_queue.task_done()

                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"🐦 Error in bird worker: {e}")

        except Exception as e:
            print(f"🐦 Bird worker thread failed: {e}")
        finally:
            print("🐦 Bird worker thread stopped")

    def tracker_worker(self):
        """Worker thread for tracker operations (periodic queries)"""
        print("🔍 Tracker worker thread started")

        # Prepare query statement
        query_stmt = self.session.prepare(f"""
            SELECT timestamp, species, latitude, longitude
            FROM {self.table}
            WHERE bird_id = ?
            ORDER BY timestamp DESC
        """)

        try:
            while not self.shutdown_event.is_set():
                try:
                    # Get task from queue (with timeout)
                    task = self.tracker_queue.get(timeout=1)

                    if task is None:  # Shutdown signal
                        break

                    query_time = datetime.now()

                    # Query all birds
                    tracking_results = {}
                    for i in range(self.num_birds):
                        bird_id = f"bird_{i + 1:02d}"

                        try:
                            rows = list(self.session.execute(query_stmt, (bird_id,)))

                            if rows:
                                last_location = rows[
                                    0
                                ]  # Most recent (first in DESC order)
                                tracking_results[bird_id] = {
                                    "species": last_location.species,
                                    "last_timestamp": last_location.timestamp,
                                    "latitude": last_location.latitude,
                                    "longitude": last_location.longitude,
                                    "total_locations": len(rows),
                                }
                            else:
                                tracking_results[bird_id] = {
                                    "error": "No locations found"
                                }

                        except Exception as e:
                            tracking_results[bird_id] = {"error": str(e)}

                    # Log tracking results
                    with open(self.log_file, "a", encoding="utf-8") as f:
                        f.write(
                            f"[{query_time.strftime('%Y-%m-%d %H:%M:%S')}] TRACKER: Query Results:\n"
                        )
                        for bird_id, result in tracking_results.items():
                            if "error" in result:
                                f.write(f"  {bird_id}: {result['error']}\n")
                            else:
                                f.write(
                                    f"  {bird_id} ({result['species']}): Last at ({result['latitude']:.4f}, {result['longitude']:.4f}) "
                                    f"on {result['last_timestamp'].strftime('%Y-%m-%d %H:%M:%S')} | Total: {result['total_locations']}\n"
                                )
                        f.write("\n")

                    print(
                        f"🔍 Tracker query completed for {len(tracking_results)} birds"
                    )

                    self.tracker_queue.task_done()

                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"🔍 Error in tracker worker: {e}")

        except Exception as e:
            print(f"🔍 Tracker worker thread failed: {e}")
        finally:
            print("🔍 Tracker worker thread stopped")

    def schedule_bird_operations(self):
        """Schedule bird operations (initial inserts and updates)"""
        print("📅 Scheduling bird operations...")

        # Schedule initial inserts
        for i in range(self.num_birds):
            bird_id = f"bird_{i + 1:02d}"
            self.bird_queue.put(("initial", bird_id, 0))

        # Schedule updates
        for update_round in range(self.updates_per_bird):
            for i in range(self.num_birds):
                bird_id = f"bird_{i + 1:02d}"
                self.bird_queue.put(("update", bird_id, update_round + 1))

        print(
            f"✓ Scheduled {self.num_birds} initial operations and {self.num_birds * self.updates_per_bird} updates"
        )

    def schedule_tracker_operations(self, duration_minutes=10):
        """Schedule tracker operations for a specified duration"""
        print(f"📅 Scheduling tracker operations for {duration_minutes} minutes...")

        # Calculate number of queries needed
        total_queries = (duration_minutes * 60) // self.tracker_query_interval

        for i in range(total_queries):
            self.tracker_queue.put("query")

        print(f"✓ Scheduled {total_queries} tracker queries")

    def run_combined_client(self, duration_minutes=10):
        """Run the combined client with both bird and tracker threads"""
        print("=== Combined Threaded Client Starting ===")
        print(f"Duration: {duration_minutes} minutes")
        print(f"Bird update interval: {self.bird_update_interval} seconds")
        print(f"Tracker query interval: {self.tracker_query_interval} seconds")

        try:
            # Setup database
            self.setup_keyspace_and_table()

            # Start worker threads
            bird_thread = threading.Thread(target=self.bird_worker, name="BirdWorker")
            tracker_thread = threading.Thread(
                target=self.tracker_worker, name="TrackerWorker"
            )

            bird_thread.start()
            tracker_thread.start()

            # Schedule operations
            self.schedule_bird_operations()
            self.schedule_tracker_operations(duration_minutes)

            # Main control loop
            start_time = time.time()
            end_time = start_time + (duration_minutes * 60)

            bird_update_next = start_time + self.bird_update_interval
            tracker_query_next = start_time + self.tracker_query_interval

            while time.time() < end_time and not self.shutdown_event.is_set():
                current_time = time.time()

                # Check if it's time for tracker query
                if current_time >= tracker_query_next:
                    if not self.tracker_queue.empty():
                        tracker_query_next = current_time + self.tracker_query_interval
                    else:
                        print("🔍 No more tracker queries scheduled")

                time.sleep(1)  # Check every second

            print(f"\n⏰ {duration_minutes} minutes elapsed, shutting down...")

        except KeyboardInterrupt:
            print("\n⏹️  Shutdown requested by user")
        except Exception as e:
            print(f"\n✗ Combined client failed: {e}")
        finally:
            # Shutdown threads
            self.shutdown_event.set()

            # Send shutdown signals
            self.bird_queue.put(None)
            self.tracker_queue.put(None)

            # Wait for threads to finish
            bird_thread.join(timeout=5)
            tracker_thread.join(timeout=5)

            print("✓ All threads stopped")

            # Clean up
            self.cleanup()

    def cleanup(self):
        """Clean up connections"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        print("Connection closed")


def main():
    """Main function"""
    client = CombinedThreadedClient()

    try:
        duration = input("Enter duration in minutes (default 10): ").strip()
        duration = int(duration) if duration else 10

        client.run_combined_client(duration_minutes=duration)

    except ValueError:
        print("Invalid input, using default duration of 10 minutes")
        client.run_combined_client(duration_minutes=10)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
