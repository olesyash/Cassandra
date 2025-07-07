#!/usr/bin/env python3
"""
Bird Client for Cassandra
Creates birds tracking table, inserts initial bird locations, and performs periodic updates.
"""

import time
import os
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class BirdClient:
    def __init__(self):
        # Configuration
        self.cluster_hosts = os.getenv(
            "CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3,cassandra-4"
        ).split(",")
        self.port = 9042
        self.keyspace = "trackbirds"
        self.table = "birds_tracking"

        # Bird configuration
        self.num_birds = 10
        self.updates_per_bird = 20
        self.update_interval = 5  # 5 seconds for testing
        
        # Tracing configuration
        self.traced_bird_id = "bird_01"  # Focus on one bird for detailed tracing
        self.trace_log_file = "bird_update_traces.log"

        # Bird species for variety
        self.species_list = [
            "Eurasian Hoopoe ",
            "White-spectacled Bulbul",
            "Griffon Vulture",
            "Tristram's Starling",
            "Chukar Partridge",
            "Little Green Bee-eater",
            "Syrian Woodpecker",
            "Desert Lark",
            "Barn Owl",
            "European Roller",
        ]

        # Initialize connection
        self.cluster = None
        self.session = None
        self.connect_to_cassandra()
        
        # Initialize trace log file
        self.initialize_trace_log()

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

    def initialize_trace_log(self):
        """Initialize the trace log file for UPDATE operations"""
        try:
            with open(self.trace_log_file, "w", encoding="utf-8") as f:
                f.write("=== Bird UPDATE Operations Trace Log ===\n")
                f.write(f"Traced Bird: {self.traced_bird_id}\n")
                f.write(f"Log started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
            print(f"✓ Trace log file initialized: {self.trace_log_file}")
        except Exception as e:
            print(f"✗ Failed to initialize trace log file: {e}")

    def parse_and_log_trace(self, operation_type, bird_id, trace, operation_details):
        """Parse trace results and log detailed information about coordinator and replicas"""
        if not trace:
            return
            
        try:
            with open(self.trace_log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"\n[{timestamp}] {operation_type} OPERATION TRACE for {bird_id}\n")
                f.write("-" * 60 + "\n")
                f.write(f"Operation Details: {operation_details}\n")
                f.write(f"Trace ID: {trace.trace_id}\n")
                f.write(f"Coordinator: {trace.coordinator}\n")
                f.write(f"Total Duration: {trace.duration.total_seconds() * 1000000:.0f} microseconds\n")
                f.write(f"Request Type: {getattr(trace, 'request_type', 'N/A')}\n")
                f.write(f"Parameters: {getattr(trace, 'parameters', 'N/A')}\n\n")
                
                # Parse trace events to show operation flow
                f.write("OPERATION FLOW (Coordinator and Replicas Timeline):\n")
                f.write("-" * 50 + "\n")
                
                if hasattr(trace, 'events') and trace.events:
                    for i, event in enumerate(trace.events, 1):
                        source = getattr(event, 'source', 'Unknown')
                        source_elapsed_raw = getattr(event, 'source_elapsed', 0)
                        thread_name = getattr(event, 'thread_name', 'Unknown')
                        activity = getattr(event, 'description', 'Unknown activity')
                        
                        # Convert source_elapsed to microseconds if it's a timedelta
                        if hasattr(source_elapsed_raw, 'total_seconds'):
                            source_elapsed = int(source_elapsed_raw.total_seconds() * 1000000)
                        else:
                            source_elapsed = int(source_elapsed_raw) if source_elapsed_raw else 0
                        
                        f.write(f"  Step {i:2d}: [{source_elapsed:8d} μs] {source}\n")
                        f.write(f"           Thread: {thread_name}\n")
                        f.write(f"           Activity: {activity}\n")
                        
                        # Identify coordinator vs replica operations
                        if source == str(trace.coordinator):
                            f.write(f"           >>> COORDINATOR OPERATION <<<\n")
                        else:
                            f.write(f"           >>> REPLICA OPERATION <<<\n")
                        f.write("\n")
                else:
                    f.write("  No detailed trace events available\n")
                
                f.write("=" * 60 + "\n\n")
                
        except Exception as e:
            print(f"✗ Error logging trace information: {e}")

    def create_keyspace_and_table(self):
        """Create keyspace and birds tracking table if they don't exist"""
        try:
            print(f"Creating keyspace '{self.keyspace}' if it doesn't exist...")

            # Create keyspace
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}
            """)

            # Set keyspace
            self.session.set_keyspace(self.keyspace)

            # Create table
            print(f"Creating table '{self.table}' if it doesn't exist...")
            self.session.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    bird_id TEXT,
                    date DATE,
                    timestamp TIMESTAMP,
                    species TEXT,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    PRIMARY KEY ((bird_id, date), timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """)

            print("✓ Keyspace and table created successfully")

        except Exception as e:
            print(f"✗ Error creating keyspace/table: {e}")
            raise

    def insert_initial_bird_locations(self):
        """Insert initial locations for 10 birds"""
        print(f"\nInserting initial locations for {self.num_birds} birds...")

        # Prepare statement for better performance
        insert_stmt = self.session.prepare(f"""
            INSERT INTO {self.table} (bird_id, date, timestamp, species, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?)
        """)

        base_lat = 40.7128  # Starting around NYC
        base_lon = -74.0060

        for i in range(self.num_birds):
            bird_id = f"bird_{i + 1:02d}"
            species = self.species_list[i % len(self.species_list)]
            timestamp = datetime.now()

            # Generate initial location with some spread
            latitude = base_lat + (i * 0.1)
            longitude = base_lon + (i * 0.1)

            try:
                # Add tracing for the specific traced bird's initial insert
                if bird_id == self.traced_bird_id:
                    print(f"  🔍 TRACING INITIAL INSERT for {bird_id}...")
                    date = timestamp.date()
                    result = self.session.execute(
                        insert_stmt, (bird_id, date, timestamp, species, latitude, longitude),
                        trace=True
                    )
                    
                    # Get and parse trace information
                    trace = result.get_query_trace()
                    operation_details = f"INITIAL INSERT location ({latitude:.4f}, {longitude:.4f})"
                    self.parse_and_log_trace("INSERT", bird_id, trace, operation_details)
                    
                    print(f"  ✓ TRACED INITIAL INSERT for {bird_id} ({species}) at ({latitude:.4f}, {longitude:.4f}) - Trace logged")
                else:
                    # Regular insert without tracing for other birds
                    date = timestamp.date()
                    self.session.execute(
                        insert_stmt, (bird_id, date, timestamp, species, latitude, longitude)
                    )
                    print(
                        f"  ✓ Inserted initial location for {bird_id} ({species}) at ({latitude:.4f}, {longitude:.4f})"
                    )

                # Small delay between insertions
                time.sleep(1)

            except Exception as e:
                print(f"  ✗ Failed to insert {bird_id}: {e}")

        print(f"✓ Completed initial insertions for {self.num_birds} birds")

    def update_bird_locations(self):
        """Run 20 update operations for each bird with 1-minute intervals"""
        print(f"\nStarting location updates: {self.updates_per_bird} updates per bird")
        print(f"Update interval: {self.update_interval} seconds")

        # Prepare statement for better performance
        update_stmt = self.session.prepare(f"""
            INSERT INTO {self.table} (bird_id, date, timestamp, species, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?)
        """)

        base_lat = 40.7128
        base_lon = -74.0060

        for update_round in range(self.updates_per_bird):
            print(f"\n--- Update Round {update_round + 1}/{self.updates_per_bird} ---")

            for i in range(self.num_birds):
                bird_id = f"bird_{i + 1:02d}"
                species = self.species_list[i % len(self.species_list)]
                timestamp = datetime.now()

                # Simulate bird movement (random walk)
                # Each update moves the bird slightly from its base position
                movement_factor = (update_round + 1) * 0.01
                latitude = (
                    base_lat + (i * 0.1) + (movement_factor * (1 if i % 2 == 0 else -1))
                )
                longitude = (
                    base_lon + (i * 0.1) + (movement_factor * (1 if i % 3 == 0 else -1))
                )

                try:
                    # Add tracing for the specific traced bird
                    if bird_id == self.traced_bird_id:
                        print(f"  🔍 TRACING UPDATE for {bird_id}...")
                        date = timestamp.date()
                        result = self.session.execute(
                            update_stmt, (bird_id, date, timestamp, species, latitude, longitude),
                            trace=True
                        )
                        
                        # Get and parse trace information
                        trace = result.get_query_trace()
                        operation_details = f"UPDATE location to ({latitude:.4f}, {longitude:.4f}) - Round {update_round + 1}"
                        self.parse_and_log_trace("UPDATE", bird_id, trace, operation_details)
                        
                        print(f"  ✓ TRACED UPDATE {bird_id} location to ({latitude:.4f}, {longitude:.4f}) - Trace logged")
                    else:
                        # Regular update without tracing for other birds
                        date = timestamp.date()
                        self.session.execute(
                            update_stmt, (bird_id, date, timestamp, species, latitude, longitude)
                        )
                        print(
                            f"  ✓ Updated {bird_id} location to ({latitude:.4f}, {longitude:.4f})"
                        )

                except Exception as e:
                    print(f"  ✗ Failed to update {bird_id}: {e}")

            # Wait for the specified interval before next update round
            if (
                update_round < self.updates_per_bird - 1
            ):  # Don't wait after the last update
                print(
                    f"  ⏳ Waiting {self.update_interval} seconds before next update..."
                )
                time.sleep(self.update_interval)

        print(
            f"\n✓ Completed all {self.updates_per_bird} update rounds for {self.num_birds} birds"
        )

    def run(self):
        """Main execution method"""
        try:
            print("=== Bird Client Starting ===")
            print(
                f"Target: {self.num_birds} birds with {self.updates_per_bird} updates each"
            )
            print(f"Update interval: {self.update_interval} seconds")
            print(f"🔍 TRACING ENABLED for {self.traced_bird_id} - UPDATE operations will be traced")
            print(f"📁 Trace results will be logged to: {self.trace_log_file}")

            # Step 1: Create keyspace and table
            self.create_keyspace_and_table()

            # Step 2: Insert initial bird locations
            self.insert_initial_bird_locations()

            # Step 3: Perform periodic updates
            self.update_bird_locations()

            print("\n=== Bird Client Completed Successfully ===")
            print(f"📊 Trace Summary:")
            print(f"   - Traced bird: {self.traced_bird_id}")
            print(f"   - Total operations traced: {1 + self.updates_per_bird} (1 INSERT + {self.updates_per_bird} UPDATEs)")
            print(f"   - Trace log file: {self.trace_log_file}")
            print(f"   - Check the trace file for detailed coordinator/replica flow analysis")

        except Exception as e:
            print(f"\n✗ Bird Client failed: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up connections"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        print("Connection closed")


if __name__ == "__main__":
    client = BirdClient()
    client.run()
