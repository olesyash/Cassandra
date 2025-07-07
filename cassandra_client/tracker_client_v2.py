#!/usr/bin/env python3
"""
Tracker Client for Cassandra
Queries bird locations periodically, derives last location, and writes to log file.
"""

import time
import os
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class TrackerClient:
    def __init__(self):
        # Configuration
        self.cluster_hosts = os.getenv(
            "CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3,cassandra-4"
        ).split(",")
        self.port = 9042
        self.keyspace = "trackbirds"
        self.table = "birds_tracking"

        # Tracking configuration
        self.num_birds = 10
        self.query_interval = 30  # Query every 30 seconds
        self.log_file = "bird_tracking_log.txt"
        
        # Tracing configuration
        self.traced_bird_id = "bird_01"  # Focus on same bird as bird_client_v2
        self.trace_log_file = "bird_select_traces.log"

        # Initialize connection
        self.cluster = None
        self.session = None
        self.connect_to_cassandra()

        # Initialize log files
        self.initialize_log_file()
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

            # Set keyspace
            self.session.set_keyspace(self.keyspace)
            print(f"✓ Using keyspace '{self.keyspace}'")

        except Exception as e:
            print(f"✗ Failed to connect to Cassandra: {e}")
            raise

    def initialize_log_file(self):
        """Initialize the log file with headers"""
        try:
            with open(self.log_file, "w", encoding="utf-8") as f:
                f.write("=== Bird Tracking Log ===\n")
                f.write(
                    f"Log started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                )
                f.write(
                    "Format: [Timestamp] Bird ID | Species | Last Location | Total Locations\n"
                )
                f.write("=" * 80 + "\n\n")
            print(f"✓ Log file initialized: {self.log_file}")
        except Exception as e:
            print(f"✗ Failed to initialize log file: {e}")
            raise

    def initialize_trace_log(self):
        """Initialize the trace log file for SELECT operations"""
        try:
            with open(self.trace_log_file, "w", encoding="utf-8") as f:
                f.write("=== Bird SELECT Operations Trace Log ===\n")
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

    def get_bird_locations(self, bird_id):
        """Get all locations for a specific bird for today's date, ordered by timestamp descending"""
        try:
            from datetime import date as date_type
            
            # Query for today's date (most realistic scenario for real-time tracking)
            today = date_type.today()
            
            # Get all locations for this bird_id and today's date
            # With the new primary key ((bird_id, date), timestamp), this query is efficient
            # and results will be automatically sorted by timestamp DESC due to clustering order
            query = f"""
                SELECT timestamp, species, latitude, longitude
                FROM {self.table}
                WHERE bird_id = ? AND date = ?
                ORDER BY timestamp DESC
            """
            stmt = self.session.prepare(query)

            # Add tracing for the specific traced bird
            if bird_id == self.traced_bird_id:
                print(f"  🔍 TRACING SELECT for {bird_id} on date {today}...")
                result = self.session.execute(stmt, (bird_id, today), trace=True)
                rows = list(result)
                
                # Get and parse trace information
                trace = result.get_query_trace()
                operation_details = f"SELECT all locations for {bird_id} on {today} (found {len(rows)} records)"
                self.parse_and_log_trace("SELECT", bird_id, trace, operation_details)
                print(f"  ✓ TRACED SELECT for {bird_id} on {today} - Found {len(rows)} locations - Trace logged")
                return rows
            else:
                # Regular query without tracing for other birds
                rows = list(self.session.execute(stmt, (bird_id, today)))
                return rows

        except Exception as e:
            print(f"✗ Error querying locations for {bird_id}: {e}")
            return []

    def derive_last_location(self, locations):
        """Derive the last (most recent) location from the list of locations"""
        if not locations:
            return None

        # Locations are already ordered by timestamp DESC, so first one is the latest
        return locations[0]

    def format_location_info(self, bird_id, locations):
        """Format location information for logging"""
        if not locations:
            return f"No locations found for {bird_id}"

        last_location = self.derive_last_location(locations)
        total_locations = len(locations)

        timestamp = last_location.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        species = last_location.species
        latitude = last_location.latitude
        longitude = last_location.longitude

        return (
            f"{bird_id} | {species} | "
            f"Last: ({latitude:.4f}, {longitude:.4f}) at {timestamp} | "
            f"Total locations: {total_locations}"
        )

    def log_tracking_data(self, query_time, tracking_data):
        """Write tracking data to log file"""
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(f"[{query_time}] Tracking Query Results:\n")
                f.write("-" * 60 + "\n")

                for bird_id, data in tracking_data.items():
                    f.write(f"  {data}\n")

                f.write("\n")

        except Exception as e:
            print(f"✗ Error writing to log file: {e}")

    def query_all_birds(self):
        """Query locations for all birds and return formatted data"""
        query_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tracking_data = {}

        print(f"\n--- Querying all birds at {query_time} ---")

        for i in range(self.num_birds):
            bird_id = f"bird_{i + 1:02d}"

            try:
                # Get locations for this bird
                locations = self.get_bird_locations(bird_id)

                # Format the information
                formatted_info = self.format_location_info(bird_id, locations)
                tracking_data[bird_id] = formatted_info

                print(f"  ✓ {bird_id}: {len(locations)} locations found")

            except Exception as e:
                error_msg = f"Error querying {bird_id}: {e}"
                tracking_data[bird_id] = error_msg
                print(f"  ✗ {error_msg}")

        return query_time, tracking_data

    def run_continuous_tracking(self, duration_minutes=None):
        """Run continuous tracking for a specified duration or indefinitely"""
        print("=== Tracker Client Starting ===")
        print(f"Query interval: {self.query_interval} seconds")
        print(f"Tracking {self.num_birds} birds")
        print(f"Log file: {self.log_file}")
        print(f"🔍 TRACING ENABLED for {self.traced_bird_id} - SELECT operations will be traced")
        print(f"📁 Trace results will be logged to: {self.trace_log_file}")

        if duration_minutes:
            print(f"Running for {duration_minutes} minutes")
            end_time = datetime.now().timestamp() + (duration_minutes * 60)
        else:
            print("Running indefinitely (Ctrl+C to stop)")
            end_time = None

        query_count = 0

        try:
            while True:
                query_count += 1

                # Query all birds
                query_time, tracking_data = self.query_all_birds()

                # Log the data
                self.log_tracking_data(query_time, tracking_data)

                print(f"✓ Query #{query_count} completed and logged")

                # Check if we should stop
                if end_time and datetime.now().timestamp() >= end_time:
                    print(
                        f"\n✓ Completed {query_count} queries over {duration_minutes} minutes"
                    )
                    break

                # Wait before next query
                print(f"⏳ Waiting {self.query_interval} seconds before next query...")
                time.sleep(self.query_interval)

        except KeyboardInterrupt:
            print(f"\n✓ Tracking stopped by user after {query_count} queries")
            print(f"📊 Trace Summary:")
            print(f"   - Traced bird: {self.traced_bird_id}")
            print(f"   - Total SELECT operations traced: {query_count}")
            print(f"   - Trace log file: {self.trace_log_file}")
            print(f"   - Check the trace file for detailed coordinator/replica flow analysis")
        except Exception as e:
            print(f"\n✗ Tracking failed: {e}")
            raise

    def run_single_query(self):
        """Run a single query for all birds"""
        print("=== Tracker Client - Single Query ===")

        query_time, tracking_data = self.query_all_birds()
        self.log_tracking_data(query_time, tracking_data)

        print(f"\n✓ Single query completed and logged to {self.log_file}")

        # Also print results to console
        print("\nResults:")
        for bird_id, data in tracking_data.items():
            print(f"  {data}")

    def cleanup(self):
        """Clean up connections"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        print("Connection closed")


def main():
    """Main function - runs continuous tracking indefinitely"""
    tracker = TrackerClient()

    try:
        print("\nStarting continuous tracking (indefinite)...")
        print("Press Ctrl+C to stop tracking")
        tracker.run_continuous_tracking()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        tracker.cleanup()


if __name__ == "__main__":
    main()
