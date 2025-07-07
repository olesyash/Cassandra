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
        self.keyspace = "birds_tracking"
        self.table = "bird_locations"

        # Bird configuration
        self.num_birds = 10
        self.updates_per_bird = 20
        self.update_interval = 60  # 1 minute in seconds

        # Bird species for variety
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

        # Initialize connection
        self.cluster = None
        self.session = None
        self.connect_to_cassandra()

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

    def create_keyspace_and_table(self):
        """Create keyspace and birds tracking table if they don't exist"""
        try:
            print(f"Creating keyspace '{self.keyspace}' if it doesn't exist...")

            # Create keyspace
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 2}}
            """)

            # Set keyspace
            self.session.set_keyspace(self.keyspace)

            # Create table
            print(f"Creating table '{self.table}' if it doesn't exist...")
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

            print("✓ Keyspace and table created successfully")

        except Exception as e:
            print(f"✗ Error creating keyspace/table: {e}")
            raise

    def insert_initial_bird_locations(self):
        """Insert initial locations for 10 birds"""
        print(f"\nInserting initial locations for {self.num_birds} birds...")

        # Prepare statement for better performance
        insert_stmt = self.session.prepare(f"""
            INSERT INTO {self.table} (bird_id, timestamp, species, latitude, longitude)
            VALUES (?, ?, ?, ?, ?)
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
                self.session.execute(
                    insert_stmt, (bird_id, timestamp, species, latitude, longitude)
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
            INSERT INTO {self.table} (bird_id, timestamp, species, latitude, longitude)
            VALUES (?, ?, ?, ?, ?)
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
                    self.session.execute(
                        update_stmt, (bird_id, timestamp, species, latitude, longitude)
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

            # Step 1: Create keyspace and table
            self.create_keyspace_and_table()

            # Step 2: Insert initial bird locations
            self.insert_initial_bird_locations()

            # Step 3: Perform periodic updates
            self.update_bird_locations()

            print("\n=== Bird Client Completed Successfully ===")

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
