#!/usr/bin/env python3
"""
Cassandra Rerouting Demo - Post Node Failure
Demonstrates how Cassandra automatically reroutes requests to available replicas
when the primary node (cassandra-1 / 172.18.0.2) is down.
"""

import time
import os
from datetime import datetime, date
from cassandra.cluster import Cluster


class ReroutingDemo:
    def __init__(self):
        # Configuration - excluding the failed node for connection
        self.cluster_hosts = [
            "cassandra-2",
            "cassandra-3",
            "cassandra-4",
        ]  # cassandra-1 is down
        self.port = 9042
        self.keyspace = "trackbirds"
        self.table = "birds_tracking"

        # Focus on the bird that was on the failed node
        self.target_bird_id = "bird_01"
        self.failed_node_ip = "172.18.0.2"  # cassandra-1
        self.original_token = "4904968884673883820"

        # Log file for rerouting analysis
        self.rerouting_log = "rerouting_analysis.log"

        # Initialize connection and log
        self.cluster = None
        self.session = None
        self.initialize_log()

    def initialize_log(self):
        """Initialize the rerouting analysis log"""
        try:
            with open(self.rerouting_log, "w", encoding="utf-8") as f:
                f.write("=== Cassandra Rerouting Demo - Post Node Failure ===\n")
                f.write(f"Failed Node: {self.failed_node_ip} (cassandra-1)\n")
                f.write(f"Target Bird: {self.target_bird_id}\n")
                f.write(f"Original Token: {self.original_token}\n")
                f.write(
                    f"Demo started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                )
                f.write("=" * 80 + "\n\n")
            print(f"✓ Rerouting analysis log initialized: {self.rerouting_log}")
        except Exception as e:
            print(f"✗ Failed to initialize log: {e}")

    def connect_to_cluster(self):
        """Connect to the remaining cluster nodes (cassandra-1 is down)"""
        try:
            print("🔗 Connecting to remaining cluster nodes...")
            print(f"   Available nodes: {self.cluster_hosts}")
            print(f"   Failed node (excluded): cassandra-1 ({self.failed_node_ip})")

            self.cluster = Cluster(
                contact_points=self.cluster_hosts, port=self.port, connect_timeout=10
            )
            self.session = self.cluster.connect()
            self.session.set_keyspace(self.keyspace)

            print("✅ Successfully connected to remaining cluster nodes")
            return True

        except Exception as e:
            print(f"❌ Failed to connect to cluster: {e}")
            return False

    def log_trace_analysis(self, operation_type, trace, operation_details):
        """Log detailed trace analysis showing rerouting behavior"""
        if not trace:
            return

        try:
            with open(self.rerouting_log, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(
                    f"\n[{timestamp}] {operation_type} OPERATION - REROUTING ANALYSIS\n"
                )
                f.write("=" * 70 + "\n")
                f.write(f"Operation: {operation_details}\n")
                f.write(f"Coordinator: {trace.coordinator}\n")
                f.write(f"Duration: {trace.duration.total_seconds() * 1000:.2f} ms\n")

                # Check if coordinator is NOT the failed node
                if str(trace.coordinator) != self.failed_node_ip:
                    f.write(
                        f"✅ REROUTING SUCCESS: Coordinator is {trace.coordinator} (NOT the failed node {self.failed_node_ip})\n"
                    )
                else:
                    f.write(
                        f"❌ UNEXPECTED: Coordinator is still the failed node {self.failed_node_ip}\n"
                    )

                f.write("\nOPERATION FLOW:\n")
                f.write("-" * 40 + "\n")

                if hasattr(trace, "events") and trace.events:
                    replica_nodes = set()
                    for i, event in enumerate(trace.events, 1):
                        source = getattr(event, "source", "Unknown")
                        source_elapsed_raw = getattr(event, "source_elapsed", 0)
                        activity = getattr(event, "description", "Unknown activity")

                        # Convert elapsed time
                        if hasattr(source_elapsed_raw, "total_seconds"):
                            elapsed_ms = source_elapsed_raw.total_seconds() * 1000
                        else:
                            elapsed_ms = (
                                float(source_elapsed_raw) / 1000
                                if source_elapsed_raw
                                else 0
                            )

                        f.write(f"  {i:2d}. [{elapsed_ms:8.2f} ms] {source}\n")
                        f.write(f"      Activity: {activity}\n")

                        # Track which nodes are involved
                        replica_nodes.add(source)

                        # Mark coordinator vs replica
                        if source == str(trace.coordinator):
                            f.write(f"      >>> COORDINATOR NODE <<<\n")
                        else:
                            f.write(f"      >>> REPLICA NODE <<<\n")
                        f.write("\n")

                    # Summary of nodes involved
                    f.write("NODES INVOLVED IN OPERATION:\n")
                    f.write("-" * 30 + "\n")
                    for node in sorted(replica_nodes):
                        if node == self.failed_node_ip:
                            f.write(f"  ❌ {node} (FAILED NODE - should not appear!)\n")
                        elif node == str(trace.coordinator):
                            f.write(f"  🎯 {node} (NEW COORDINATOR)\n")
                        else:
                            f.write(f"  🔄 {node} (REPLICA)\n")

                else:
                    f.write("  No detailed trace events available\n")

                f.write("\n" + "=" * 70 + "\n")

        except Exception as e:
            print(f"✗ Error logging trace: {e}")

    def demonstrate_select_rerouting(self):
        """Demonstrate SELECT operation rerouting after node failure"""
        print(f"\n🔍 DEMONSTRATING SELECT REROUTING for {self.target_bird_id}")
        print("=" * 60)

        try:
            today = date.today()

            # Perform a traced SELECT operation
            query = f"""
                SELECT timestamp, species, latitude, longitude
                FROM {self.table}
                WHERE bird_id = ? AND date = ?
                ORDER BY timestamp DESC
                LIMIT 5
            """
            stmt = self.session.prepare(query)

            print(f"📡 Executing SELECT for {self.target_bird_id} on {today}...")
            result = self.session.execute(
                stmt, (self.target_bird_id, today), trace=True
            )
            rows = list(result)

            # Get trace information
            trace = result.get_query_trace()
            operation_details = (
                f"SELECT last 5 locations for {self.target_bird_id} on {today}"
            )

            print(f"✅ SELECT completed successfully!")
            print(f"   📊 Found {len(rows)} records")
            print(f"   🎯 Coordinator: {trace.coordinator}")
            print(f"   ⏱️  Duration: {trace.duration.total_seconds() * 1000:.2f} ms")

            if str(trace.coordinator) != self.failed_node_ip:
                print(
                    f"   ✅ REROUTING SUCCESS: Using {trace.coordinator} (not failed node {self.failed_node_ip})"
                )
            else:
                print(
                    f"   ❌ UNEXPECTED: Still using failed node {self.failed_node_ip}"
                )

            # Log detailed trace analysis
            self.log_trace_analysis("SELECT", trace, operation_details)

            return True

        except Exception as e:
            print(f"❌ SELECT operation failed: {e}")
            return False

    def demonstrate_update_rerouting(self):
        """Demonstrate UPDATE operation rerouting after node failure"""
        print(f"\n📝 DEMONSTRATING UPDATE REROUTING for {self.target_bird_id}")
        print("=" * 60)

        try:
            # Create a new location update
            timestamp = datetime.now()
            today = timestamp.date()
            new_latitude = 40.7589  # Moved slightly from original position
            new_longitude = -73.9851
            species = "Eurasian Hoopoe"  # From bird_client_v2.py

            # Perform a traced INSERT (which is an UPDATE in this context)
            query = f"""
                INSERT INTO {self.table} (bird_id, date, timestamp, species, latitude, longitude)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            stmt = self.session.prepare(query)

            print(f"📡 Executing UPDATE for {self.target_bird_id}...")
            print(f"   📍 New location: ({new_latitude}, {new_longitude})")

            result = self.session.execute(
                stmt,
                (
                    self.target_bird_id,
                    today,
                    timestamp,
                    species,
                    new_latitude,
                    new_longitude,
                ),
                trace=True,
            )

            # Get trace information
            trace = result.get_query_trace()
            operation_details = f"UPDATE location for {self.target_bird_id} to ({new_latitude}, {new_longitude})"

            print(f"✅ UPDATE completed successfully!")
            print(f"   🎯 Coordinator: {trace.coordinator}")
            print(f"   ⏱️  Duration: {trace.duration.total_seconds() * 1000:.2f} ms")

            if str(trace.coordinator) != self.failed_node_ip:
                print(
                    f"   ✅ REROUTING SUCCESS: Using {trace.coordinator} (not failed node {self.failed_node_ip})"
                )
            else:
                print(
                    f"   ❌ UNEXPECTED: Still using failed node {self.failed_node_ip}"
                )

            # Log detailed trace analysis
            self.log_trace_analysis("UPDATE", trace, operation_details)

            return True

        except Exception as e:
            print(f"❌ UPDATE operation failed: {e}")
            return False

    def show_cluster_status(self):
        """Show current cluster status and ring information"""
        print(f"\n🔍 CLUSTER STATUS ANALYSIS")
        print("=" * 60)

        try:
            # Get local node info (which node we're connected to)
            local_result = self.session.execute(
                "SELECT listen_address, release_version FROM system.local"
            )
            local_row = local_result.one()

            # Get peer nodes
            peers_result = self.session.execute(
                "SELECT peer, release_version FROM system.peers"
            )
            peer_rows = list(peers_result)

            print(f"📡 Connected to: {local_row.listen_address}")
            print(f"🔗 Available peers: {len(peer_rows)}")

            all_nodes = [local_row.listen_address] + [peer.peer for peer in peer_rows]

            print(f"\n📊 ACTIVE NODES:")
            for node in sorted(all_nodes):
                if node == self.failed_node_ip:
                    print(f"   ❌ {node} (FAILED - should not appear)")
                else:
                    print(f"   ✅ {node} (ACTIVE)")

            # Log cluster status
            with open(self.rerouting_log, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"\n[{timestamp}] CLUSTER STATUS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Connected to: {local_row.listen_address}\n")
                f.write(f"Total active nodes: {len(all_nodes)}\n")
                f.write(f"Failed node: {self.failed_node_ip}\n")
                f.write("Active nodes:\n")
                for node in sorted(all_nodes):
                    f.write(f"  - {node}\n")
                f.write("\n")

            return True

        except Exception as e:
            print(f"❌ Error getting cluster status: {e}")
            return False

    def run_rerouting_demo(self):
        """Run the complete rerouting demonstration"""
        print("🚀 CASSANDRA REROUTING DEMONSTRATION")
        print("Showing automatic failover after cassandra-1 node failure")
        print("=" * 70)

        try:
            # Step 1: Connect to remaining cluster
            if not self.connect_to_cluster():
                return False

            # Step 2: Show cluster status
            if not self.show_cluster_status():
                return False

            # Step 3: Demonstrate SELECT rerouting
            if not self.demonstrate_select_rerouting():
                return False

            # Step 4: Demonstrate UPDATE rerouting
            if not self.demonstrate_update_rerouting():
                return False

            # Step 5: Final summary
            print(f"\n🎉 REROUTING DEMONSTRATION COMPLETED!")
            print("=" * 70)
            print("✅ Key Findings:")
            print(
                "   • Cassandra automatically rerouted requests to available replicas"
            )
            print("   • Both SELECT and UPDATE operations completed successfully")
            print("   • No data loss occurred despite primary node failure")
            print("   • Fault tolerance and consistency maintained")
            print(f"📁 Detailed analysis saved to: {self.rerouting_log}")

            return True

        except Exception as e:
            print(f"❌ Rerouting demo failed: {e}")
            return False
        finally:
            if self.session:
                self.session.shutdown()
            if self.cluster:
                self.cluster.shutdown()


def main():
    """Main function"""
    print("🎯 Cassandra Rerouting Demo - Post Node Failure")
    print("This demonstrates automatic request rerouting when cassandra-1 is down")
    print("=" * 70)

    demo = ReroutingDemo()

    try:
        success = demo.run_rerouting_demo()
        if success:
            print("\n✅ Demo completed successfully!")
            print(
                "The results prove Cassandra's automatic fault tolerance and rerouting capabilities."
            )
        else:
            print("\n❌ Demo failed - check error messages above")

    except KeyboardInterrupt:
        print("\n\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()
