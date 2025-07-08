#!/usr/bin/env python3
"""
Node Analysis Script for Cassandra - Question 3
Finds where bird_01 data is located and provides manual instructions
"""

import os
import time
from datetime import datetime
from cassandra.cluster import Cluster


class NodeAnalyzer:
    def __init__(self):
        # Configuration
        self.cluster_hosts = [
            "cassandra-1",
            "cassandra-2",
            "cassandra-3",
            "cassandra-4",
        ]
        self.port = 9042
        self.keyspace = "trackbirds"
        self.table = "birds_tracking"

        # Focus on specific bird for analysis
        self.target_bird_id = "bird_01"

        # Log file
        self.analysis_log_file = "node_analysis.log"

        # Initialize connection
        self.cluster = None
        self.session = None
        self.initialize_log()

    def initialize_log(self):
        """Initialize log file for the analysis"""
        try:
            with open(self.analysis_log_file, "w", encoding="utf-8") as f:
                f.write("=== Cassandra Node Analysis for Question 3 ===\n")
                f.write(f"Target Bird: {self.target_bird_id}\n")
                f.write(
                    f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                )
                f.write("=" * 80 + "\n\n")
            print(f"✓ Log file initialized: {self.analysis_log_file}")
        except Exception as e:
            print(f"✗ Failed to initialize log file: {e}")

    def connect_to_cassandra(self):
        """Connect to Cassandra cluster"""
        try:
            print("Connecting to Cassandra cluster...")
            self.cluster = Cluster(
                contact_points=self.cluster_hosts, port=self.port, connect_timeout=10
            )
            self.session = self.cluster.connect()
            self.session.set_keyspace(self.keyspace)
            print(f"✓ Connected to Cassandra cluster")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to Cassandra: {e}")
            return False

    def get_cluster_info(self):
        """Get basic cluster information"""
        print(f"\n{'=' * 60}")
        print(f"🔍 STEP 1: Getting Cluster Information")
        print(f"{'=' * 60}")

        try:
            # Get local node info
            local_result = self.session.execute("SELECT * FROM system.local")
            local_row = local_result.one()

            # Get peer nodes info
            peers_result = self.session.execute("SELECT * FROM system.peers")
            peer_rows = list(peers_result)

            print(f"✓ Connected to local node: {local_row.listen_address}")
            print(f"✓ Found {len(peer_rows)} peer nodes")

            # Log cluster info
            with open(self.analysis_log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] CLUSTER INFORMATION\n")
                f.write("-" * 40 + "\n")
                f.write(f"Local Node: {local_row.listen_address}\n")
                f.write(f"Cluster Name: {local_row.cluster_name}\n")
                f.write(f"Cassandra Version: {local_row.release_version}\n")
                f.write(f"Total Nodes: {len(peer_rows) + 1}\n")
                f.write("\nPeer Nodes:\n")
                for peer in peer_rows:
                    f.write(f"  - {peer.peer}\n")
                f.write("\n")

            return True

        except Exception as e:
            print(f"✗ Error getting cluster info: {e}")
            return False

    def find_bird_token_and_location(self):
        """Find bird_01's token and determine its location"""
        print(f"\n{'=' * 60}")
        print(f"🔍 STEP 2: Finding {self.target_bird_id} Token and Location")
        print(f"{'=' * 60}")

        try:
            # Check if bird data exists - use ALLOW FILTERING for partial partition key
            check_query = (
                f"SELECT COUNT(*) FROM {self.table} WHERE bird_id = ? ALLOW FILTERING"
            )
            stmt = self.session.prepare(check_query)
            result = self.session.execute(stmt, (self.target_bird_id,))
            count = result.one()[0]

            if count == 0:
                print(f"✗ No data found for {self.target_bird_id}")
                print(f"💡 Run 'python bird_client_v2.py' first to populate data")
                return None

            print(f"✓ Found {count} records for {self.target_bird_id}")

            # Get the token for the bird using Cassandra's token function
            # Since we have composite partition key (bird_id, date), we need to get a specific record
            # Let's get the most recent record for today or use ALLOW FILTERING
            from datetime import date as date_type

            today = date_type.today()

            # First try with today's date
            token_query = f"SELECT token(bird_id, date), bird_id, date FROM {self.table} WHERE bird_id = ? AND date = ? LIMIT 1"
            stmt = self.session.prepare(token_query)
            result = self.session.execute(stmt, (self.target_bird_id, today))
            row = result.one()

            # If no data for today, use ALLOW FILTERING to get any record
            if not row:
                print(
                    f"No data for {self.target_bird_id} on {today}, checking all dates..."
                )
                token_query = f"SELECT token(bird_id, date), bird_id, date FROM {self.table} WHERE bird_id = ? ALLOW FILTERING LIMIT 1"
                stmt = self.session.prepare(token_query)
                result = self.session.execute(stmt, (self.target_bird_id,))
                row = result.one()

            if not row:
                print(f"✗ Could not get token for {self.target_bird_id}")
                return None

            bird_token = row[0]  # token(bird_id, date)
            bird_date = row[2]  # date column
            print(
                f"✓ Token for {self.target_bird_id} (date: {bird_date}): {bird_token}"
            )

            # Log the token information
            with open(self.analysis_log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] BIRD TOKEN ANALYSIS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Bird ID: {self.target_bird_id}\n")
                f.write(f"Date: {bird_date}\n")
                f.write(f"Token: {bird_token}\n")
                f.write(f"Record count: {count}\n\n")

            return bird_token

        except Exception as e:
            print(f"✗ Error finding bird token: {e}")
            return None

    def analyze_token_ranges(self, bird_token):
        """Analyze token ranges to determine which node owns the bird's data"""
        print(f"\n{'=' * 60}")
        print(f"🔍 STEP 3: Analyzing Token Ranges")
        print(f"{'=' * 60}")

        try:
            # Get token ring information from system tables
            ring_query = """
                SELECT peer, tokens 
                FROM system.peers
            """

            peers_result = self.session.execute(ring_query)
            peer_rows = list(peers_result)

            # Get local node token info
            local_query = "SELECT listen_address, tokens FROM system.local"
            local_result = self.session.execute(local_query)
            local_row = local_result.one()

            print(f"✓ Analyzing token ownership...")

            # Combine all nodes and their tokens
            all_nodes = []

            # Add local node
            if local_row and local_row.tokens:
                for token in local_row.tokens:
                    all_nodes.append(
                        {
                            "address": local_row.listen_address,
                            "token": int(token),
                            "is_local": True,
                        }
                    )

            # Add peer nodes
            for peer in peer_rows:
                if peer.tokens:
                    for token in peer.tokens:
                        all_nodes.append(
                            {
                                "address": peer.peer,
                                "token": int(token),
                                "is_local": False,
                            }
                        )

            # Sort by token value
            all_nodes.sort(key=lambda x: x["token"])

            print(
                f"✓ Found {len(all_nodes)} token ranges across {len(peer_rows) + 1} nodes"
            )

            # Find which node owns the bird's token
            bird_token_int = int(bird_token)
            owner_node = None

            for i, node in enumerate(all_nodes):
                # Get the previous token (start of this node's range)
                prev_token = (
                    all_nodes[i - 1]["token"] if i > 0 else all_nodes[-1]["token"]
                )
                current_token = node["token"]

                # Check if bird token falls in this range
                if prev_token < current_token:
                    # Normal range
                    if prev_token < bird_token_int <= current_token:
                        owner_node = node
                        break
                else:
                    # Wrap-around range (crosses the ring)
                    if bird_token_int > prev_token or bird_token_int <= current_token:
                        owner_node = node
                        break

            if owner_node:
                print(
                    f"✅ FOUND: {self.target_bird_id} data is owned by node: {owner_node['address']}"
                )

                # Map IP to container name for easier reference
                container_mapping = {
                    "172.18.0.2": "cassandra-1",
                    "172.18.0.3": "cassandra-2",
                    "172.18.0.4": "cassandra-3",
                    "172.18.0.5": "cassandra-4",
                }

                container_name = container_mapping.get(owner_node["address"], "unknown")

                print(f"📦 Container: {container_name}")
                print(f"🎯 Token Range: ...{owner_node['token']}")

                # Log the ownership information
                with open(self.analysis_log_file, "a", encoding="utf-8") as f:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"[{timestamp}] TOKEN OWNERSHIP ANALYSIS\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Primary replica node: {owner_node['address']}\n")
                    f.write(f"Container name: {container_name}\n")
                    f.write(f"Token range end: {owner_node['token']}\n")
                    f.write(f"Bird token: {bird_token}\n\n")

                return owner_node, container_name
            else:
                print(f"✗ Could not determine owner for token {bird_token}")
                return None, None

        except Exception as e:
            print(f"✗ Error analyzing token ranges: {e}")
            return None, None

    def provide_manual_instructions(self, owner_node, container_name, bird_token):
        """Provide step-by-step manual instructions for Question 3"""
        print(f"\n{'=' * 80}")
        print(f"📋 MANUAL INSTRUCTIONS for Question 3: Node Failure Simulation")
        print(f"{'=' * 80}")

        instructions = f"""
🎯 TARGET NODE IDENTIFIED: {container_name} ({owner_node["address"]})

📝 STEP-BY-STEP INSTRUCTIONS:

3.1 📊 INSPECT TOKEN RING (Before Failure):
    Run this command to see the current ring status:
    
    docker exec {container_name} nodetool ring
    
    ✏️  Copy the output and save it as "ring_before_failure.txt"

3.2 🔍 BIRD TOKEN ANALYSIS (Already Done):
    ✅ Bird: {self.target_bird_id}
    ✅ Token: {bird_token}
    ✅ Primary replica: {owner_node["address"]} ({container_name})

3.3 💥 SIMULATE NODE FAILURE:
    Stop the node that owns {self.target_bird_id}'s data:
    
    docker stop {container_name}
    
    ⏳ Wait 10-15 seconds for the cluster to detect the failure

3.4 📊 INSPECT TOKEN RING (After Failure):
    Check ring status from a different node:
    
    docker exec cassandra-2 nodetool ring
    (or use cassandra-3/cassandra-4 if cassandra-2 was stopped)
    
    ✏️  Copy the output and save it as "ring_after_failure.txt"
    📝 Compare with the "before" output - notice the missing node!

3.5 🧪 TEST OPERATIONS (After Failure):
    A) Test INSERT operation:
       docker exec cassandra-client python bird_client_v2.py
       
    B) Test SELECT operation:
       docker exec cassandra-client python tracker_client_v2.py
       
    📝 Check if operations still work and examine trace logs for coordinator changes

🔄 OPTIONAL - RECOVERY:
    Restart the failed node:
    
    docker start {container_name}
    
    ⏳ Wait 15-30 seconds, then check ring status:
    docker exec {container_name} nodetool ring

📁 SAVE YOUR RESULTS:
    - ring_before_failure.txt
    - ring_after_failure.txt  
    - Screenshots of trace logs showing coordinator changes
    - Analysis log: {self.analysis_log_file}
"""

        print(instructions)

        # Log the instructions
        with open(self.analysis_log_file, "a", encoding="utf-8") as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] MANUAL INSTRUCTIONS GENERATED\n")
            f.write("-" * 40 + "\n")
            f.write(instructions)
            f.write("\n" + "=" * 80 + "\n")

        return True

    def run_analysis(self):
        """Run the complete analysis"""
        print("🚀 Starting Node Analysis for Question 3")
        print("=" * 70)

        try:
            # Connect to cluster
            if not self.connect_to_cassandra():
                return False

            # Get cluster information
            if not self.get_cluster_info():
                return False

            # Find bird token
            bird_token = self.find_bird_token_and_location()
            if not bird_token:
                return False

            # Analyze token ranges
            owner_node, container_name = self.analyze_token_ranges(bird_token)
            if not owner_node:
                return False

            # Provide manual instructions
            self.provide_manual_instructions(owner_node, container_name, bird_token)

            print("\n" + "=" * 70)
            print("✅ Analysis Complete!")
            print(f"📁 Detailed log saved to: {self.analysis_log_file}")
            print("📋 Follow the manual instructions above for Question 3")

            return True

        except Exception as e:
            print(f"✗ Analysis failed: {e}")
            return False
        finally:
            if self.session:
                self.session.shutdown()
            if self.cluster:
                self.cluster.shutdown()


def main():
    """Main function"""
    print("🔍 Cassandra Node Analysis Tool")
    print("This script finds where bird_01 is located and provides Q3 instructions")
    print("=" * 70)

    analyzer = NodeAnalyzer()

    try:
        success = analyzer.run_analysis()
        if success:
            print("\n✅ Use the instructions above to complete Question 3!")
        else:
            print("\n❌ Analysis failed - check error messages above")

    except KeyboardInterrupt:
        print("\n\n🛑 Analysis interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()
