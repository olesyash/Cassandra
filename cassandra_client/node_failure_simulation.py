#!/usr/bin/env python3
"""
Node Failure Simulation Script for Cassandra
Covers Question 3: Node Failure Simulation
"""

import subprocess
import os
import time
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class NodeFailureSimulator:
    def __init__(self):
        # Configuration
        self.cluster_hosts = ["cassandra-1", "cassandra-2", "cassandra-3", "cassandra-4"]
        self.port = 9042
        self.keyspace = "birds_tracking"
        self.table = "bird_locations"
        
        # Focus on specific bird for simulation
        self.target_bird_id = "bird_01"
        
        # Log files
        self.ring_log_file = "node_ring_analysis.log"
        self.failure_log_file = "node_failure_simulation.log"
        
        # Initialize connection
        self.cluster = None
        self.session = None
        self.initialize_logs()

    def initialize_logs(self):
        """Initialize log files for the simulation"""
        try:
            # Ring analysis log
            with open(self.ring_log_file, "w", encoding="utf-8") as f:
                f.write("=== Cassandra Node Ring Analysis Log ===\n")
                f.write(f"Target Bird: {self.target_bird_id}\n")
                f.write(f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
            
            # Failure simulation log
            with open(self.failure_log_file, "w", encoding="utf-8") as f:
                f.write("=== Node Failure Simulation Log ===\n")
                f.write(f"Target Bird: {self.target_bird_id}\n")
                f.write(f"Simulation started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
                
            print(f"✓ Log files initialized:")
            print(f"  - Ring analysis: {self.ring_log_file}")
            print(f"  - Failure simulation: {self.failure_log_file}")
            
        except Exception as e:
            print(f"✗ Failed to initialize log files: {e}")

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

    def run_nodetool_command(self, container_name, command):
        """Execute nodetool command in specified container"""
        try:
            full_command = f"docker exec {container_name} nodetool {command}"
            print(f"🔧 Running: {full_command}")
            
            result = subprocess.run(
                full_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return result.stdout
            else:
                print(f"✗ Command failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print(f"✗ Command timed out")
            return None
        except Exception as e:
            print(f"✗ Error running command: {e}")
            return None

    def inspect_token_ring(self, phase="BEFORE"):
        """3.1 & 3.4: Use nodetool ring to inspect token distribution"""
        print(f"\n{'='*60}")
        print(f"🔍 STEP: Inspecting Token Ring ({phase} Node Failure)")
        print(f"{'='*60}")
        
        # Try each container until we find one that works
        ring_output = None
        for container in self.cluster_hosts:
            container_name = f"{container.replace('cassandra-', 'cassandra-')}"
            ring_output = self.run_nodetool_command(container_name, "ring")
            if ring_output:
                break
        
        if not ring_output:
            print("✗ Failed to get ring information from any node")
            return None
        
        # Log the ring information
        try:
            with open(self.ring_log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"\n[{timestamp}] TOKEN RING STATUS - {phase} NODE FAILURE\n")
                f.write("-" * 60 + "\n")
                f.write(ring_output)
                f.write("\n" + "=" * 60 + "\n")
        except Exception as e:
            print(f"✗ Error logging ring information: {e}")
        
        print("✓ Token ring information captured")
        print("Ring Status:")
        print(ring_output)
        
        return ring_output

    def find_bird_token_and_replica(self):
        """3.2: Find specific bird's token and locate its replica"""
        print(f"\n{'='*60}")
        print(f"🔍 STEP: Finding Token for {self.target_bird_id}")
        print(f"{'='*60}")
        
        if not self.connect_to_cassandra():
            return None, None
        
        try:
            # Get the token for the bird using Cassandra's token function
            query = f"SELECT token(bird_id), bird_id FROM {self.table} WHERE bird_id = ?"
            stmt = self.session.prepare(query)
            result = self.session.execute(stmt, (self.target_bird_id,))
            row = result.one()
            
            if not row:
                print(f"✗ No data found for {self.target_bird_id}")
                return None, None
            
            bird_token = row[0]  # token(bird_id)
            print(f"✓ Token for {self.target_bird_id}: {bird_token}")
            
            # Log the token information
            with open(self.failure_log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"\n[{timestamp}] BIRD TOKEN ANALYSIS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Bird ID: {self.target_bird_id}\n")
                f.write(f"Token: {bird_token}\n")
                f.write("\n")
            
            # Get ring information to find which node owns this token
            ring_output = self.inspect_token_ring("BEFORE")
            if ring_output:
                replica_info = self.analyze_token_ownership(bird_token, ring_output)
                return bird_token, replica_info
            
            return bird_token, None
            
        except Exception as e:
            print(f"✗ Error finding bird token: {e}")
            return None, None

    def analyze_token_ownership(self, bird_token, ring_output):
        """Analyze which node owns the given token"""
        try:
            lines = ring_output.strip().split('\n')
            
            # Skip header lines and parse token ranges
            data_lines = []
            for line in lines:
                if line.strip() and not line.startswith('Address') and not line.startswith('Note:'):
                    parts = line.split()
                    if len(parts) >= 6:  # Ensure we have enough columns
                        data_lines.append(parts)
            
            if not data_lines:
                print("✗ No valid ring data found")
                return None
            
            print(f"\n🔍 Analyzing token ownership for token: {bird_token}")
            
            # Find the node that owns this token
            bird_token_int = int(bird_token)
            
            for i, parts in enumerate(data_lines):
                try:
                    # Ring output format: Address  Rack  Status  Token  Owns  Host ID  ...
                    address = parts[0]
                    status = parts[2]
                    token_end = int(parts[3])
                    owns_percentage = parts[4]
                    
                    # Get the previous token (start of range)
                    if i == 0:
                        # First entry, wraps around from the last entry
                        token_start = int(data_lines[-1][3])
                    else:
                        token_start = int(data_lines[i-1][3])
                    
                    # Check if bird token falls in this range
                    if token_start < token_end:
                        # Normal range
                        if token_start < bird_token_int <= token_end:
                            replica_info = {
                                'address': address,
                                'status': status,
                                'token_range_start': token_start,
                                'token_range_end': token_end,
                                'owns_percentage': owns_percentage
                            }
                            
                            print(f"✓ Token {bird_token} belongs to node: {address}")
                            print(f"  Status: {status}")
                            print(f"  Token range: {token_start} to {token_end}")
                            print(f"  Owns: {owns_percentage}")
                            
                            # Log the ownership information
                            with open(self.failure_log_file, "a", encoding="utf-8") as f:
                                f.write(f"TOKEN OWNERSHIP ANALYSIS:\n")
                                f.write(f"  Primary replica node: {address}\n")
                                f.write(f"  Node status: {status}\n")
                                f.write(f"  Token range: {token_start} to {token_end}\n")
                                f.write(f"  Ownership percentage: {owns_percentage}\n\n")
                            
                            return replica_info
                    else:
                        # Wrap-around range (crosses zero)
                        if bird_token_int > token_start or bird_token_int <= token_end:
                            replica_info = {
                                'address': address,
                                'status': status,
                                'token_range_start': token_start,
                                'token_range_end': token_end,
                                'owns_percentage': owns_percentage
                            }
                            
                            print(f"✓ Token {bird_token} belongs to node: {address} (wrap-around range)")
                            print(f"  Status: {status}")
                            print(f"  Token range: {token_start} to {token_end} (wraps)")
                            print(f"  Owns: {owns_percentage}")
                            
                            # Log the ownership information
                            with open(self.failure_log_file, "a", encoding="utf-8") as f:
                                f.write(f"TOKEN OWNERSHIP ANALYSIS:\n")
                                f.write(f"  Primary replica node: {address}\n")
                                f.write(f"  Node status: {status}\n")
                                f.write(f"  Token range: {token_start} to {token_end} (wrap-around)\n")
                                f.write(f"  Ownership percentage: {owns_percentage}\n\n")
                            
                            return replica_info
                            
                except (ValueError, IndexError) as e:
                    print(f"⚠️  Skipping malformed ring line: {line}")
                    continue
            
            print(f"✗ Could not find owner for token {bird_token}")
            return None
            
        except Exception as e:
            print(f"✗ Error analyzing token ownership: {e}")
            return None

    def simulate_node_failure(self, replica_info):
        """3.3: Simulate failure by stopping the node that holds the bird's token"""
        if not replica_info:
            print("✗ No replica information available for node failure simulation")
            return False
        
        print(f"\n{'='*60}")
        print(f"🚨 STEP: Simulating Node Failure")
        print(f"{'='*60}")
        
        target_address = replica_info['address']
        
        # Map IP address to container name
        container_mapping = {
            'cassandra-1': ['172.18.0.2', '127.0.0.1', 'cassandra-1'],
            'cassandra-2': ['172.18.0.3', '127.0.0.2', 'cassandra-2'],  
            'cassandra-3': ['172.18.0.4', '127.0.0.3', 'cassandra-3'],
            'cassandra-4': ['172.18.0.5', '127.0.0.4', 'cassandra-4']
        }
        
        target_container = None
        for container, addresses in container_mapping.items():
            if any(addr in target_address for addr in addresses):
                target_container = container
                break
        
        if not target_container:
            print(f"✗ Could not map address {target_address} to container")
            return False
        
        print(f"🎯 Target node: {target_address}")
        print(f"🐳 Target container: {target_container}")
        
        # Log the failure simulation
        try:
            with open(self.failure_log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"[{timestamp}] NODE FAILURE SIMULATION\n")
                f.write("-" * 40 + "\n")
                f.write(f"Target address: {target_address}\n")
                f.write(f"Target container: {target_container}\n")
                f.write(f"Reason: This node holds the primary replica for {self.target_bird_id}\n\n")
        except Exception as e:
            print(f"⚠️  Warning: Could not log failure simulation: {e}")
        
        # Stop the container
        try:
            stop_command = f"docker stop {target_container}"
            print(f"🛑 Stopping container: {stop_command}")
            
            result = subprocess.run(
                stop_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                print(f"✓ Successfully stopped {target_container}")
                
                # Wait a moment for the cluster to detect the failure
                print("⏳ Waiting 10 seconds for cluster to detect failure...")
                time.sleep(10)
                
                return True
            else:
                print(f"✗ Failed to stop container: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"✗ Stop command timed out")
            return False
        except Exception as e:
            print(f"✗ Error stopping container: {e}")
            return False

    def test_operations_after_failure(self):
        """3.5: Re-run operations and trace the results to show flow changes"""
        print(f"\n{'='*60}")
        print(f"🔍 STEP: Testing Operations After Node Failure")
        print(f"{'='*60}")
        
        # Try to reconnect (may fail to some nodes)
        if not self.connect_to_cassandra():
            print("✗ Could not reconnect to cluster after failure")
            return False
        
        try:
            # Test UPDATE operation with tracing
            print(f"🔧 Testing UPDATE operation for {self.target_bird_id}...")
            
            update_query = f"""
                INSERT INTO {self.table} (bird_id, timestamp, species, latitude, longitude)
                VALUES (?, ?, ?, ?, ?)
            """
            
            stmt = self.session.prepare(update_query)
            timestamp = datetime.now()
            
            # Execute with tracing
            result = self.session.execute(
                stmt, 
                (self.target_bird_id, timestamp, "Sparrow", 40.7500, -74.0200),
                trace=True
            )
            
            trace = result.get_query_trace()
            self.log_failure_trace("UPDATE", trace, "After node failure")
            
            print("✓ UPDATE operation completed")
            
            # Test SELECT operation with tracing
            print(f"🔧 Testing SELECT operation for {self.target_bird_id}...")
            
            select_query = f"""
                SELECT * FROM {self.table} WHERE bird_id = ? LIMIT 5
            """
            
            stmt = self.session.prepare(select_query)
            result = self.session.execute(stmt, (self.target_bird_id,), trace=True)
            
            trace = result.get_query_trace()
            self.log_failure_trace("SELECT", trace, "After node failure")
            
            rows = list(result)
            print(f"✓ SELECT operation completed - Found {len(rows)} records")
            
            return True
            
        except Exception as e:
            print(f"✗ Error testing operations after failure: {e}")
            
            # Log the error
            try:
                with open(self.failure_log_file, "a", encoding="utf-8") as f:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    f.write(f"[{timestamp}] OPERATION TEST FAILED\n")
                    f.write(f"Error: {str(e)}\n\n")
            except:
                pass
            
            return False

    def log_failure_trace(self, operation_type, trace, context):
        """Log trace information after node failure"""
        if not trace:
            return
            
        try:
            with open(self.failure_log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"\n[{timestamp}] {operation_type} TRACE - {context}\n")
                f.write("-" * 50 + "\n")
                f.write(f"Trace ID: {trace.trace_id}\n")
                f.write(f"Coordinator: {trace.coordinator}\n")
                f.write(f"Duration: {trace.duration.total_seconds() * 1000000:.0f} microseconds\n\n")
                
                f.write("OPERATION FLOW AFTER NODE FAILURE:\n")
                f.write("-" * 40 + "\n")
                
                if hasattr(trace, 'events') and trace.events:
                    for i, event in enumerate(trace.events, 1):
                        source = getattr(event, 'source', 'Unknown')
                        source_elapsed_raw = getattr(event, 'source_elapsed', 0)
                        activity = getattr(event, 'description', 'Unknown activity')
                        
                        # Convert source_elapsed to microseconds if it's a timedelta
                        if hasattr(source_elapsed_raw, 'total_seconds'):
                            source_elapsed = int(source_elapsed_raw.total_seconds() * 1000000)
                        else:
                            source_elapsed = int(source_elapsed_raw) if source_elapsed_raw else 0
                        
                        f.write(f"  Step {i:2d}: [{source_elapsed:8d} μs] {source}\n")
                        f.write(f"           Activity: {activity}\n")
                        
                        # Check if this is the coordinator
                        if source == str(trace.coordinator):
                            f.write(f"           >>> COORDINATOR (AFTER FAILURE) <<<\n")
                        else:
                            f.write(f"           >>> REPLICA (AFTER FAILURE) <<<\n")
                        f.write("\n")
                else:
                    f.write("  No detailed trace events available\n")
                
                f.write("=" * 50 + "\n\n")
                
        except Exception as e:
            print(f"✗ Error logging failure trace: {e}")

    def restart_failed_node(self, replica_info):
        """Restart the failed node (optional cleanup step)"""
        if not replica_info:
            return False
        
        target_address = replica_info['address']
        
        # Map IP address to container name (same logic as before)
        container_mapping = {
            'cassandra-1': ['172.18.0.2', '127.0.0.1', 'cassandra-1'],
            'cassandra-2': ['172.18.0.3', '127.0.0.2', 'cassandra-2'],  
            'cassandra-3': ['172.18.0.4', '127.0.0.3', 'cassandra-3'],
            'cassandra-4': ['172.18.0.5', '127.0.0.4', 'cassandra-4']
        }
        
        target_container = None
        for container, addresses in container_mapping.items():
            if any(addr in target_address for addr in addresses):
                target_container = container
                break
        
        if not target_container:
            return False
        
        try:
            restart_command = f"docker start {target_container}"
            print(f"🔄 Restarting container: {restart_command}")
            
            result = subprocess.run(
                restart_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                print(f"✓ Successfully restarted {target_container}")
                print("⏳ Waiting 15 seconds for node to rejoin cluster...")
                time.sleep(15)
                return True
            else:
                print(f"✗ Failed to restart container: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"✗ Error restarting container: {e}")
            return False

    def run_complete_simulation(self):
        """Run the complete node failure simulation"""
        print("🚀 Starting Complete Node Failure Simulation")
        print("=" * 70)
        
        try:
            # Step 3.1: Inspect initial token ring
            print("\n📋 PHASE 1: Initial Token Ring Analysis")
            initial_ring = self.inspect_token_ring("BEFORE")
            if not initial_ring:
                print("✗ Failed to get initial ring information")
                return False
            
            # Step 3.2: Find bird token and replica
            print("\n🔍 PHASE 2: Token and Replica Analysis")
            bird_token, replica_info = self.find_bird_token_and_replica()
            if not bird_token or not replica_info:
                print("✗ Failed to find bird token or replica information")
                return False
            
            # Step 3.3: Simulate node failure
            print("\n💥 PHASE 3: Node Failure Simulation")
            failure_success = self.simulate_node_failure(replica_info)
            if not failure_success:
                print("✗ Failed to simulate node failure")
                return False
            
            # Step 3.4: Re-inspect token ring after failure
            print("\n📋 PHASE 4: Token Ring After Failure")
            failure_ring = self.inspect_token_ring("AFTER")
            if not failure_ring:
                print("⚠️  Could not get ring information after failure (expected)")
            
            # Step 3.5: Test operations and trace flow
            print("\n🔧 PHASE 5: Operation Testing After Failure")
            operation_success = self.test_operations_after_failure()
            
            # Optional: Restart the failed node
            print("\n🔄 PHASE 6: Node Recovery (Optional)")
            user_input = input("Do you want to restart the failed node? (y/N): ").strip().lower()
            if user_input == 'y':
                restart_success = self.restart_failed_node(replica_info)
                if restart_success:
                    # Final ring inspection
                    print("\n📋 FINAL: Token Ring After Recovery")
                    self.inspect_token_ring("AFTER_RECOVERY")
            
            print("\n" + "=" * 70)
            print("🎉 Node Failure Simulation Complete!")
            print(f"📁 Check these files for detailed analysis:")
            print(f"   - Ring analysis: {self.ring_log_file}")
            print(f"   - Failure simulation: {self.failure_log_file}")
            
            return True
            
        except Exception as e:
            print(f"✗ Simulation failed: {e}")
            return False
        finally:
            if self.session:
                self.session.shutdown()
            if self.cluster:
                self.cluster.shutdown()

    def cleanup(self):
        """Clean up connections"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()


def main():
    """Main function"""
    print("🚀 Cassandra Node Failure Simulation")
    print("This script implements Question 3: Node Failure Simulation")
    print("=" * 70)
    
    simulator = NodeFailureSimulator()
    
    try:
        success = simulator.run_complete_simulation()
        if success:
            print("\n✅ Simulation completed successfully!")
        else:
            print("\n❌ Simulation encountered errors!")
            
    except KeyboardInterrupt:
        print("\n\n🛑 Simulation interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
    finally:
        simulator.cleanup()


if __name__ == "__main__":
    main() 