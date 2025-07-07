#!/usr/bin/env python3
"""
Quick Node Failure Demo for Question 3
Simple wrapper to demonstrate the node failure simulation
"""

import time
import sys
from node_failure_simulation import NodeFailureSimulator


def quick_demo():
    """Run a quick demonstration of node failure simulation"""
    print("🚀 Quick Node Failure Simulation Demo")
    print("=" * 50)
    print("This demo implements Question 3 requirements:")
    print("3.1 - Token ring inspection")
    print("3.2 - Bird token and replica analysis") 
    print("3.3 - Node failure simulation")
    print("3.4 - Ring analysis after failure")
    print("3.5 - Operation tracing after failure")
    print("=" * 50)
    
    # Ask user if they want to proceed
    response = input("\nProceed with simulation? (y/N): ").strip().lower()
    if response != 'y':
        print("Demo cancelled.")
        return
    
    # Create simulator
    simulator = NodeFailureSimulator()
    
    try:
        print("\n🔍 Phase 1: Analyzing token ring...")
        ring_output = simulator.inspect_token_ring("DEMO_BEFORE")
        if not ring_output:
            print("❌ Could not get ring information. Is Cassandra running?")
            print("Run: docker-compose up -d")
            return
        
        print("\n🔍 Phase 2: Finding bird token...")
        bird_token, replica_info = simulator.find_bird_token_and_replica()
        if not bird_token:
            print("❌ Could not find bird token. Is bird data available?")
            print("Run: python bird_client_v2.py")
            return
        
        print(f"\n✅ Found token {bird_token} for {simulator.target_bird_id}")
        print(f"✅ Primary replica: {replica_info['address'] if replica_info else 'Unknown'}")
        
        # Ask before proceeding with failure
        response = input(f"\nProceed with stopping node {replica_info['address'] if replica_info else 'Unknown'}? (y/N): ").strip().lower()
        if response != 'y':
            print("Stopping demo before node failure.")
            return
        
        print("\n💥 Phase 3: Simulating node failure...")
        failure_success = simulator.simulate_node_failure(replica_info)
        if not failure_success:
            print("❌ Failed to simulate node failure")
            return
        
        print("\n🔍 Phase 4: Checking ring after failure...")
        simulator.inspect_token_ring("DEMO_AFTER")
        
        print("\n🧪 Phase 5: Testing operations after failure...")
        simulator.test_operations_after_failure()
        
        print("\n🎉 Demo completed successfully!")
        print(f"📁 Check these files for detailed results:")
        print(f"   - {simulator.ring_log_file}")
        print(f"   - {simulator.failure_log_file}")
        
        # Ask about recovery
        response = input(f"\nRestart the failed node? (y/N): ").strip().lower()
        if response == 'y':
            print("\n🔄 Restarting failed node...")
            simulator.restart_failed_node(replica_info)
            simulator.inspect_token_ring("DEMO_RECOVERY")
            print("✅ Node recovery completed!")
        
    except KeyboardInterrupt:
        print("\n\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
    finally:
        simulator.cleanup()


def show_manual_commands():
    """Show manual commands for each step"""
    print("📋 Manual Commands for Question 3:")
    print("=" * 40)
    print()
    print("Step 3.1 - Inspect token ring:")
    print("  docker exec cassandra-1 nodetool ring")
    print()
    print("Step 3.2 - Find bird token:")
    print("  # Use the simulation script or manual CQL:")
    print("  # SELECT token(bird_id) FROM bird_locations WHERE bird_id = 'bird_01';")
    print()
    print("Step 3.3 - Simulate failure:")
    print("  docker stop cassandra-1  # (replace with actual target)")
    print()
    print("Step 3.4 - Re-inspect ring:")
    print("  docker exec cassandra-2 nodetool ring")
    print()
    print("Step 3.5 - Test operations:")
    print("  python bird_client_v2.py")
    print("  python tracker_client_v2.py")
    print()


def main():
    """Main function with options"""
    if len(sys.argv) > 1 and sys.argv[1] == "--manual":
        show_manual_commands()
        return
    
    print("Question 3: Node Failure Simulation")
    print("Choose an option:")
    print("1. Run automated demo")
    print("2. Run full simulation")
    print("3. Show manual commands")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        quick_demo()
    elif choice == "2":
        simulator = NodeFailureSimulator()
        simulator.run_complete_simulation()
    elif choice == "3":
        show_manual_commands()
    else:
        print("Invalid choice. Running automated demo...")
        quick_demo()


if __name__ == "__main__":
    main() 