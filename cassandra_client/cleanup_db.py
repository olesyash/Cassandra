from cassandra.cluster import Cluster

def cleanup_cassandra():
    print("Connecting to Cassandra cluster...")
    try:
        # Connect to Cassandra cluster
        cluster = Cluster(
            ['cassandra-1', 'cassandra-2', 'cassandra-3', 'cassandra-4'], 
            port=9042, 
            connect_timeout=10
        )
        session = cluster.connect()
        print("✓ Connected to Cassandra cluster")
        
        # Keyspace to drop
        keyspace = "trackbirds"
        
        # Check if keyspace exists
        rows = session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s",
            [keyspace]
        )
        
        if not rows.one():
            print(f"✗ Keyspace '{keyspace}' does not exist")
            return
            
        # Drop the keyspace (this will delete all data)
        print(f"\nDropping keyspace '{keyspace}'...")
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        print(f"✓ Successfully dropped keyspace '{keyspace}'")
        
    except Exception as e:
        print(f"\n✗ Error during cleanup: {e}")
        
    finally:
        # Always close the connection
        if 'session' in locals():
            session.shutdown()
        if 'cluster' in locals():
            cluster.shutdown()
        print("\nConnection closed")

if __name__ == "__main__":
    print("=== Cassandra Database Cleanup Tool ===")
    print("This will DROP the 'trackbirds' keyspace and all its data!")
    
    confirm = input("\nAre you sure you want to continue? (yes/no): ").strip().lower()
    if confirm == 'yes':
        cleanup_cassandra()
    else:
        print("\nOperation cancelled by user")
