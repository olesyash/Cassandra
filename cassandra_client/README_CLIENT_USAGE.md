# Bird and Tracker Clients Usage Guide

This document explains how to use the new Cassandra client applications for bird tracking.

## Available Clients

### 1. Simple Execution Clients

#### Bird Client (`bird_client_v2.py`)
- **Purpose**: Creates birds tracking table, inserts initial bird locations, and performs periodic updates
- **Features**:
  - Creates keyspace `birds_tracking` and table `bird_locations`
  - Inserts initial locations for 10 birds
  - Performs 20 updates per bird with 1-minute intervals
  - Uses prepared statements for better performance
  - Comprehensive logging and error handling

**Usage**:
```bash
python bird_client_v2.py
```

#### Tracker Client (`tracker_client_v2.py`)
- **Purpose**: Queries bird locations periodically and writes results to log file
- **Features**:
  - Queries all birds to derive last known location
  - Periodic queries (configurable interval)
  - Writes detailed logs to `bird_tracking_log.txt`
  - Multiple execution modes: single query, timed duration, or continuous

**Usage**:
```bash
python tracker_client_v2.py
```

Choose from:
1. Single query (run once)
2. Continuous tracking for specific duration
3. Continuous tracking (indefinite)

### 2. Combined Threaded Client

#### Combined Client (`combined_threaded_client.py`)
- **Purpose**: Uses queues and threads where each thread runs different workloads
- **Features**:
  - **Bird Worker Thread**: Handles bird location inserts and updates
  - **Tracker Worker Thread**: Handles periodic queries and logging
  - Thread-safe operations using queues
  - Coordinated shutdown and cleanup

**Usage**:
```bash
python combined_threaded_client.py
```

## Requirements

Before running any client, ensure:

1. **Cassandra Cluster is Running**:
   ```bash
   docker-compose up -d
   ```

2. **Dependencies Installed**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Variables** (optional):
   - `CASSANDRA_HOSTS`: Comma-separated list of Cassandra hosts
   - Default: `cassandra-1,cassandra-2,cassandra-3,cassandra-4`

## Configuration

### Bird Client Configuration
- **Number of Birds**: 10 (bird_01 to bird_10)
- **Updates per Bird**: 20
- **Update Interval**: 60 seconds (1 minute)
- **Species**: Rotating list of 10 bird species

### Tracker Client Configuration
- **Query Interval**: 30 seconds (configurable)
- **Log File**: `bird_tracking_log.txt`
- **Query Scope**: All 10 birds

### Combined Client Configuration
- **Bird Update Interval**: 60 seconds
- **Tracker Query Interval**: 30 seconds
- **Log File**: `combined_bird_tracking_log.txt`

## Database Schema

### Keyspace: `birds_tracking`
- **Replication**: SimpleStrategy with factor 2
- **Consistency**: Default consistency level

### Table: `bird_locations`
```cql
CREATE TABLE bird_locations (
    bird_id TEXT,
    timestamp TIMESTAMP,
    species TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    PRIMARY KEY (bird_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

## Log Files

### Bird Tracking Log (`bird_tracking_log.txt`)
Format:
```
[2024-01-15 10:30:00] Tracking Query Results:
  bird_01 | Sparrow | Last: (40.7128, -74.0060) at 2024-01-15 10:29:45 | Total locations: 5
  bird_02 | Eagle | Last: (40.8128, -73.9060) at 2024-01-15 10:29:50 | Total locations: 3
  ...
```

### Combined Log (`combined_bird_tracking_log.txt`)
Format:
```
[2024-01-15 10:30:00] BIRD: initial - bird_01 (Sparrow) at (40.7128, -74.0060)
[2024-01-15 10:30:30] TRACKER: Query Results:
  bird_01 (Sparrow): Last at (40.7128, -74.0060) on 2024-01-15 10:30:00 | Total: 1
  ...
```

## Example Workflow

1. **Start Cassandra Cluster**:
   ```bash
   docker-compose up -d
   ```

2. **Run Bird Client** (in one terminal):
   ```bash
   python bird_client_v2.py
   ```

3. **Run Tracker Client** (in another terminal):
   ```bash
   python tracker_client_v2.py
   ```

4. **Or use Combined Client**:
   ```bash
   python combined_threaded_client.py
   ```

## Error Handling

All clients include comprehensive error handling:
- Connection failures to Cassandra
- Query execution errors
- File I/O errors
- Graceful shutdown on interruption

## Performance Considerations

- **Prepared Statements**: Used for better performance
- **Connection Pooling**: Cassandra driver handles connection pooling
- **Thread Safety**: Combined client uses thread-safe operations
- **Batch Operations**: Consider batching for high-volume scenarios

## Monitoring

- Console output shows real-time progress
- Log files provide detailed history
- Error messages include timestamps and context

## Cleanup

To clean up the database:
```bash
python cleanup_db.py
```

This will drop the `birds_tracking` keyspace and all its data. 