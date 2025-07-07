# Cassandra Tracing Implementation Guide

## Overview
This implementation adds comprehensive tracing functionality to answer **Question 2.2** about Cassandra's execution trace and query flow analysis.

## What's Been Implemented

### 🔍 Tracing Features Added

1. **UPDATE Operation Tracing** (`bird_client_v2.py`)
   - Traces all UPDATE operations for `bird_01`
   - Traces the initial INSERT operation for `bird_01`
   - Logs detailed coordinator and replica flow

2. **SELECT Operation Tracing** (`tracker_client_v2.py`)
   - Traces all SELECT queries for `bird_01`
   - Shows query execution path through cluster nodes
   - Logs timing and coordinator information

3. **Detailed Trace Parsing**
   - Extracts coordinator and replica information
   - Shows operation flow with timestamps
   - Identifies which nodes handle which operations

## How to Use

### Step 1: Start Cassandra Cluster
```bash
docker-compose up -d
```

### Step 2: Run Bird Client (in Terminal 1)
```bash
python bird_client_v2.py
```
**What it traces:**
- 1 INSERT operation for `bird_01`
- 20 UPDATE operations for `bird_01` (every 60 seconds)
- Creates: `bird_update_traces.log`

### Step 3: Run Tracker Client (in Terminal 2)
```bash
python tracker_client_v2.py
```
**What it traces:**
- SELECT operations for `bird_01` (every 30 seconds)
- Creates: `bird_select_traces.log`

## Trace Log Files Generated

### 1. `bird_update_traces.log`
Contains detailed traces for:
- INSERT operations (initial bird location)
- UPDATE operations (location changes)

### 2. `bird_select_traces.log`  
Contains detailed traces for:
- SELECT operations (querying bird locations)

## Trace Information Captured

For each traced operation, the logs contain:

### Basic Trace Info
- **Trace ID**: Unique identifier for the operation
- **Coordinator**: Which node coordinated the operation
- **Duration**: Total operation time in microseconds
- **Request Type**: Type of CQL operation
- **Parameters**: Query parameters used

### Operation Flow Analysis
- **Step-by-step timeline** of the operation
- **Source node** for each step
- **Thread information** for internal processing
- **Activity description** for each step
- **Coordinator vs Replica identification**

## Example Trace Output

```
[2024-01-15 10:30:00] UPDATE OPERATION TRACE for bird_01
------------------------------------------------------------
Operation Details: UPDATE location to (40.7128, -74.0060) - Round 1
Trace ID: 12345678-1234-1234-1234-123456789abc
Coordinator: /172.18.0.2:9042
Total Duration: 2847 microseconds
Request Type: Execute
Parameters: ['bird_01', '2024-01-15 10:30:00+00:00', 'Sparrow', 40.7128, -74.0060]

OPERATION FLOW (Coordinator and Replicas Timeline):
--------------------------------------------------
  Step  1: [     245 μs] /172.18.0.2:9042
           Thread: Native-Transport-Requests-1
           Activity: Parsing INSERT INTO birds_tracking...
           >>> COORDINATOR OPERATION <<<

  Step  2: [     567 μs] /172.18.0.3:9042
           Thread: MutationStage-2
           Activity: Applying mutation locally
           >>> REPLICA OPERATION <<<

  Step  3: [    1234 μs] /172.18.0.4:9042
           Thread: MutationStage-1
           Activity: Applying mutation locally
           >>> REPLICA OPERATION <<<
```

## Question 2.2 Requirements Satisfied

✅ **Execution trace with "trace" flag**: Implemented using `trace=True` parameter  
✅ **get_query_trace() usage**: Used to retrieve trace results  
✅ **UPDATE operation tracing**: All updates for bird_01 are traced  
✅ **SELECT operation tracing**: All queries for bird_01 are traced  
✅ **Coordinator identification**: Shows which node coordinates each operation  
✅ **Replica flow analysis**: Shows operation flow across replica nodes  
✅ **Timestamp analysis**: Detailed timing information for each step  

## Analysis Instructions

1. **Run both clients simultaneously** to capture INSERT/UPDATE and SELECT traces
2. **Examine the trace log files** to see coordinator vs replica operations
3. **Compare timings** between different operations
4. **Identify patterns** in how Cassandra distributes operations
5. **Analyze the flow** of operations across the 4-node cluster

## Key Observations You Can Make

- **Coordinator Selection**: Which nodes act as coordinators for different operations
- **Replica Distribution**: How data is replicated across nodes
- **Operation Timing**: Performance characteristics of different operations
- **Network Communication**: Flow of data between cluster nodes
- **Consistency Handling**: How Cassandra ensures data consistency

This implementation provides comprehensive insight into Cassandra's internal operation flow, fully satisfying the requirements of Question 2.2. 