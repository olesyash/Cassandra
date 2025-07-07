# Question 3: Node Failure Simulation Guide

## Overview
This guide provides a complete implementation for **Question 3: Node Failure Simulation** in Cassandra. It covers all required steps from token ring analysis to failure simulation and flow analysis.

## 🎯 Question 3 Requirements

✅ **3.1** Use `nodetool ring` command to inspect token distribution  
✅ **3.2** Choose a specific bird, find its token, and locate its replica  
✅ **3.3** Simulate failure by stopping a node that holds the bird's token  
✅ **3.4** Re-run `nodetool ring` after failure  
✅ **3.5** Re-run operations and analyze trace results for flow changes  

## 🚀 Quick Start

### Prerequisites
1. **Cassandra cluster running**:
   ```bash
   docker-compose up -d
   ```

2. **Bird data available**:
   ```bash
   python bird_client_v2.py
   # Let it run for a few minutes to insert data
   ```

### Run Complete Simulation
```bash
python node_failure_simulation.py
```

This single script performs all steps of Question 3 automatically!

## 📋 Manual Step-by-Step Process

If you prefer to run steps manually:

### Step 3.1: Inspect Token Distribution
```bash
# Check ring status from any container
docker exec cassandra-1 nodetool ring

# Or use the built-in function
python -c "
from node_failure_simulation import NodeFailureSimulator
sim = NodeFailureSimulator()
sim.inspect_token_ring('MANUAL')
"
```

### Step 3.2: Find Bird Token and Replica
```bash
# Find token for bird_01
python -c "
from node_failure_simulation import NodeFailureSimulator
sim = NodeFailureSimulator()
token, replica = sim.find_bird_token_and_replica()
print(f'Token: {token}')
print(f'Replica: {replica}')
"
```

### Step 3.3: Simulate Node Failure
```bash
# Stop the node that holds bird_01's token
# (The script will automatically identify and stop the correct node)
```

### Step 3.4: Re-inspect Ring After Failure
```bash
# Check ring status after failure
docker exec cassandra-2 nodetool ring  # Use a different node
```

### Step 3.5: Test Operations with Tracing
```bash
# The script automatically tests UPDATE and SELECT operations
# with tracing enabled to show flow changes
```

## 📊 Expected Results

### Before Node Failure
- **4 nodes active** in the ring
- **Token ranges distributed** across all nodes
- **bird_01 token** assigned to specific primary replica
- **Operations flow** through normal coordinator/replica pattern

### After Node Failure
- **3 nodes active** in the ring
- **Token ranges redistributed** to remaining nodes
- **Failed node marked as DOWN**
- **Operations still work** but with different flow pattern
- **New coordinator** handles bird_01 operations

## 📁 Generated Files

The simulation creates detailed log files:

### 1. `node_ring_analysis.log`
Contains token ring snapshots:
- Before failure
- After failure  
- After recovery (if performed)

### 2. `node_failure_simulation.log`
Contains detailed simulation data:
- Bird token analysis
- Replica identification
- Node failure process
- Operation traces after failure

## 🔍 Understanding the Results

### Token Ring Analysis
```
Address        Rack       Status  Token                    Owns   Host ID
172.18.0.2     rack1      Up      -9223372036854775808     25.0%  uuid1
172.18.0.3     rack1      Up      -4611686018427387904     25.0%  uuid2  
172.18.0.4     rack1      Up      0                        25.0%  uuid3
172.18.0.5     rack1      Up      4611686018427387903      25.0%  uuid4
```

### Token Assignment
- Each node owns a **token range**
- Bird data is assigned based on **hash of partition key**
- `token(bird_id)` function shows exact token value
- Primary replica is determined by token range ownership

### Failure Impact
- **Failed node** no longer appears as "Up"
- **Remaining nodes** handle the failed node's token range
- **Consistency level** may affect operation success
- **Trace shows different coordinator** after failure

## 🔧 Trace Analysis Examples

### Before Failure
```
Coordinator: /172.18.0.2:9042  (Primary replica node)
Step 1: [245 μs] /172.18.0.2:9042 - Coordinator operation
Step 2: [567 μs] /172.18.0.3:9042 - Replica operation  
Step 3: [890 μs] /172.18.0.4:9042 - Replica operation
```

### After Failure (Node 172.18.0.2 down)
```
Coordinator: /172.18.0.3:9042  (New coordinator)
Step 1: [312 μs] /172.18.0.3:9042 - Coordinator operation
Step 2: [445 μs] /172.18.0.4:9042 - Replica operation
Step 3: [623 μs] /172.18.0.5:9042 - Replica operation
```

## 🎛️ Advanced Usage

### Target Different Bird
```python
# Modify the script to target a different bird
simulator = NodeFailureSimulator()
simulator.target_bird_id = "bird_05"  # Change target
simulator.run_complete_simulation()
```

### Custom Container Stopping
```bash
# Manually stop specific container
docker stop cassandra-2

# Check impact
docker exec cassandra-1 nodetool ring
```

### Recovery Testing
```bash
# Restart failed node
docker start cassandra-2

# Wait for rejoin
sleep 15

# Check ring status
docker exec cassandra-1 nodetool ring
```

## ⚠️ Important Notes

1. **Replication Factor**: With RF=2, losing 1 node should not cause data loss
2. **Consistency Levels**: Operations may fail if consistency cannot be met
3. **Automatic Recovery**: Cassandra automatically handles node failures
4. **Data Redistribution**: Token ranges are automatically redistributed

## 🧪 Experiment Variations

### Test Different Scenarios
1. **Stop multiple nodes** (test cluster resilience)
2. **Stop coordinator vs replica** (different failure patterns)
3. **Different consistency levels** (QUORUM vs ONE)
4. **Network partitions** (using `iptables` rules)

### Performance Analysis
1. **Compare latencies** before and after failure
2. **Measure throughput** with reduced nodes
3. **Analyze recovery time** when node rejoins

## 📈 Learning Outcomes

After completing this simulation, you'll understand:

- **Token ring mechanics** and data distribution
- **Replica selection** and coordinator responsibility  
- **Failure detection** and automatic recovery
- **Consistency vs availability** trade-offs
- **Operation flow changes** during failures
- **Cassandra's fault tolerance** mechanisms

## 🎉 Success Criteria

Your simulation is successful if you can:

✅ Identify bird token and owning node  
✅ Successfully stop the owning node  
✅ Show ring changes after failure  
✅ Execute operations after failure  
✅ Demonstrate different trace flows  
✅ Analyze coordinator changes  

This comprehensive simulation demonstrates Cassandra's distributed nature and fault tolerance capabilities! 