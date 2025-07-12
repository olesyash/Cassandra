Olesya Sharify and Yaniv Ankri

To start environment:
docker-compose up -d

Then do attach shell to cassandra-cassandra-client and run:
python bird_client.py
This will create all birds

Then do attach shell to cassandra-cassandra-client-tracker and run:
python tracker_client.py 


The logs are attached:
bird_tracking_log
bird_select_traces
bird_update_traces

To locate the token and replica for a specific bird:
Attach shell to cassandra-cassandra-client and run:
python node_failure_simulation.py
