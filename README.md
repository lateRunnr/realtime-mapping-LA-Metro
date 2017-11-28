# realtime-mapping-LA-Metro
Realtime mapping of LA metro (bus and train services) using GIS locations of buses and trains.

Steps:

1) Real time data ingested into Kafka cluster.

2) Data from Kafka cluster is consumed :
  a) Batch Layer
  b) Speed Layer (Real Time Streaming)

3) The results from Batch layer and Speed Layer are fed into a Service layer showing the real time positioning of LA buses and trains
