#!/bin/bash
# Start Kafka KRaft brokers on all 3 nodes
/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/kraft/server.properties
ssh ubuntu@worker1 "/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/kraft/server.properties"
ssh ubuntu@worker2 "/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/kraft/server.properties"

sleep 5
/usr/local/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server 192.168.56.10:9092 describe --replication
