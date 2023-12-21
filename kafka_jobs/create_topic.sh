#!/bin/bash

# Setting default values
default_partitions=10
default_replication_factor=3

# Check if topic name is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <topic_name> [partitions] [replication_factor]"
    exit 1
fi

# Assigning input parameters to variables
topic_name=$1
partitions=${2:-$default_partitions}
replication_factor=${3:-$default_replication_factor}

# Kafka bootstrap server information
bootstrap_server="34.142.194.212:9092"

# Creating the Kafka topic
kafka-topics.sh --bootstrap-server $bootstrap_server --create --topic $topic_name \
    --partitions $partitions --replication-factor $replication_factor
