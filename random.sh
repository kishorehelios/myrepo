#!/bin/sh
# This is a comment!
echo $((RANDOM % 121 - 20))   &    # echo $((RANDOM % (b-a+1) + a))
cd /home/hduser/kafka_2.10-0.9.0.0/
echo $((RANDOM % 121 - 20)) | bin/kafka-console-producer.sh --broker-list localhost:9098 --topic 'temp_log'
