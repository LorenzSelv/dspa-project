# USAGE EXAMPLE: tail -n +2 dataset/1k-users-sorted/streams/likes_event_stream.csv | head | kafka-tools/pipe_to_topic.sh likes
$KAFKA/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $1
