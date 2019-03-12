KAFKA=/home/lorenzo/kafka/

$KAFKA/bin/kafka-topics.sh --list --zookeeper localhost:2181 | while read topic; do
    # $KAFKA/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic $topic --config retention.ms=1000
    $KAFKA/bin/kafka-configs.sh --zookeeper localhost --alter --entity-type topics --entity-name $topic --add-config retention.ms=1000
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $topic
done
