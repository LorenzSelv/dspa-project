# export env variable in .bashrc
# KAFKA=/home/sara/kafka/

$KAFKA/bin/kafka-topics.sh --list --zookeeper localhost:2181 | while read topic; do
    if [ "$topic" = "__consumer_offsets" ]; then continue; fi
    $KAFKA/bin/kafka-configs.sh --zookeeper localhost --alter --entity-type topics --entity-name $topic --add-config retention.ms=1000
    $KAFKA/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $topic
done
