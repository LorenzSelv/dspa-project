### Setup

* export `$KAFKA` env var to local installation of kakfa, used by `kafka-tools`  
`export KAFKA="/home/$USER/kafka/"`

* change path in `Cargo.toml` to local cloned repo of `timely`, we should change this..  
  (do not commit the changes, but also do not gitignore it)

### Build & Run

* terminal 1 -- zookeeper server  
`~/kafka $ bin/zookeeper-server-start.sh config/zookeeper.properties`

* terminal 2 -- kafka server  
`~/kafka $ bin/kafka-server-start.sh config/server.properties`

* terminal 3 -- main application  
`~/dspa-project $ kafka-tools/delete_all_topics.sh && cargo run --release -- w1`

* terminal 4 -- producer  
`~/dspa-project/producer $ cargo run --bin prod --release ../dataset/tests/task1/active/ # or other dataset path`
