### Setup (assuming ubuntu 18.04)

* export `$KAFKA` env var to point to local installation of kakfa, used by `kafka-tools`
`export KAFKA="/home/$USER/kafka/"`

* make sure kafka `config/server.properties` has the option `delete.topic.enable=true`

* install `postgresql` and setup an account (the following _should_ be enough):
```
sudo apt install postgresql-10 postgresql-client-10
sudo -u postgres pqsl
# in the psql shell issue the \password command and set the password to "postgres" (IMPORTANT)
```

* download the 1k dataset and store it at `dataset/1k-users-sorted/`

* load tables into database so they can be accessed by the application, passing the name of the
dataset as a command line argument
`psql postgres://postgres:postgres@localhost:5432 -f db-tools/tables.sql -v db="1k-users-sorted"`

* rust and cargo installation. Our `cargo --version` returns `cargo 1.35.0-nightly`.
`curl https://sh.rustup.rs -sSf | sh`


### Build & Run

* terminal 1 -- zookeeper server  
`~/kafka $ bin/zookeeper-server-start.sh config/zookeeper.properties`

* terminal 2 -- kafka server  
`~/kafka $ bin/kafka-server-start.sh config/server.properties`

* terminal 3 -- delete kafka topic and run the main application 
`~/dspa-project $ kafka-tools/reset.sh && cargo run --release --bin main -- -q1,2,3 -w2`

* terminal 4 -- producer
`~/dspa-project/producer $ cargo run --bin prod --release ../dataset/1k_users_sorted/streams/ # or other dataset path`
