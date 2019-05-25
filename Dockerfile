FROM ubuntu:18.04

RUN mkdir workspace
WORKDIR workspace

RUN apt-get update && apt-get install -y wget git curl unzip vim

# setup kafka
RUN apt-get install -y default-jdk
RUN wget https://www-eu.apache.org/dist/kafka/2.2.0/kafka_2.12-2.2.0.tgz -O kafka.tgz
RUN mkdir kafka && tar xzf kafka.tgz -C kafka --strip-components 1
RUN echo "\ndelete.topic.enable=true" >> kafka/config/server.properties
ENV KAFKA="/workspace/kafka"

# clone repo
RUN git clone https://github.com/LorenzSelv/dspa-project/
WORKDIR dspa-project

ENV DEBIAN_FRONTEND=noninteractive

#install postgresql & init database
RUN apt-get install -y postgresql-10 postgresql-client-10
RUN rm -rf /var/lib/postgresql/10/main/*
RUN su - postgres -c "/usr/lib/postgresql/10/bin/pg_ctl -D /var/lib/postgresql/10/main initdb"
RUN su - postgres -c "/usr/lib/postgresql/10/bin/pg_ctl -D /var/lib/postgresql/10/main start"

# download rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN apt-get install -y musl-dev musl-tools make python gcc g++

RUN cargo build --release

RUN cd producer && cargo build --release && cd ..

RUN apt-get install -y tmux

# download 1k dataset
RUN wget https://polybox.ethz.ch/index.php/s/qRlRpFhoPtdO6bR/download -O 1k-users-sorted.zip
RUN unzip 1k-users-sorted.zip -d dataset/

# download 10k dataset -- UNCOMMENT ME
# RUN wget https://polybox.ethz.ch/index.php/s/8JRHOc3fICXtqzN/download -O 10k-users-sorted.zip
# RUN unzip 10k-users-sorted.zip -d dataset/

COPY .tmux.conf /root/
COPY dspa-tmux.sh .

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

ENTRYPOINT pg_ctlcluster 10 main start && \
           su - postgres -c "psql -U postgres -d postgres -c \"alter user postgres with password 'postgres';\"" && \
           psql postgres://postgres:postgres@localhost:5432 -f db-tools/tables.sql -v db=$DATASET && \
           /bin/bash -c 'source dspa-tmux.sh'

