#!/bin/bash

TERM="xterm-color"
PS1='\[\033[01;34m\]\w \[\033[01;32m\]➤  \[\033[00m\]'
WS="/workspace/"
SRC="/workspace/dspa-project"

SESS="dspa"
INIT="export TERM=\"xterm-color\" && \
      export PS1='\[\033[01;34m\]\w \[\033[01;32m\]➤  \[\033[00m\]' && \
      clear"
RUNAPP="kafka-tools/reset.sh && cargo run --release --bin main -- -q 1,2,3 -w1"
RUNPROD="cargo run --bin prod --release ../dataset/1k-users-sorted/streams/"

#### build window
tmux new-session -d -s $SESS -n "build" -c "$SRC" || exit 1
tmux split-window -t $SESS:build.0 -v -c "$SRC/producer" || exit 1
# run app
tmux send-keys -t $SESS:build.0 "$INIT" Enter
tmux send-keys -t $SESS:build.0 "clear" Enter
tmux send-keys -t $SESS:build.0 "$RUNAPP"
# run producer
tmux send-keys -t $SESS:build.1 "$INIT" Enter
tmux send-keys -t $SESS:build.1 "clear" Enter
tmux send-keys -t $SESS:build.1 "$RUNPROD"

#### kafka server window
tmux new-window -t $SESS -n "kafka" -c "$WS/kafka" || exit 1
tmux split-window -t $SESS:kafka.0 -v -c "$WS/kafka" || exit 1
# zookeeper server
tmux send-keys -t $SESS:kafka.0 "bin/zookeeper-server-start.sh config/zookeeper.properties" Enter
# kafka server
tmux send-keys -t $SESS:kafka.1 "bin/kafka-server-start.sh config/server.properties" Enter

tmux select-window -t $SESS:build.0 || exit 1
tmux attach -t $SESS
