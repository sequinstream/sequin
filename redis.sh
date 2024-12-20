#!/bin/bash

ANNOUNCE_IP=$1
PORT=6379
BUS_PORT=16379  # Standard Redis cluster bus port

CONF_FILE="/tmp/redis.conf"

# generate redis.conf file
echo "port $PORT
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly yes
loglevel debug
protected-mode no
cluster-announce-ip $ANNOUNCE_IP
cluster-announce-port $PORT
cluster-announce-bus-port $BUS_PORT
" >> $CONF_FILE

# start server
redis-server $CONF_FILE 