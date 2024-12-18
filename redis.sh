#!/bin/bash

ANNOUNCE_IP=$1
ANNOUNCE_PORT=$(expr $2)
ANNOUNCE_BUS_PORT=$(expr $ANNOUNCE_PORT + 100)

CONF_FILE="/tmp/redis.conf"

# generate redis.conf file
echo "port 6379
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly yes
loglevel debug
protected-mode no
cluster-announce-ip $ANNOUNCE_IP
cluster-announce-port $ANNOUNCE_PORT
cluster-announce-bus-port $ANNOUNCE_BUS_PORT
" >> $CONF_FILE

# start server
redis-server $CONF_FILE 