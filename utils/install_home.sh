#!/bin/bash
REDIS_PORT=$1
./install_server.sh --REDIS_PORT $REDIS_PORT --REDIS_CONFIG_FILE $HOME/redis_conf/$REDIS_PORT.conf --REDIS_LOG $HOME/redis_log/$REDIS_PORT.log --REDIS_DATA_DIR $HOME/redis_data/$REDIS_PORT --REDIS_EXECUTABLE `readlink -f ../bin/redis-server`
