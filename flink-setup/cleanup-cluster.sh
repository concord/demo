#!/usr/bin/env bash

source ./cluster-config.sh

ssh $MASTER  $INSTALL_DIR/bin/flink-daemon.sh stop jobmanager --configDir $INSTALL_DIR/conf/

for host in $SLAVES
do
    ssh $host  $INSTALL_DIR/bin/flink-daemon.sh stop taskmanager --configDir $INSTALL_DIR/conf/
done
for host in $MASTER $SLAVES
do
    rm -rf $INSTALL_DIR
done
