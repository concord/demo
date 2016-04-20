#!/usr/bin/env bash

source ./cluster-config.sh

wget http://apache.claz.org/flink/flink-1.0.1/$TARBALL

scp ./$TARBALL $MASTER:$TARGET_DIR/$TARBALL
ssh $MASTER  tar -xvf $TARGET_DIR/$TARBALL
scp ./flink-conf.yaml $MASTER:$INSTALL_DIR/conf/flink-conf.yaml
ssh $MASTER  $INSTALL_DIR/bin/flink-daemon.sh start jobmanager --configDir $INSTALL_DIR/conf/ --executionMode cluster

for host in $SLAVES
do
    scp ./$TARBALL $host:$TARGET_DIR/$TARBALL
    ssh $host  tar -xvf $TARGET_DIR/$TARBALL
    scp ./flink-conf.yaml $host:$INSTALL_DIR/conf/flink-conf.yaml
    ssh $host  $INSTALL_DIR/bin/flink-daemon.sh start taskmanager --configDir $INSTALL_DIR/conf/
done
