#!/bin/bash 

echo " INTERRUZIONE DEL SERVIZIO MONGODB .... "
sudo service mongod stop

echo " CHIUSURA DI HADOOP .... "
$HADOOP_DIR/sbin/stop-dfs.sh
$HADOOP_DIR/sbin/stop-yarn.sh

echo "  COMPLETATO"
