#!/bin/bash 

echo " AVVIAMENTO DEL SERVIZIO MONGODB .... "
sudo service mongod start

echo " AVVIAMENDO DI HADOOP .... "
$HADOOP_DIR/sbin/start-dfs.sh
$HADOOP_DIR/sbin/start-yarn.sh
jps
echo "  COMPLETATO"
