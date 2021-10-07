#!/bin/bash 
# specificare il nome del database (nomefile.json)


if [  -z "$1" ]
then
 echo "il comando è errato, la sintassi è: \n sh init-system.sh nomeDatabase \nil file json (jsonArray) deve essere nella cartella /input "
else
echo "Importazione del DATABASE ...."
mongoimport --db $1 --collection $1 --jsonArray --drop --file ~/hadoop-2.7.4/babynamesProject/input/$1.json

echo "ESPORTAZIONE DELLE DIPENDENZE NECESSARIE ...."
#Questo comando richiama lo script che permette di esportare correttamente le librerie
sh $HADOOP_DIR/etc/hadoop/hadoop-env.sh
echo "  COMPLETATO"
fi

