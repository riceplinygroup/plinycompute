
#!/usr/bin/env bash

PEM_FILE=$1
USERNAME=$2

if [ ! -d "$PDB_HOME/conf/cluster" ]
         then mkdir $PDB_HOME/conf/cluster
fi
$PDB_HOME/scripts/proc/collect_cluster_info.sh $PEM_FILE $USERNAME > $PDB_HOME/conf/cluster/cluster_info.txt
echo "Done! The info of cluster are written to $PDB_HOME/conf/cluster/cluster_info.txt"
