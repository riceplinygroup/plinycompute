
#!/usr/bin/env bash

usage() {
    cat <<EOM
    Usage: scripts/internal/$(basename $0) param1

           param1: <pem_file> (e.g. conf/pdb-key.pem)
           param2: <username> (username)

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && { usage; }

pem_file=$1
username=$2

if [ -z ${pem_file} ];
    then echo "ERROR: please provide two parameters: one is the path to your pem file and the other is the username to connect.";
    echo "Usage: scripts/startWorkers.sh #pem_file #username";
    exit -1;
fi

if [ -z ${username} ];
    then echo "ERROR: please provide two parameters: one is the path to your pem file and the other is the username to connect.";
    echo "Usage: scripts/startWorkers.sh #pem_file #username";
    exit -1;
fi

if [ ! -d "$PDB_HOME/conf/cluster" ]
   then mkdir $PDB_HOME/conf/cluster
fi
$PDB_HOME/scripts/internal/proc/collect_cluster_info.sh $pem_file $username > $PDB_HOME/conf/cluster/cluster_info.txt
echo "Done! The info of cluster are written to $PDB_HOME/conf/cluster/cluster_info.txt"
