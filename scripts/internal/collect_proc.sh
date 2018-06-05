#!/usr/bin/env bash
#  Copyright 2018 Rice University
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  ========================================================================    

set -o errexit

usage() {
    cat <<EOM

    Description: This script collects CPU and memory information of the 
    machines in a cluster of PlinyCompute.

    Usage: scripts/$(basename $0) param1

           param1: <pem_file>
                      Specify the private key to connect to other machines in 
                      the cluster; the default is conf/pdb-key.pem

EOM
   exit -1;
}

[ -z $1 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }


pem_file=$1
USERNAME=ubuntu

if [ ! -f ${pem_file} ]; then
    echo -e "Pem file ""\033[33;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
    exit -1;
fi

#remember to set environment variable: PDB_HOME first
if [ -z ${PDB_HOME} ]; 
    then echo "[ERROR] Please set PDB_HOME first";
else 
    echo "Your current PDB_HOME is: '$PDB_HOME'";
fi

# remember to provide your pem file as parameter
# if no pem file is provided, uses conf/pdb-key.pem as default
if [ -z ${pem_file} ];
    then pem_file=$PDB_HOME/conf/pdb-key.pem;
    chmod -R 600 $PDB_HOME/conf/pdb-key.pem
else
    echo "Your pem file is: '$pem_file'";
    chmod -R 600 $pem_file
fi

if [ ! -d "$PDB_HOME/conf" ]
    then mkdir $PDB_HOME/conf
fi

if [ ! -f "$PDB_HOME/conf/serverlist" ]
   then cp serverlist $PDB_HOME/conf/serverlist
fi

#distribute the collection scripts
$PDB_HOME/scripts/internal/proc/distribute.sh $pem_file $USERNAME

#collect the information of the distributed cluster
$PDB_HOME/scripts/internal/proc/collect.sh $pem_file $USERNAME

#cleanup the collection scripts
$PDB_HOME/scripts/internal/proc/cleanupLocal.sh $pem_file $USERNAME
