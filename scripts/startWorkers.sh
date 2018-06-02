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

usage() {
    cat <<EOM

    Description: This script launches a cluster of PlinyCompute worker nodes
    whose IP addresses are defined in the conf/serverlist file.

    Usage: scripts/$(basename $0) param1 param2 param3 param4

           param1: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster; the default is conf/pdb-key.pem
           param2: <manager_node_ip>
                      Specify the public IP address of the manager node in a
                      cluster.
           param3: <num_threads>
                      Specify the number of threads; default 4
           param4: <shared_memory>
                      Specify the amount of shared memory in Mbytes; default
                      is 4096

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

pem_file=$1
managerIp=$2
numThreads=$3
sharedMem=$4
user=ubuntu
ip_len_valid=3
pdb_dir=$PDB_INSTALL
PDB_SSH_SLEEP=10
testSSHTimeout=3

if [ ! -f ${pem_file} ]; then
    echo -e "Pem file ""\033[33;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
    exit -1;
fi

# By default disable strict host key checking
if [ "$PDB_SSH_OPTS" = "" ]; then
  PDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

if [ -z ${pem_file} ];
    then echo "ERROR: please provide at least two parameters: one is your the path to your pem file and the other is the manager IP";
    echo "Usage: scripts/startWorkers.sh #pem_file #managerIp #threadNum #sharedMemSize";
    exit -1;
fi

if [ -z ${managerIp} ];
    then echo "ERROR: please provide at least two parameters: one is the path to your pem file and the other is the manager IP";
    echo "Usage: scripts/startWorkers.sh #pem_file #managerIp #threadNum #sharedMemSize";
    exit -1;
fi

if [ -z ${numThreads} ];
     then numThreads=4;
fi

if [ -z ${sharedMem} ];
     then sharedMem=4096;
fi

while read line
do
    [[ $line == *#* ]] && continue # skips commented lines
    [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/conf/serverlist

length=${#arr[@]}
echo "There are $length servers defined in $PDB_HOME/conf/serverlist"

for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ]
   then
      # checks that ssh to a node is possible, times out after 3 seconds
      nc -zw$testSSHTimeout ${ip_addr} 22
      if [ $? -eq 0 ]
      then
          echo -e "\n+++++++++++ start server: $ip_addr"
          ssh -i $pem_file $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;  scripts/startWorker.sh $numThreads $sharedMem $managerIp $ip_addr &" &
          sleep $PDB_SSH_SLEEP
          ssh -i $pem_file $user@$ip_addr $pdb_dir/scripts/internal/checkProcess.sh pdb-worker
      else
          echo "Cannot connect to IP address: ${ip_addr}, connection timed out on port 22 after $testSSHTimeout seconds."            
      fi
   fi
done

echo "pdb-worker nodes have been launched!"
