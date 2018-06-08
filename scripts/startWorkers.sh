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
    echo -e "Pem file ""\e[31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
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
if [ $length -eq 0 ]; then
   echo -e "[Error] There are no IP addresses in file  ""\e[31m""conf/serverlist""\e[0m""."
   echo -e "Make sure it contains at least one entry."
   exit -1;
else
   echo "There are $length worker nodes defined in conf/serverlist"
fi

resultOkHeader="*** Successful results ("
resultFailedHeader="*** Failed results ("
workersOk=0
workersFailed=0

for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ];then
      # checks that ssh to a node is possible, times out after 3 seconds
      nc -zw$testSSHTimeout ${ip_addr} 22
      if [ $? -eq 0 ];then
          echo -e "\n+++++++++++ starting worker node at IP address $ip_addr"
          ssh -i $pem_file $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;"
          if [ $? -ne 0 ];then
             resultFailed+="Directory $pdb_dir not found in worker node with IP ""\e[31m""${ip_addr}""\e[0m"". Failed to start.\n"
             workersFailed=$[$workersFailed + 1]
          else
             # checks if a worker is already running on that machine
             ssh -i $pem_file $user@$ip_addr $pdb_dir/scripts/internal/checkProcess.sh pdb-worker
             if [ $? -eq 0 ];then
                resultFailed+="There's a worker already running on machine with IP ""\e[31m""${ip_addr}""\e[0m""\n"
                echo -e "There's a worker already running on machine with IP ""\e[31m""${ip_addr}""\e[0m""\n"
                workersFailed=$[$workersFailed + 1]
             else
                ssh -i $pem_file $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir; scripts/internal/startWorker.sh $numThreads $sharedMem $managerIp $ip_addr &" &
                sleep $PDB_SSH_SLEEP
                ssh -i $pem_file $user@$ip_addr $pdb_dir/scripts/internal/checkProcess.sh pdb-worker
                if [ $? -eq 0 ];then
                   resultOk+="Worker node with IP $ip_addr successfully started.\n"
                   workersOk=$[$workersOk + 1]
                fi
             fi
          fi
      else
         resultFailed+="Connection to ""\e[31m""IP ${ip_addr}""\e[0m"", failed. Worker node failed to start.\n"
         echo -e "Connection to ""\e[31m""IP ${ip_addr}""\e[0m"", failed. Worker node failed to start.\n"
         workersFailed=$[$workersFailed + 1]
      fi
   fi
done

echo -e "\e[35m""---------------------------------"
echo -e "Results of script $(basename $0):""\e[0m"
echo -e "$resultFailedHeader$workersFailed/$length) ***\n$resultFailed"
echo -e "$resultOkHeader$workersOk/$length) ***\n$resultOk"
echo -e "\e[35m""---------------------------------\n""\e[0m"

if [ $workersFailed -ne 0 ];then
   echo -e "\e[31m""PlinyCompute workers failed to start, because $workersFailed worker nodes failed to launch!""\e[0m"
else
   echo "All $workersOk registered PlinyCompute worker nodes have been successfuly started!"
fi
