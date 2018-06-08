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

    Description: This script launches a cluster of PlinyCompute, including the
    manager node and all worker nodes (defined in conf/serverlist).

    Usage: scripts/$(basename $0) param1 param2 param3 param4 param5

           param1: <cluster_type>
                      Specify the type of cluster; {'standalone', 'distributed'}
           param2: <manager_node_ip> 
                      Specify the public IP address of the manager node in a 
                      cluster; the default is localhost
           param3: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster, only required when running in distributed
                      mode; the default is conf/pdb-key.pem
           param4: [num_threads]
                      Specify the number of threads; default 1
           param5: [shared_memory]
                      Specify the amount of shared memory in Mbytes; default 
                      is 2048

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && [ -z $3 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

cluster_type=$1
managerIp=$2
pem_file=$3
numThreads=$4
sharedMem=$5
user=ubuntu
ip_len_valid=3
pdb_dir=$PDB_INSTALL
testSSHTimeout=3
PDB_SSH_SLEEP=3
PDB_SLEEP_TIME=3

# kills running processes
pkill -9  pdb-manager
pkill -9  pdb-worker

# By default disable strict host key checking
if [ "$PDB_SSH_OPTS" = "" ]; then
   PDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

if [ "$cluster_type" = "distributed" ];then
   # checks if the manager node ip address is reachable
   nc -zw$testSSHTimeout $managerIp 22
   if [ $? -ne 0 ]; then
      echo -e "Error: the IP address ""\033[33;31m""'$managerIp'""\e[0m"" of the manager node is not reachable."     
      exit -1; 
   fi
   if [ -z $3 ];then 
      echo -e "Error: pem file was not provided as the third argument when invoking the script."
      exit -1;
   fi
   if [ ! -f ${pem_file} ]; then
      echo -e "Pem file ""\033[33;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
      exit -1;
    fi
fi

if [ "$cluster_type" != "standalone" ] && [ "$cluster_type" != "distributed" ];
   then echo "ERROR: the value of cluster_type can only be either: 'standalone' or 'distributed'";   
fi

if [ -z $2 ];then
   echo -e "Error: IP address for manager node was not provided as second argument to the script"
   exit -1;
fi

if [ -z ${numThreads} ];
   then numThreads=1;
fi

if [ -z ${sharedMem} ];
   then sharedMem=2048;
fi

# parses conf/serverlist file
if [ "$cluster_type" = "standalone" ];then
   conf_file="conf/serverlist.test"
else
   conf_file="conf/serverlist"
fi

echo "Reading cluster IP addresses from file: $conf_file" 

if [ ! -f $PDB_HOME/$conf_file ];then
   echo -e "Either ""\033[33;31m""conf/serverlist""\e[0m" or "\033[33;31m""conf/serverlist.test""\e[0m"" files were not found."
   echo -e "If running in standalone mode, make sure ""\033[33;31m""conf/serverlist.test""\e[0m"" exists."
   echo -e "If running in distributed mode, make sure ""\033[33;31m""conf/serverlist""\e[0m"" exists"
   echo -e "with the IP addresses of the worker nodes."
   exit -1
fi

while read line
do
   [[ $line == *#* ]] && continue # skips commented lines
   [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/$conf_file

echo ${arr}

length=${#arr[@]}
echo "There are $length worker nodes defined in conf/serverlist"

resultOkHeader="*** Successful results ("
resultFailedHeader="*** Failed results ("
totalOk=0
totalFailed=0

echo ""
echo "#####################################"
echo " Launching a manager node."
echo "#####################################"

# launches manager node
if [ "$cluster_type" = "standalone" ];then
   $PDB_HOME/bin/pdb-manager $managerIp 8108 Y &
else
   $PDB_HOME/bin/pdb-manager $managerIp 8108 N $pem_file 1.5 &
fi

sleep $PDB_SLEEP_TIME

if pgrep -x "pdb-manager" > /dev/null
then
   echo "manager node has been successfully started and is running!"
else
   echo "manager node hasn't started! It could be that ssh takes too long time. Please retry!"
   exit -1;
fi

echo " Waiting 10 secs before launching worker nodes."

sleep 10

workersOk=0;
workersFailed=0;

for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ];then
     # checks that ssh to a remote node is possible, times out after 3 seconds
     only_ip=${ip_addr%:*}
     if [[ ${ip_addr} != *":"* ]];then
        nc -zw$testSSHTimeout ${ip_addr} 22
     else
        nc -zw$testSSHTimeout ${only_ip} 22
     fi
     # launches worker nodes only if connection is established
     if [ $? -eq 0 ] 
     then
        echo -e "\n+++++++++++ starting worker node at IP address: $ip_addr"
        if [[ ${ip_addr} != *":"* ]];then
           ssh -i $pem_file $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;  scripts/internal/startWorker.sh $numThreads $sharedMem $managerIp $ip_addr &" &
           sleep $PDB_SSH_SLEEP
           ssh -i $pem_file $user@$ip_addr $pdb_dir/scripts/internal/checkProcess.sh pdb-worker
        else
           ./bin/pdb-worker $numThreads $sharedMem $managerIp $ip_addr &
           sleep $PDB_SSH_SLEEP
           ./scripts/internal/checkProcess.sh pdb-worker
        fi
        if [ $? -eq 0 ]
        then
           workersOk=$[$workersOk + 1]
           resultOk+="Worker node with IP: $ip_addr successfully started.\n"
        else
           resultFailed+="Worker node with IP: ""\033[33;31m""${ip_addr}""\e[0m"", failed to start.\n"
           workersFailed=$[$workersFailed + 1]
        fi
     else
        resultFailed+="Connection to ""\033[33;31m""IP ${ip_addr}""\e[0m"", failed. Worker node failed to start.\n"
        echo -e "Connection to ""\033[33;31m""IP ${ip_addr}""\e[0m"", failed. Worker node failed to start.\n"
        workersFailed=$[$workersFailed + 1]
     fi      
   fi
done

if [ $workersOk -eq 0 ]
then
   echo -e "\033[33;31m""PlinyCompute cluster failed to start, because $workersFailed worker nodes failed to launch!""\033[33;31m"
else
   echo "PlinyCompute cluster has been successfuly started with $workersOk worker nodes!"
fi

echo -e "\033[33;35m""---------------------------------"
echo -e "Results of script $(basename $0):""\e[0m"
echo -e "$resultFailedHeader$workersFailed/$length) ***\n$resultFailed"
echo -e "$resultOkHeader$workersOk/$length) ***\n$resultOk"
echo -e "\033[33;35m""---------------------------------\n""\e[0m"
