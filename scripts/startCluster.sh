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
pemFile=$1
managerIp=$2
cluster_type=$3
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

if [ -z ${pemFile} ];
   then echo "ERROR: please provide at least three parameters: 1) pem file, 2) IP address of manager node, and 3) type of cluster {'standalone', 'distributed'}";
   echo "Usage: ./scripts/launchCluster.sh #pemFile #managerIp #threadNum #sharedMemSize #clusterType";
   exit -1;
fi

if [ -z ${managerIp} ];
   then echo "ERROR: please provide at least three parameters: 1) pem file, 2) IP address of manager node, and 3) type of cluster {'standalone', 'distributed'}";
   echo "Usage: ./scripts/launchCluster.sh #pemFile #managerIp #clusterType #threadNum #sharedMemSize";
   exit -1;
fi

if [ -z ${cluster_type} ];
   then echo "ERROR: please provide at least three parameters: 1) pem file, 2) IP address of manager node, and 3) type of cluster {'standalone', 'distributed'}";
   echo "Usage: ./scripts/launchCluster.sh #pemFile #managerIp #clusterType #threadNum #sharedMemSize";
   exit -1;
fi

if [ "$cluster_type" != "standalone" ] && [ "$cluster_type" != "distributed" ];
   then echo "ERROR: the value of cluster_type can only be either: 'standalone' or 'distributed'";   
fi
    
if [ -z ${numThreads} ];
   then numThreads=4;
fi

if [ -z ${sharedMem} ];
   then sharedMem=4096;
fi

# parses conf/serverlist file
if [ "$cluster_type" = "standalone" ];then
   conf_file="conf/serverlist.test"
else
   conf_file="conf/serverlist"
fi

echo "Reading cluster IP addresses from file: $conf_file" 
while read line
do
   [[ $line == *#* ]] && continue # skips commented lines
   [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/$conf_file
echo ${arr}

length=${#arr[@]}
echo "There are $length servers defined in $PDB_HOME/$conf_file"

echo ""
echo "#####################################"
echo " Launching a manager node."
echo "#####################################"

# launches manager node
$PDB_HOME/bin/pdb-manager localhost 8108 N $pemFile 1.5 &

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
           ssh -i $pemFile $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;  scripts/startWorker.sh $numThreads $sharedMem $managerIp $ip_addr &" &
           sleep $PDB_SSH_SLEEP
           ssh -i $pemFile $user@$ip_addr $pdb_dir/scripts/checkProcess.sh pdb-worker
        else
           ./bin/pdb-worker $numThreads $sharedMem $managerIp $ip_addr &
           sleep $PDB_SSH_SLEEP
           ./scripts/checkProcess.sh pdb-worker
        fi
        if [ $? -eq 0 ]
        then
           workersOk=$[$workersOk + 1]
        else
           workersFailed=$[$workersFailed + 1]
        fi
     else
        echo "Cannot start worker node with IP address: ${ip_addr}, connection times out after $testSSHTimeout seconds."
        workersFailed=$[$workersFailed + 1]
     fi      
   fi
done

if [ $workersOk -eq 0 ]
then
   echo "PlinyCompute cluster failed to start, because $workersFailed worker nodes failed to launch!"
else
   echo "PlinyCompute cluster has been successfuly started with $workersOk worker nodes!"
   if [ $workersFailed -gt 0 ]
   then 
      echo "There were $workersFailed workers nodes that failed to launch!"
   fi
fi

