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
numThreads=$3
sharedMem=$4
user=ubuntu
ip_len_valid=3
pdb_dir=$PDB_INSTALL
testSSHTimeout=3
PDB_SSH_SLEEP=3
PDB_SLEEP_TIME=3
pkill -9  pdb-manager
pkill -9  pdb-worker

$PDB_HOME/bin/pdb-manager localhost 8108 N $pemFile 1.5 &

echo ""
echo "#####################################"
echo " Launching manager node."
echo "#####################################"

sleep $PDB_SLEEP_TIME

if pgrep -x "pdb-manager" > /dev/null
then
   echo "manager node has been successfully started and is running!"
else
   echo "manager node hasn't started! It could be that ssh takes too long time. Please retry!"
   exit -1;
fi

echo " Waiting 10 secs before launching worker nodes."

sleep 3

# By default disable strict host key checking
if [ "$PDB_SSH_OPTS" = "" ]; then
   PDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

if [ -z ${pemFile} ];
   then echo "ERROR: please provide at least two parameters: one is your pem file and the other is the manager IP";
   echo "Usage: ./scripts/launchCluster.sh #pemFile #managerIp #threadNum #sharedMemSize";
   exit -1;
fi

if [ -z ${managerIp} ];
   then echo "ERROR: please provide at least two parameters: one is your pem file and the other is the manager IP";
   echo "Usage: ./scripts/launchCluster.sh #pemFile #managerIp #threadNum #sharedMemSize";
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

workersOk=0;
workersFailed=0;

echo "There are $length worker nodes defined in conf/serverlist"
for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ]
   then
     # checks that ssh to a node is possible, times out after 3 seconds
     nc -zw$testSSHTimeout ${ip_addr} 22 
     if [ $? -eq 0 ] 
     then
        echo -e "\n+++++++++++ starting worker node at IP address: $ip_addr"
        ssh -i $pemFile $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;  scripts/startWorker.sh $numThreads $sharedMem $managerIp $ip_addr &" &
        sleep $PDB_SSH_SLEEP
        ssh -i $pemFile $user@$ip_addr $pdb_dir/scripts/checkProcess.sh pdb-worker
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

