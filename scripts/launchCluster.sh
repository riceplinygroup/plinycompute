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
PDB_SSH_SLEEP=10
PDB_SLEEP_TIME=40
pkill -9  pdb-manager
pkill -9  pdb-worker

$PDB_HOME/bin/pdb-manager localhost 8108 N $pemFile 1.5 &

echo ""
echo "#####################################"
echo "To sleep for 100 seconds in total for all ssh to return"
echo "#####################################"

sleep $PDB_SLEEP_TIME

if pgrep -x "pdb-manager" > /dev/null
then
   echo "manager node has been started and is running!"
else
   echo "manager node hasn't started! It could be that ssh takes too long time. Please retry!"
   exit -1;
fi
sleep 10

# By default disable strict host key checking
if [ "$PDB_SSH_OPTS" = "" ]; then
   PDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

if [ -z ${pemFile} ];
   then echo "ERROR: please provide at least two parameters: one is your the path to your pem file and the other is the manager IP";
   echo "Usage: scripts/startWorkers.sh #pemFile #managerIp #threadNum #sharedMemSize";
   exit -1;
fi

if [ -z ${managerIp} ];
   then echo "ERROR: please provide at least two parameters: one is the path to your pem file and the other is the manager IP";
   echo "Usage: scripts/startWorkers.sh #pemFile #managerIp #threadNum #sharedMemSize";
   exit -1;
fi
    
if [ -z ${numThreads} ];
   then numThreads=4;
fi

if [ -z ${sharedMem} ];
   then sharedMem=4096;
fi

arr=($(awk '{print $0}' $PDB_HOME/conf/serverlist))
length=${#arr[@]}
echo "There are $length worker nodes in conf/serverlist"
for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ]
   then
      echo -e "\n+++++++++++ starting worker node at IP: $ip_addr"
      ssh -i $pemFile $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;  scripts/startWorker.sh $numThreads $sharedMem $managerIp $ip_addr &" &
      sleep $PDB_SSH_SLEEP
      ssh -i $pemFile $user@$ip_addr $pdb_dir/scripts/checkProcess.sh pdb-worker
   fi
done

echo "workers have been successfuly started!"

