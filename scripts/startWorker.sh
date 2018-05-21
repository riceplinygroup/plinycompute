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

    Description: This script launches a PlinyCompute worker node.

    Usage: scripts/$(basename $0) param1 param2 param3 param4

           param1: <num_threads> (number of threads)
           param2: <shared_memory> (amount of memory in Mbytes (e.g. 4096 = 4Gb)
           param3: <manager_node_ip> (IP address of manager node)
           param4: <worker_node_ip> (IP address of worker node)

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && [ -z $3 ] && [ -z $4 ] && { usage; }

numThreads=$1
sharedMemSize=$2
manager_ip=$3
ip_addr=$4

echo -e "+++++++++++ removing existing processes"
pkill -9 pdb-worker
pkill -9 pdb-manager 

echo -e "+++++++++++ launching a pdb-worker node at IP: $ip_addr"
echo "bin/pdb-worker $numThreads $sharedMemSize $manager_ip $ip_addr &"
if [ -n "${PDB_SSH_FOREGROUND}" ]; then
   bin/pdb-worker $numThreads $sharedMemSize $manager_ip $ip_addr
else
   nohup bin/pdb-worker $numThreads $sharedMemSize $manager_ip $ip_addr  >> logs/log.out 2>&1 < /dev/null &
fi
