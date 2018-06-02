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
    echo ""
    echo -e "\033[33;31m""    "Warning: This script stops a cluster of PlinyCompute,""
    echo -e "    "use with care!"\e[0m"
    echo ""
    cat <<EOM

    Description: This script stops a cluster of PlinyCompute, including the
    manager node and all worker nodes (defined in conf/serverlist).

    Usage: scripts/$(basename $0) param1 param2

           param1: <cluster_type>
                      Specify the type of cluster; {'standalone', 'distributed'}
           param2: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster, only required when running in distributed
                      mode; the default is conf/pdb-key.pem

EOM
   exit -1;
}

[ -z $1 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

cluster_type=$1
pem_file=$2
user=ubuntu
ip_len_valid=3
pdb_dir=$PDB_INSTALL
PDB_SSH_SLEEP=30
testSSHTimeout=3

if [ "$cluster_type" = "distributed" ];then
   if [ -z $2 ];then
      echo -e "Error: pem file was not provided as the second argument when invoking the script."
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

pkill -9 pdb-worker
pkill -9 pdb-manager

# By default disable strict host key checking
if [ "$PDB_SSH_OPTS" = "" ]; then
   PDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

if [ -z ${pem_file} ];
then
   PDB_SSH_OPTS=$PDB_SSH_OPTS
else
   PDB_SSH_OPTS="-i ${pem_file} $PDB_SSH_OPTS"
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
done < $PDB_HOME/conf/serverlist

if [ $? -ne 0 ]
then
   echo -e "Either ""\033[33;31m""conf/serverlist""\e[0m" or "\033[33;31m""conf/serverlist.test""\e[0m"" files were not found."
   echo -e "If running in standalone mode, make sure ""\033[33;31m""conf/serverlist.test""\e[0m"" exists."
   echo -e "If running in distributed mode, make sure ""\033[33;31m""conf/serverlist""\e[0m"" exists"
   echo -e "with the IP addresses of the worker nodes."
   exit -1
fi

# stops worker nodes only if running in distributed cluster
if [ "$cluster_type" = "distributed" ];then
   length=${#arr[@]}
   echo "There are $length servers defined in $PDB_HOME/conf/serverlist"

   pkill -9 pdb-manager
   echo "PlinyCompute manager node in a distributed cluster has been stopped!"

   for (( i=0 ; i<=$length ; i++ ))
   do
      ip_addr=${arr[i]}
      if [ ${#ip_addr} -gt "$ip_len_valid" ]
      then
         # checks that ssh to a node is possible, times out after 3 seconds
         nc -zw$testSSHTimeout ${ip_addr} 22
         if [ $? -eq 0 ]
         then
            echo -e "\n+++++++++++ stop server: $ip_addr"
            ssh $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;  scripts/stopWorker.sh"
         else
            echo "Cannot connect to IP address: ${ip_addr}, connection timed out on port 22 after $testSSHTimeout seconds."
         fi
      fi
   done
else
   pkill -9 pdb-manager
   pkill -9 pdb-worker
   echo "PlinyCompute standalone cluster has been stopped!"
fi

