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

    Description: This script launches a PlinyCompute manager node.

    Usage: scripts/$(basename $0) param1 param2 param3 

           param1: <cluster_type>
                      Specify the type of cluster; {'standalone', 'distributed'}
           param2: <manager_node_ip>
                      Specify the public IP address of the manager node in a
                      cluster; if running in distributed node, use the IP address
                      instead of 'localhost'
           param3: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster, only required when running in distributed
                      mode; the default is conf/pdb-key.pem

EOM
   exit -1;
}

[ -z $1 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

cluster_type=$1
managerIp=$2
pem_file=$3
PDB_SLEEP_TIME=10
testSSHTimeout=3

if [ "$cluster_type" != "standalone" ] && [ "$cluster_type" != "distributed" ];
   then echo "[Error] the value of cluster_type can only be either: 'standalone' or 'distributed'";
   exit -1;
fi

if [ "$cluster_type" = "distributed" ];then
   if [ -z $2 ];then
      echo -e "Error: IP address for manager node was not provided as second argument to the script"
      exit -1;
   fi
   # first checks if a manager node is already running
   if pgrep -x pdb-manager > /dev/null
   then
      echo -e "[Warning] A PlinyCompute cluster seems to be running. Stop it first by running"
      echo -e "the following script '""\e[34m""./scripts/stopManager.sh.""\e[31m"
      exit -1;
   fi
   if [ "$managerIp" = "localhost" ];then
      echo -e "\e[31m""[Error] When running the cluster in 'distributed' mode, use the public IP"
      echo -e "address of the manager node, instead of 'localhost'""\e[0m"
      exit -1;
   fi
   # checks if the manager node ip address is reachable
   nc -zw$testSSHTimeout $managerIp 22 > /dev/null 2>&1
   if [ $? -ne 0 ]; then
      echo -e "[Error] The IP address of the manager node provided: ""\e[31m""'$managerIp'""\e[0m"" is not reachable."
      exit -1;
   fi
   if [ -z $3 ];then
      echo -e "[Error] pem file was not provided as the third argument when invoking the script."
      exit -1;
   fi
   if [ ! -f ${pem_file} ]; then
      echo -e "[Error] Pem file ""\e[31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
      exit -1;
    fi
else
   if pgrep -x pdb-manager > /dev/null
   then
      echo -e "[Warning] A PlinyCompute manager node seems to be running. Stop it first by running"
      echo -e "the script '""\e[34m""./scripts/stopManager.sh""\e[0m""'."
      exit -1;
   fi
fi

# launches manager node
if [ "$cluster_type" = "standalone" ];then
echo -e "Launching $PDB_HOME/bin/pdb-manager $managerIp 8108 Y &"
   $PDB_HOME/bin/pdb-manager $managerIp 8108 Y &
else
   $PDB_HOME/bin/pdb-manager $managerIp 8108 N $pem_file 1.5 &
fi

echo "#####################################"
echo "To sleep for 100 seconds in total for"
echo "all ssh connections to return"
echo "#####################################"

sleep $PDB_SLEEP_TIME

for x in {1..10};
do
   if pgrep -x "pdb-manager" > /dev/null
   then
       echo "PlinyCompute manager node successfully started!"
       exit 0
   fi
   sleep 10
done

echo -e "\e[31m""PlinyCompute manager node didn't started! It could be that ssh takes too long. Please retry!""\e[31m"
