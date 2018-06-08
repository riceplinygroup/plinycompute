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
    echo -e "\e[31m""    "Warning: This script stops a cluster of PlinyCompute,""
    echo -e "    "use with care!"\e[0m"
    echo ""
    cat <<EOM

    Description: This script stops a cluster of PlinyCompute, including the
    manager node and all worker nodes (defined in conf/serverlist).

    Usage: scripts/$(basename $0) <param1> <param2> [param3]

           param1: <cluster_type>
                      Specify the type of cluster; {'standalone', 'distributed'}

           param2: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster, only required when running in distributed
                      mode; the default is conf/pdb-key.pem

           param3: [force]
                      This argument is optional, if provided it doesn't prompt user
                      for confirmation when stopping a cluster of PlinyCompute.

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
isForced=""

echo -e "\e[31m""This script stops a cluster of PlinyCompute, use it carefully!""\e[0m"

if [ "$cluster_type" != "standalone" ] && [ "$cluster_type" != "distributed" ];
   then echo "ERROR: the value of cluster_type can only be either: 'standalone' or 'distributed'";
   exit -1;
fi

if [ "$cluster_type" = "distributed" ];then
   isForced=$3
   argName="third"
   if [ -z $2 ];then
      echo -e "Error: pem file was not provided as the second argument when invoking the script."
      exit -1;
   fi
   if [ ! -f ${pem_file} ]; then
      echo -e "Pem file ""\e[31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
      exit -1;
    fi
   conf_file="$PDB_HOME/conf/serverlist"
   if [ ! -f $conf_file ];then
      echo -e "[Error] Cluster cannot be started because the file ""\e[31m""$conf_file""\e[0m"" was not found."
      echo -e "Make sure ""\e[31m""$conf_file""\e[0m"" exists."
      exit -1;
   fi
else
   conf_file="$PDB_HOME/conf/serverlist.test"
   if [ ! -f $conf_file ];then
      echo -e "[Error] Cluster cannot be started because the file ""\e[31m""$conf_file""\e[0m"" was not found."
      echo -e "Make sure ""\e[31m""$conf_file""\e[0m"" exists."
      exit -1;
   fi
   isForced=$2
   argName="second"
fi

if [ "x$isForced" = "x" ];then
   read -p "Do you want to stop this PlinyCompute cluster? [Y/n] " -n 1 -r
   echo " "
   if [[ ! $REPLY =~ ^[Yy]$ ]]
   then
      echo "Stop cluster request was cancelled. PlinyCompute cluster is still running."
      [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
   fi
else
   if [ "$isForced" != "force" ];then
      echo -e "\e[31m""Error: the value of the $argName argument should be 'force'""\e[0m"
      echo -e "PlinyCompute cluster is running."
      exit -1;
   fi
fi

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
echo "Reading cluster IP addresses from file: $conf_file"

while read line
do
    [[ $line == *#* ]] && continue # skips commented lines
    [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $conf_file

# stops worker nodes only if running in distributed cluster
if [ "$cluster_type" = "distributed" ];then
   length=${#arr[@]}
   if [ $length -eq 0 ]; then
      echo -e "[Error] There are no IP addresses in file  ""\e[31m""$conf_file""\e[0m""."
      echo -e "Make sure it contains at least one entry."
      exit -1;
   else
      echo "There are $length worker nodes defined in $conf_file"
   fi

   resultOkHeader="*** Successful results ("
   resultFailedHeader="*** Failed results ("
   totalOk=0
   workersFailed=0

   pkill -9 pdb-manager
   echo "PlinyCompute manager node in a distributed cluster has been stopped!"

   for (( i=0 ; i<=$length ; i++ ))
   do
      ip_addr=${arr[i]}
      if [ ${#ip_addr} -gt "$ip_len_valid" ];then
         # checks that ssh to a node is possible, times out after 3 seconds
         nc -zw$testSSHTimeout ${ip_addr} 22
         if [ $? -eq 0 ];then
            echo -e "\n+++++++++++ stop worker node at IP address: $ip_addr"
            ssh -i $pem_file $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;"
            if [ $? -ne 0 ];then
               resultFailed+="Directory $pdb_dir not found in worker node with IP ""\e[31m""${ip_addr}""\e[0m"". Failed to stop.\n"
               workersFailed=$[$workersFailed + 1]
            else
               # checks if a worker is running on that machine
               ssh -i $pem_file $user@$ip_addr $pdb_dir/scripts/internal/checkProcess.sh pdb-worker
               if [ $? -eq 0 ];then
                  ssh $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;  $pdb_dir/scripts/internal/stopWorker.sh"
                  resultOk+="Worker node with IP: $ip_addr successfully stopped.\n"
                  totalOk=`expr $totalOk + 1`
               else
                  resultFailed+="Nothing to stop on worker node with IP ""\e[31m""${ip_addr}""\e[0m""\n"
                  workersFailed=$[$workersFailed + 1]
               fi
            fi
         else
            resultFailed+="Connection to ""\e[31m""IP ${ip_addr}""\e[0m"", failed. Worker node was not stopped.\n"
            workersFailed=$[$workersFailed + 1]
            echo -e "Connection to ""\e[31m""IP ${ip_addr}""\e[0m"", failed. Worker node was not stopped."
         fi
      fi
   done
   echo -e "\e[35m""---------------------------------"
   echo -e "Results of script $(basename $0):""\e[0m"
   echo -e "$resultFailedHeader$workersFailed/$length) ***\n$resultFailed"
   echo -e "$resultOkHeader$totalOk/$length) ***\n$resultOk"
   echo -e "\e[35m""---------------------------------\n""\e[0m"
else
   pgrep -ax pdb-manager | grep localhost > /dev/null
   if [ $? -eq 0 ];then
      pkill -9 pdb-manager
      echo -e "PlinyCompute manager node has been terminated!"
   else
      echo -e "No manager process to terminate."
   fi
   pgrep -ax pdb-worker | grep localhost > /dev/null
   if [ $? -eq 0 ];then
      pkill -9 pdb-worker
      echo "PlinyCompute standalone workers have been stopped!"
   else
      echo "No worker processes to terminate."
   fi
fi

