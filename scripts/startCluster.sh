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
                      cluster; if running in distributed node, use the IP address
                      instead of 'localhost'
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

# By default disable strict host key checking
if [ "$PDB_SSH_OPTS" = "" ]; then
   PDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

if [ "$cluster_type" != "standalone" ] && [ "$cluster_type" != "distributed" ];
   then echo "[Error] the value of cluster_type can only be either: 'standalone' or 'distributed'";
fi

if [ -z $2 ];then
   echo -e "Error: IP address for manager node was not provided as second argument to the script"
   exit -1;
fi

if [ "$cluster_type" = "distributed" ];then
   # first checks if a manager node is already running
   pgrep -ax pdb-manager | grep -v localhost > /dev/null
   if [ $? -eq 0 ];then
      echo -e "[Warning] A PlinyCompute cluster seems to be running in distributed mode. Stop it first by running"
      echo -e "the following script '""\e[34m""./scripts/stopCluster.sh distributed ""\e[0m""""\e[31m""key-file.pem""\e[0m""'."
      echo -e "Replacing the ""\e[31m""key-file.pem""\e[0m"" argument with your own pem file."
      exit -1;
   fi
   conf_file="conf/serverlist"
   if [ ! -f $PDB_HOME/$conf_file ];then
      echo -e "[Error] Cluster cannot be started because the file ""\e[31m""$conf_file""\e[0m"" was not found."
      echo -e "Make sure ""\e[31m""$conf_file""\e[0m"" exists."
      exit -1
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
   pgrep -ax pdb-manager | grep localhost > /dev/null
   if [ $? -eq 0 ];then
      echo -e "[Warning] A PlinyCompute cluster seems to be running in standalone mode. Stop it first by running"
      echo -e "the script '""\e[34m""./scripts/stopCluster.sh standalone""\e[0m""'."
      exit -1;
   fi
   conf_file="conf/serverlist.test"
   if [ ! -f $PDB_HOME/$conf_file ];then
      echo -e "[Error] Cluster cannot be started because the file ""\e[31m""$conf_file""\e[0m"" was not found."
      echo -e "Make sure ""\e[31m""$conf_file""\e[0m"" exists."
      exit -1
   fi      
fi

if [ -z ${numThreads} ];
   then numThreads=1;
fi

if [ -z ${sharedMem} ];
   then sharedMem=2048;
fi

# parses conf/serverlist file
echo "Reading cluster IP addresses from file: $conf_file" 

while read line
do
   [[ $line == *#* ]] && continue # skips commented lines
   [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/$conf_file

echo ${arr}

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
     if [ $? -eq 0 ];then 
        echo -e "\n+++++++++++ starting worker node at IP address $ip_addr"
        if [[ ${ip_addr} != *":"* ]];then
           ssh -i $pem_file $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;"
           if [ $? -ne 0 ];then
              resultFailed+="Directory $pdb_dir not found in worker node with IP ""\e[31m""${ip_addr}""\e[0m"". Failed to start.\n"
              workersFailed=$[$workersFailed + 1]
           else
              ssh -i $pem_file $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir; scripts/internal/startWorker.sh $numThreads $sharedMem $managerIp $ip_addr &" &
              sleep $PDB_SSH_SLEEP
              ssh -i $pem_file $user@$ip_addr $pdb_dir/scripts/internal/checkProcess.sh pdb-worker
              if [ $? -eq 0 ];then
                 resultOk+="Worker node with IP: $ip_addr successfully started.\n"
                 workersOk=$[$workersOk + 1]
              fi
           fi
        else
           ./bin/pdb-worker $numThreads $sharedMem $managerIp $ip_addr &
           sleep $PDB_SSH_SLEEP
           ./scripts/internal/checkProcess.sh pdb-worker
           if [ $? -eq 0 ];then
              resultOk+="Worker node with IP: $ip_addr successfully started.\n"
              workersOk=$[$workersOk + 1]
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
   echo -e "\e[31m""PlinyCompute cluster failed to start, because $workersFailed worker nodes failed to launch!""\e[31m"
else
   echo "PlinyCompute cluster has been successfuly started with $workersOk worker nodes!"
fi

