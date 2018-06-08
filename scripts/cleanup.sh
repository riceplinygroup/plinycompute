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
    echo -e "\e[31m""    "Warning: This script deletes stored data. Deleted data cannot be"
             "restored, use it carefully!"\e[0m"

    cat <<EOM

    Description: This script deletes all PlinyCompute storage, catalog metadata,
    and kills both pdb-manager and pdb-worker processes in one machine if 
    cluster-type=standalone or the entire cluster if cluster_type=distributed.

    Usage: scripts/$(basename $0) <param1> <param2> [param3]

           param1: <cluster_type>
                      Specify the type of cluster; {'standalone', 'distributed'}

           param2: <pem_file>
                      Specify the private key to connect to other machines in 
                      the cluster. This arg is required only when the value of
                      cluster_type is 'distributed'; the default value is 
                      conf/pdb-key.pem

           param3: [force]
                      This argument is optional, if provided it doesn't prompt user
                      for confirmation when cleaning up stored data in an installation
                      of PlinyCompute.         

EOM
   exit -1;
}

[ -z $1 ] && [ -z $2 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

cluster_type=$1
pem_file=$2
user=ubuntu
ip_len_valid=3
pdb_dir=$PDB_INSTALL
testSSHTimeout=3
isForced=""

echo -e "\e[31m""This script deletes all PlinyCompute stored data, use it carefully!""\e[0m"

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
else
   isForced=$2
   argName="second"
fi

if [ "x$isForced" = "x" ];then
   read -p "Do you want to delete all PlinyCompute stored data?i [Y/n] " -n 1 -r
   echo " "
   if [[ ! $REPLY =~ ^[Yy]$ ]]
   then
      echo "Cleanup process cancelled. Stored data were not deleted."
      [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
   fi
else
   if [ "$isForced" != "force" ];then
      echo -e "\e[31m""Error: the value of the $argName argument should be 'force'""\e[0m"
      echo -e "All data were kept in storage."
      exit -1;
   fi
fi

scripts/internal/cleanupNode.sh force

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

while read line
do
   [[ $line == *#* ]] && continue # skips commented lines
   [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/$conf_file

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

for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ];then
      only_ip=${ip_addr%:*}
      # checks that ssh to a node is possible, times out after 3 seconds
      if [[ ${ip_addr} != *":"* ]];then
         nc -zw$testSSHTimeout ${ip_addr} 22
      else
         nc -zw$testSSHTimeout ${only_ip} 22
      fi
      if [ $? -eq 0 ];then
            echo -e "\n+++++++++++ cleanup server: $ip_addr"
         if [[ ${ip_addr} != *":"* ]];then
            ssh $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir; scripts/internal/cleanupNode.sh force"
            resultOk+="Worker node with IP: $only_ip successfully cleanned.\n"
            totalOk=`expr $totalOk + 1`
         else
            ./scripts/internal/cleanupNode.sh force
            resultOk+="Worker node with IP: $ip_addr successfully cleanned.\n"
            totalOk=`expr $totalOk + 1`
         fi
      else
         resultFailed+="Connection to ""\e[31m""IP ${ip_addr}""\e[0m"", failed. Worker node was not cleanned.\n"
         totalFailed=`expr $totalFailed + 1`
      fi
    fi
done

echo -e "\e[35m""---------------------------------"
echo -e "Results of script $(basename $0):""\e[0m"
echo -e "$resultFailedHeader$totalFailed/$length) ***\n$resultFailed"
echo -e "$resultOkHeader$totalOk/$length) ***\n$resultOk"
echo -e "\e[35m""---------------------------------\n""\e[0m"

