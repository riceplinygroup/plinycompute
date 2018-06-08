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

pem_file=$1
user=ubuntu
ip_len_valid=3
testSSHTimeout=3

if [[ ! -v PDB_INSTALL ]]; then
   pdb_dir="/tmp/pdb_install"
else
   pdb_dir=$PDB_INSTALL
fi

usage() {
    echo ""
    echo -e "\e[;31m""    "Warning: This script deletes PlinyCompute stored data in remote""
    echo -e "    "machines in a cluster, use with care!"\e[0m"
    echo ""

    cat <<EOM

    Description: This script installs a distributed instance of PlinyCompute by
    copying the required executables and scripts to the nodes in a cluster. It 
    first cleans the contents of an installation of PlinyCompute in folder 
    '$pdb_dir' (if it exists).

    Usage: scripts/$(basename $0) <param1>

           param1: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster; the default is conf/pdb-key.pem

EOM
   exit -1;
}

[ -z $1 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

if [ ! -f ${pem_file} ]; then
    echo -e "Pem file ""\e[;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
    exit -1;
fi

echo -e "\e[;31m""Before installing PlinyCompute, this script deletes all PlinyCompute stored data, use it carefully!""\e[0m"
echo -e "PlinyCompute default installation path has been set to: ""\e[;32m""$pdb_dir""\e[0m"

read -p "Do you want to delete all PlinyCompute stored data?i [Y/n] " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Installation process cancelled. All previous stored data remained unchanged."
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
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

echo "Reading cluster IP addresses from file: $PDB_HOME/conf/serverlist"

if [ ! -f $PDB_HOME/conf/serverlist ];then
   echo -e "The file ""\e[;31m""conf/serverlist""\e[0m"" was not found."
   echo -e "Make sure ""\e[;31m""conf/serverlist""\e[0m"" exists"
   echo -e "and contains the IP addresses of the worker nodes."
   exit -1
fi

while read line
do
   [[ $line == *#* ]] && continue # skips commented lines
   [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/conf/serverlist

length=${#arr[@]}

if [ $length -eq 0 ]; then
   echo -e "[Error] There are no IP addresses in file ""\e[31m""conf/serverlist""\e[0m""."
   echo -e "Make sure it contains at least one entry."
   exit -1;
else
   echo "There are $length worker nodes defined in file conf/serverlist"
fi

resultOkHeader="*** Successful results ("
resultFailedHeader="*** Failed results ("
totalOk=0
totalFailed=0

for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ]
   then
      # checks that ssh to a node is possible, times out after 3 seconds
      nc -w $testSSHTimeout $ip_addr 22 > /dev/null 2>&1
      if [ $? -eq 0 ]
      then
         echo -e "\n+++++++++++ install worker node at IP: $ip_addr"
         ssh $PDB_SSH_OPTS $user@$ip_addr "rm -rf $pdb_dir; mkdir $pdb_dir; mkdir $pdb_dir/bin; mkdir  $pdb_dir/logs; mkdir $pdb_dir/scripts; mkdir $pdb_dir/scripts/internal"
         scp $PDB_SSH_OPTS -r $PDB_HOME/bin/pdb-worker $user@$ip_addr:$pdb_dir/bin/ 
         scp $PDB_SSH_OPTS -r $PDB_HOME/scripts/internal/cleanupNode.sh $PDB_HOME/scripts/internal/stopWorker.sh $user@$ip_addr:$pdb_dir/scripts/internal
         scp $PDB_SSH_OPTS -r $PDB_HOME/scripts/internal/checkProcess.sh $PDB_HOME/scripts/internal/startWorker.sh $user@$ip_addr:$pdb_dir/scripts/internal
         ssh $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir; scripts/internal/cleanupNode.sh force"
         resultOk+="Worker node with IP: $ip_addr successfully installed.\n"
         totalOk=`expr $totalOk + 1`
      else
         resultFailed+="Connection to ""\e[;31m""IP ${ip_addr}""\e[0m"", failed. Files were not installed.\n"
         totalFailed=`expr $totalFailed + 1`
         echo -e "Connection to ""\e[;31m""IP ${ip_addr}""\e[0m"", failed. Files were not installed."
      fi
   fi
done

echo -e "\e[;35m""---------------------------------"
echo -e "Results of script $(basename $0):""\e[0m"
echo -e "$resultFailedHeader$totalFailed/$length) ***\n$resultFailed"
echo -e "$resultOkHeader$totalOk/$length) ***\n$resultOk"
echo -e "\e[;35m""---------------------------------\n""\e[0m"
