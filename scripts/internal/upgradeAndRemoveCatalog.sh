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
    echo -e "\033[33;31m""    "Warning: This script deletes PlinyCompute catalog metadata in remote""
    echo -e "    "machines in a cluster, use it carefully!"\e[0m"
    echo ""

    cat <<EOM

    Description: This script removes PlinyCompute catalog metadata, and updates
    executables and scripts on worker nodes.

    Usage: scripts/$(basename $0) param1

           param1: <pem_file>
                      Specify the private key to connect to other machines in
                      the cluster; the default is conf/pdb-key.pem

EOM
   exit -1;
}

[ -z $1 ] && { usage; }  || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

pem_file=$1
user=ubuntu
ip_len_valid=3
pdb_dir=$PDB_INSTALL
testSSHTimeout=3

if [ ! -f ${pem_file} ]; then
    echo -e "Pem file ""\033[33;31m""'$pem_file'""\e[0m"" not found, make sure the path and file name are correct!"
    exit -1;
fi

echo "To strip shared libraries, this may take some time..."
strip libraries/*.so
echo "stripped all shared libraries!"

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

echo "Removes local catalog and temp shared libraries"
rm -rf $PDB_HOME/CatalogDir
rm /var/tmp/*.so

echo "Reading cluster IP addresses from file: $PDB_HOME/conf/serverlist"
while read line
do
   [[ $line == *#* ]] && continue # skips commented lines
   [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/conf/serverlist

if [ $? -ne 0 ]
then
   echo -e "The file ""\033[33;31m""conf/serverlist""\e[0m"" was not found."
   echo -e "Make sure ""\033[33;31m""conf/serverlist""\e[0m"" exists"
   echo -e "and contains the IP addresses of the worker nodes."
   exit -1
fi

length=${#arr[@]}
echo "There are $length servers defined in $PDB_HOME/conf/serverlist"

for (( i=0 ; i<=$length ; i++ ))
do
   ip_addr=${arr[i]}
   if [ ${#ip_addr} -gt "$ip_len_valid" ]
   then
      # checks that ssh to a node is possible, times out after 3 seconds
      nc -zw$testSSHTimeout ${ip_addr} 22
      if [ $? -eq 0 ]
      then
         echo -e "\n+++++++++++ install server: $ip_addr"
         ssh $PDB_SSH_OPTS $user@$ip_addr "rm /var/tmp/*.so; rm $pdb_dir/log.out; rm -rf $pdb_dir/Catalog*; rm $pdb_dir/logs/*"
         scp $PDB_SSH_OPTS -r $PDB_HOME/bin/pdb-worker $user@$ip_addr:$pdb_dir/bin/ 
         scp $PDB_SSH_OPTS -r $PDB_HOME/scripts/cleanupNode.sh $PDB_HOME/scripts/stopWorker.sh $user@$ip_addr:$pdb_dir/scripts/
         scp $PDB_SSH_OPTS -r $PDB_HOME/scripts/internal/checkProcess.sh $PDB_HOME/scripts/internal/startWorker.sh $user@$ip_addr:$pdb_dir/scripts/internal
      else
         echo -e "Connection to ""\033[33;31m""IP: ${ip_addr}""\e[0m"", failed."
      fi
   fi
done


