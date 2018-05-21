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
    Usage: scripts/$(basename $0) param1

           param1: <pem_file> (e.g. conf/pdb-key.pem)

EOM
   exit -1;
}

[ -z $1 ] && { usage; }

pem_file=$1
user=ubuntu
ip_len_valid=3
pdb_dir=$PDB_INSTALL
PDB_SSH_SLEEP=30
testSSHTimeout=3

if [ ! -f ${pem_file} ]; then
    echo "Pem file '$pem_file' not found, make sure the path and file name are correct!"
    exit -1;
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

while read line
do
    [[ $line == *#* ]] && continue # skips commented lines
    [[ ! -z "${line// }" ]] && arr[i++]=$line # include only non-empty lines
done < $PDB_HOME/conf/serverlist

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
         echo -e "\n+++++++++++ stop server: $ip_addr"
         ssh $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir;  scripts/stopWorker.sh"
      else
         echo "Cannot connect to IP address: ${ip_addr}, connection timed out on port 22 after $testSSHTimeout seconds."
      fi
   fi
done

echo "pdb-worker nodes have been stopped!"
