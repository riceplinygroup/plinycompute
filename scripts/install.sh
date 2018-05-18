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
testSSHTimeout=3

if [ ! -f ${pem_file} ]; then
    echo "Pem file $pem_file not found!"
    exit -1;
fi

scripts/cleanupNode.sh
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

echo $PDB_HOME/conf/serverlist

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
         echo -e "\n+++++++++++ install server: $ip_addr"
         ssh $PDB_SSH_OPTS $user@$ip_addr "rm -rf $pdb_dir; mkdir $pdb_dir; mkdir $pdb_dir/bin; mkdir  $pdb_dir/logs; mkdir $pdb_dir/scripts"
         scp $PDB_SSH_OPTS -r $PDB_HOME/bin/pdb-worker $user@$ip_addr:$pdb_dir/bin/ 
         scp $PDB_SSH_OPTS -r $PDB_HOME/scripts/cleanupNode.sh $PDB_HOME/scripts/startWorker.sh $PDB_HOME/scripts/stopWorker.sh $PDB_HOME/scripts/checkProcess.sh $user@$ip_addr:$pdb_dir/scripts/
         ssh $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir; scripts/cleanupNode.sh"
      else
         echo "Cannot copy files to server with IP address: ${ip_addr}, connection timed out on port 22 after $testSSHTimeout seconds."
      fi
   fi
done


