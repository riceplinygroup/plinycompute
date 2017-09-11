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
pdb_dir=$PDB_INSTALL

scripts/cleanupNode.sh
echo "To strip shared libraries..."
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

echo $PDB_HOME/conf/serverlist

if [ "$PLINY_HOME" = "" ]; then
  echo "We do not have pliny dependency."
else
  mkdir bin
  $PLINY_HOME/scripts/cleanup.sh
  scripts/syncWithPliny.sh
fi

arr=($(awk '{print $0}' $PDB_HOME/conf/serverlist))
length=${#arr[@]}
echo "There are $length servers"
for (( i=0 ; i<=$length ; i++ ))
do
        ip_addr=${arr[i]}
        if [ ${#ip_addr} -gt "$ip_len_valid" ]
        then
                echo -e "\n+++++++++++ install server: $ip_addr"
                ssh $PDB_SSH_OPTS $user@$ip_addr "rm -rf $pdb_dir; mkdir $pdb_dir; mkdir $pdb_dir/bin; mkdir  $pdb_dir/logs; mkdir $pdb_dir/scripts"
                scp $PDB_SSH_OPTS -r $PDB_HOME/bin/pdb-server $user@$ip_addr:$pdb_dir/bin/ 
                scp $PDB_SSH_OPTS -r $PDB_HOME/scripts/cleanupNode.sh $PDB_HOME/scripts/startWorker.sh $PDB_HOME/scripts/stopWorker.sh $PDB_HOME/scripts/checkProcess.sh $user@$ip_addr:$pdb_dir/scripts/
                ssh $PDB_SSH_OPTS $user@$ip_addr "cd $pdb_dir; scripts/cleanupNode.sh"
        fi
done


