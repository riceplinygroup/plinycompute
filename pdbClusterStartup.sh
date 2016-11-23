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

PDB_CLUSTER_CONFIG_FILE="./conf/serverlist"
PEM_FILE=$1
PDB_HOME="PDBServer"
PDB_COMMAND="pdbServer"
PDB_FOLDERS_TO_COPY="bin libraries conf pdbStartup.sh"

PDB_STARTUP="pdbStartup.sh"
PDB_DIRS1="bin"
PDB_DIRS2="libraries"
PDB_DIRS3="conf"
SSHPort="22"
USER="ubuntu"



##############  Function to Copy Files to each node ####################
readClusterConfigAndCopy() {

# old_IFS=$IFS  # save the field separator
# IFS=$'\n'     # new field separator, the end of line

echo "Creating the tar.gz file to copy to cluster nodes... "; 

tar cvfz $3.tar.gz  $@; 


for line in $(cat "$1")
do

# parse the line to an array
# IFS='#' read -ra ADDR <<< "$line"

# first create the director - if exists remove and make it
ssh  -i $2 -p $SSHPort -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  $USER@$line "rm -rf $3 && mkdir -p $3" ;

# Copy tar file over
scp  -i $2 -P $SSHPort -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r $3.tar.gz  $USER@$line":" ;

# SSH to the machine and untar and remove file
ssh  -i $2 -p $SSHPort -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  $USER@$line "tar xvfz  $3.tar.gz  -C $3 && rm -f $3.tar.gz " ;


done
# IFS=$old_IFS     # restore default field separator 
# $line was ${ADDR[0]}

} 


##############  Function to Run pdbServer on each node ####################
readClusterConfigAndRun() {

# old_IFS=$IFS  # save the field separator
# IFS=$'\n'     # new field separator, the end of line

COUNTER=0


for line in $(cat "$1")
do

# parse the line to an array
# IFS='#' read -ra ADDR <<< "$line"

COUNTER=$[COUNTER + 1] 

echo $COUNTER
MasterNodeHost=""

if [ $COUNTER -eq 1 ]
then

  ssh  -i $2 -p $SSHPort -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  $USER@$line  "cd $3 && ./pdbStartup.sh " ;
  $MasterNodeHost=$line
  echo "Start a Master Node on $MasterNodeHost"

else
  echo "Start a Slave Node on $line"
  ssh  -i $2 -p $SSHPort -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  $USER@$line  "cd $3 && ./pdbStartup.sh -s $MasterNodeHost" ;
fi


echo $line 

# Now SSH to each machine and start up the pdbServer

# ssh  -i $2 -p $SSHPort -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  USER@$line  "cd $3 && ./pdbStartup.sh -s " ;

done
# IFS=$old_IFS     # restore default field separator

}


###############################################
#######
#######   Main Body of the Script
#######
###############################################


read -p "Do you wish to copy PDB to all cluster nodes?[y/n]" yn

case $yn in
  [Yy]* ) readClusterConfigAndCopy $PDB_CLUSTER_CONFIG_FILE $1 $PDB_HOME $PDB_FOLDERS_TO_COPY;;
  [Nn]* ) echo "Not Copying only starting cluster" ;;
      * ) echo "Please answer yes or no.";;
esac

echo "Now start up the cluster nodes!" ; 

readClusterConfigAndRun $PDB_CLUSTER_CONFIG_FILE $1 $PDB_HOME;


## OLD Commands - Maybe needed in future ... 
# echo "ssh -i $2 ${ADDR[0]} \"cd $3 && nohub $4 ${ADDR[1]} > /dev/null 2>&1 & \"  ";
# ssh  -i $2  -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  ${ADDR[0]} 'bash -c "cd $3 && screen -d -m ./pdbServer  ${ADDR[1]}  "' ;
# scp  -i $2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r $4 $5 $6  ${ADDR[0]}":"$3 ;
# scp  -i $2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r $7  ${ADDR[0]}":" ;


