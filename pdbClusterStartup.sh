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
#!/bin/bash
PDB_CLUSTER_CONFIG_FILE="pdbCluster.config"
PEM_FILE=$1
PDB_HOME="PDB/"
PDB_COMMAND="./pdbServer"
PDB_DIRS1="bin/pdbServer" 
PDB_DIRS2="libraries/" 
PDB_DIRS3="pdbSettings.conf" 



##############  Function to Copy Files to each node ####################
readClusterConfigAndCopy() {
   
old_IFS=$IFS  # save the field separator           
IFS=$'\n'     # new field separator, the end of line           

for line in $(cat "$1")          
do      

# parse the line to an array 
IFS='#' read -ra ADDR <<< "$line"

# first create the director - if exists remove and make it 
ssh  -i $2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  ${ADDR[0]} "rm -rf $3 && mkdir -p $3" ;

# now copy files 
scp  -i $2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r $4 $5 $6  ${ADDR[0]}":"$3 ; 

done          
IFS=$old_IFS     # restore default field separator

} 


##############  Function to Run pdbServer on each node ####################
readClusterConfigAndRun() {
   
old_IFS=$IFS  # save the field separator           
IFS=$'\n'     # new field separator, the end of line           

for line in $(cat "$1")          
do      

# parse the line to an array 
IFS='#' read -ra ADDR <<< "$line"


# Now SSH to each machine and start up the pdbServer
# echo "ssh -i $2 ${ADDR[0]} \"cd $3 && nohub $4 ${ADDR[1]} > /dev/null 2>&1 & \"  "; 

ssh  -i $2  -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  ${ADDR[0]} 'bash -c "cd $3 && nohub $4 ${ADDR[1]} > /dev/null 2>&1 & "'

done          
IFS=$old_IFS     # restore default field separator
} 


###############################################
#######  
#######   Main Body of the Script 
#######  
###############################################




read -p "Do you wish to copy PDB to all cluster nodes?[y/n]" yn

case $yn in
   [Yy]* ) readClusterConfigAndCopy $PDB_CLUSTER_CONFIG_FILE $1 $PDB_HOME  $PDB_DIRS1 $PDB_DIRS2 $PDB_DIRS3;;
   [Nn]* ) echo "Not Copying only starting cluster" ;;
       * ) echo "Please answer yes or no.";;
esac

echo "Now start up the cluster nodes!" ; 

readClusterConfigAndRun $PDB_CLUSTER_CONFIG_FILE $1 $PDB_HOME  $PDB_COMMAND ;


# scp  -i /home/kia/kiaRicekey.pem -oStrictHostKeyChecking=no  -r $PDB_DIRS   ubuntu@10.134.96.142:~pdb/


