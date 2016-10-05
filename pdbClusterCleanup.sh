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
PDB_HOME="PDBServer"
PDB_CLEANUP_SCRIPT="./tools/utils/cleanup.sh"


##############  Function to Run pdbServer on each node ####################
readClusterCleanUp() {

old_IFS=$IFS  # save the field separator
IFS=$'\n'     # new field separator, the end of line

for line in $(cat "$1")
do

# parse the line to an array
IFS='#' read -ra ADDR <<< "$line"

echo "Loging to the machine with IP $2";
scp  -i $2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r $PDB_CLEANUP_SCRIPT  ${ADDR[0]}":"$3 ;


# remove old files 
if [ "$4" = "y" ]; then 
	echo "Removing old files on ${ADDR[0]}";
	ssh  -i $2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  ${ADDR[0]}  "cd $3 && ./cleanup.sh y " ;
else
	echo "Not Removing old files on ${ADDR[0]}";
	ssh  -i $2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no  ${ADDR[0]}  "cd $3 && ./cleanup.sh n " ;
fi;



done
IFS=$old_IFS     # restore default field separator

}


###############################################
#######
#######   Main Body of the Script
#######
###############################################


read -p "Do you wish to remove all old files on Cluster?[y/n]" yn

case $yn in
   [Yy]* ) readClusterCleanUp  $PDB_CLUSTER_CONFIG_FILE $1  $PDB_HOME y ;;
   [Nn]* ) readClusterCleanUp  $PDB_CLUSTER_CONFIG_FILE $1  $PDB_HOME n ;;
       * ) echo "Please answer yes or no.";;
esac




