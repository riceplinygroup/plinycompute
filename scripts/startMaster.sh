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
pemFile=$1
PDB_SLEEP_TIME=60
pkill -9  test404
pkill -9  test603
pkill -9  pdb-cluster
pkill -9  pdb-server

$PDB_HOME/bin/pdb-cluster localhost 8108 N $pemFile &

echo "#####################################"
echo "To sleep for 100 seconds in total for all ssh to return"
echo "#####################################"

sleep $PDB_SLEEP_TIME

for x in {1..10};
do
   if pgrep -x "pdb-cluster" > /dev/null
   then
       echo "master is started!"
       exit 0
   fi
   sleep 10
done

echo "master hasn't started! It could be that ssh takes too long time. Please retry!"


