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
# by Jia
pemFile=$1

$PDB_HOME/scripts/cleanupNode.sh
#$PDB_HOME/scripts/cleanupWork.sh
sleep 5
$PDB_HOME/bin/test404 localhost 8108 N $pemFile &

echo "#####################################"
echo "To sleep for 100 seconds in total for all ssh to return"
echo "#####################################"
for x in {1..10}
do 
   sleep 10
   time=`expr $x \\* 10`
   time=`expr 100 - $time`
   echo "still need to wait $time seconds for all ssh to return"
done

for x in {1..10};
do
   if pgrep -x "test404" > /dev/null
   then
       echo "master is started!"
       exit 0
   fi
   sleep 15
done

echo "master hasn't started! It could be that ssh takes too long time. Please retry!"


#$PDB_HOME/bin/CatalogTests  --port 8108 --serverAddress localhost --command register-node --node-ip localhost --node-port  8108 --node-name master --node-type master                
