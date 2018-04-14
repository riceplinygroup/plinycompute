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
pemFile=$1
PDB_SLEEP_TIME=40
pkill -9  test404
pkill -9  test603
pkill -9  pdb-manager
pkill -9  pdb-worker

$PDB_HOME/bin/pdb-manager localhost 8108 N $pemFile 1.5 &

echo "#####################################"
echo "To sleep for 100 seconds in total for all ssh to return"
echo "#####################################"

sleep $PDB_SLEEP_TIME

for x in {1..10};
do
   if pgrep -x "pdb-manager" > /dev/null
   then
       echo "manager is started!"
       exit 0
   fi
   sleep 10
done

echo "manager hasn't started! It could be that ssh takes too long time. Please retry!"


