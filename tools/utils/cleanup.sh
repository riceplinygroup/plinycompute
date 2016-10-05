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
DATA_DIRS="pdbRoot"
CALALOG_DIAR="catalog"

echo "Removing old folders and killing PDB processes. "


# remove old files 
if [ "$1" = "y" ]; then 

echo "Removing old files ..."
rm -rf $DATA_DIRS $CALALOG_DIAR; 

fi;

echo "Killing Process pdbServer"

for KILLPID in `ps ax | grep '[p]dbServer' | awk ' { print $1;}'`; do 
  kill -9 $KILLPID;
done

echo "sleep 2 sec and kill again pdbServer processes"

sleep 2


for KILLPID in `ps ax | grep '[p]dbServer' | awk ' { print $1;}'`; do 
  kill -9 $KILLPID;
done

echo "Done!"




