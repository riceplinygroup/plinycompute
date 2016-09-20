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
DATA_DIRS="pdbRoot catalog"


echo "delete data folders and kill PDB processes. "
rm -rf $DATA_DIRS


for KILLPID in `ps ax | grep '[p]dbServer' | awk ' { print $1;}'`; do 
  kill -9 $KILLPID;
done


echo "sleep for 2 sec and kill again"
sleep 2


for KILLPID in `ps ax | grep '[p]dbServer' | awk ' { print $1;}'`; do
  kill -9 $KILLPID;
done


echo "Done!"




