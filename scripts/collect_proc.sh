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


PEM_FILE=$1



USERNAME=ubuntu


#remember to set environment variable: PDB_HOME first
if [ -z ${PDB_HOME} ]; 
    then echo "Please set PDB_HOME first!";
else 
    echo "Your current PDB_HOME is: '$PDB_HOME'";
fi

#remember to provide your pem file as parameter
if [ -z ${PEM_FILE} ];
    then PEM_FILE=$PDB_HOME/conf/jia-test-key.pem;
else
    echo "Your pem file is: '$PEM_FILE'";
fi


if [ ! -d "$PDB_HOME/conf" ]
         then mkdir $PDB_HOME/conf
fi
if [ ! -f "$PDB_HOME/conf/serverlist" ]
   then cp serverlist $PDB_HOME/conf/serverlist
fi

$PDB_HOME/scripts/proc/1_set_path.sh $PEM_FILE $USERNAME
$PDB_HOME/scripts/proc/2_collect_all_proc.sh $PEM_FILE $USERNAME
$PDB_HOME/scripts/proc/3_clear_temp_path.sh $PEM_FILE $USERNAME
