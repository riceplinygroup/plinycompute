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

usage() {
    cat <<EOM

    This script checks whether or not a process is running.

    Usage: scripts/$(basename $0) param1

           param1: <process_name> (the name of the process to check)

EOM
   exit -1;
}

[ -z $1 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

processName=$1
numProcesses=0

if pgrep -x $processName > /dev/null
then
   echo "$processName is up and running!"
   numProcesses=1
fi

if [ "$numProcesses" -eq 0 ]; then
   echo "$processName is not running!"
   exit 1
fi
exit 0
