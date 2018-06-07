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

    Description: This script stops the PlinyCompute manager node.

    Usage: scripts/$(basename $0) [param1]

           param1: [force]
                      This argument is optional, if provided it doesn't prompt user
                      for confirmation when upgrading executables in an installation
                      of PlinyCompute.

EOM
   exit -1;
}

[ -z $0 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

if [ "x$isForced" = "x" ];then
   read -p "Do you want to stop the PlinyCompute manager node? [Y/n] " -n 1 -r
   echo " "
   if [[ ! $REPLY =~ ^[Yy]$ ]]
   then
      echo "Request cancelled. The manager node is still running."
      [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
   fi
else
   if [ "$isForced" != "force" ];then
      echo -e "\e[31m""Error: the value of the $argName argument should be 'force'""\e[0m"
      echo -e "The manager node is still running."
      exit -1;
   fi
fi

pkill -9  pdb-manager

echo -e "PlinyCompute manager node was stopped!"
