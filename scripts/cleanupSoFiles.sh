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

set -o errexit

usage() {
    echo ""
    echo -e "\033[33;31m""    "Warning: This script deletes shared libraries, use it carefully!"\e[0m"
    cat <<EOM

    Description: This script deletes shared libraries.

    Usage: scripts/$(basename $0) param1

           param1: force 
                      Forces the deletion of shared libraries
EOM
   exit -1;
}

[ $# -ne 1 ] && { usage; }  || [[ "$@" = *-h ]] && { usage; }

if [[ ! "$1" = force ]]; then
   echo -e "Error: the argument 'force' was not provided to the script"
   echo -e "All shared librares were kept in storage."
   exit -1;
else
   echo -e "Deleting shared libraries in directory /var/tmp/"
   rm /var/tmp/*.so
fi

