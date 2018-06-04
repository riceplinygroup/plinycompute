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
    echo ""
    echo -e "\033[33;31m""    "Warning: This script stops a manager and worker processes of PlinyCompute,"
    "use with care!"\e[0m"
    cat <<EOM

    Description: This script stops the pdb-manager and pdb-worker processes.

    Usage: scripts/$(basename $0)

EOM
   exit -1;
}

[ $# -eq 1 ] && { usage; } || [[ "$@" = *--help ]] && { usage; } || [[ "$@" = *-h ]] && { usage; }

pkill -9 pdb-worker
pkill -9 pdb-manager
