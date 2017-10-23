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

# grab the root directory
PDB_ROOT=$(pwd)

# prompt if we should do the cleanup
cd ${PDB_ROOT}/tools/docker/build-tests-image

printf "Do you want to remove the test build data?\n"

options=("Yes" "No")
select opt in "${options[@]}"
do
    case $opt in
        "Yes")
            ./clean-tests.sh; cd ${PDB_ROOT}; exit
            ;;
        "No")
            cd ${PDB_ROOT}; exit
            ;;
        *) echo invalid option;;
    esac
done