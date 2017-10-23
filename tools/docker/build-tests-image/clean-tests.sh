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

# set the pdb directory
PDB_DIR=$(readlink -f ../../../)

# do the cleanup
rm -rf ${PDB_DIR}/Testing
rm -rf ${PDB_DIR}/CMakeFiles
rm -rf ${PDB_DIR}/bin/*
rm -rf ${PDB_DIR}/libraries/*
rm -rf ${PDB_DIR}/logs/*
rm -rf ${PDB_DIR}/cmake_install.cmake
rm -rf ${PDB_DIR}/CMakeCache.txt
rm -rf ${PDB_DIR}/CTestTestfile.cmake
rm -rf ${PDB_DIR}/Makefile

printf "Removed!\n"