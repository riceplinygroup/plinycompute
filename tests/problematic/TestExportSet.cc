/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/

#ifndef TEST_70_H
#define TEST_70_H




#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "DistributedStorageManagerClient.h"
#include "DataTypes.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

// To export a set
// TODO Dimitrije  this is a utility why is this considered an unit test?
/*  Note that data size must be larger than #numTotalThreadsInCluster*#PageSize */
/*  Below test case is tested using 8GB data in 4-node cluster, each node run 12 threads */
using namespace pdb;
int main(int argc, char* argv[]) {

    if (argc < 6) {
        std::cout << "Usage: #databaseName #setName #outputFilePath #managerIp #outputFormat" << std::endl;
    }
    std::string databaseName = argv[1];
    std::string setName = argv[2];
    std::string outputFilePath = argv[3];
    std::string managerIp = argv[4];
    std::string outputFormat = argv[5];

    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");
    pdb::DistributedStorageManagerClient temp(8108, managerIp, clientLogger);

    std::string errMsg;
    bool ret = temp.exportSet(databaseName, setName, outputFilePath, outputFormat);
    if (ret == false) {
        std::cout << errMsg << std::endl;
    } else {
        std::cout << "Set successfully exported to " << outputFilePath << std::endl;
    }

    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
}

#endif
