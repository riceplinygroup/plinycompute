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
#ifndef MASTER_MAIN_CC
#define MASTER_MAIN_CC

#include <iostream>
#include <string>

#include "PDBServer.h"
#include "CatalogServer.h"
#include "CatalogClient.h"
#include "ResourceManagerServer.h"
#include "PangeaStorageServer.h"
#include "DistributedStorageManagerServer.h"
#include "DispatcherServer.h"
#include "QuerySchedulerServer.h"
#include "StorageAddDatabase.h"
#include "SharedEmployee.h"

int main(int argc, char* argv[]) {
    int port = 8108;
    std::string managerIp;
    std::string pemFile = "conf/pdb.key";
    bool pseudoClusterMode = false;
    double partitionToCoreRatio = 0.75;
    if (argc == 3) {
        managerIp = argv[1];
        port = atoi(argv[2]);
    } else if ((argc == 4) || (argc == 5) || (argc == 6)) {
        managerIp = argv[1];
        port = atoi(argv[2]);
        std::string isPseudoStr(argv[3]);
        if (isPseudoStr.compare(std::string("Y")) == 0) {
            pseudoClusterMode = true;
            std::cout << "Running in standalone cluster mode" << std::endl;
        }
        if ((argc == 5) || (argc == 6)) {
            pemFile = argv[4];
        }
        if (argc == 6) {
            partitionToCoreRatio = stod(argv[5]);
        }

    } else {
        std::cout << "[Usage] #managerIp #port #runPseudoClusterOnOneNode (Y for running a "
                     "pseudo-cluster on one node, N for running a real-cluster distributedly, and "
                     "default is N) #pemFile (by default is conf/pdb.key) #partitionToCoreRatio "
                     "(by default is 0.75)"
                  << std::endl;
        exit(-1);
    }

    std::cout << "Starting up a distributed storage manager server\n";
    pdb::PDBLoggerPtr myLogger = make_shared<pdb::PDBLogger>("frontendLogFile.log");
    pdb::PDBServer frontEnd(port, 100, myLogger);

    ConfigurationPtr conf = make_shared<Configuration>();

    frontEnd.addFunctionality<pdb::CatalogServer>("CatalogDir", true, managerIp, port);
    frontEnd.addFunctionality<pdb::CatalogClient>(port, "localhost", myLogger);

    //initialize StatisticsDB
    std::shared_ptr<StatisticsDB> statisticsDB = std::make_shared<StatisticsDB>(conf);
    if (statisticsDB == nullptr) {
        std::cout << "fatal error in initializing statisticsDB" << std::endl;
        exit(1);
    } 

    std::string errMsg = " ";
    int numNodes = 1;
    string line;
    string nodeName;
    string hostName;
    string serverListFile;
    int portValue = 8108;

    serverListFile = pseudoClusterMode ? "conf/serverlist.test" : "conf/serverlist";

    frontEnd.addFunctionality<pdb::ResourceManagerServer>(serverListFile, port, pseudoClusterMode, pemFile);
    frontEnd.addFunctionality<pdb::DistributedStorageManagerServer>(myLogger, statisticsDB);
    auto allNodes = frontEnd.getFunctionality<pdb::ResourceManagerServer>().getAllNodes();

    // registers metadata for manager node in the catalog
    if(!frontEnd.getFunctionality<pdb::CatalogServer>().registerNode(std::make_shared<pdb::PDBCatalogNode>("localhost:" + std::to_string(port),
                                                                                                                         "localhost",
                                                                                                                         port,
                                                                                                                         "manager"), errMsg)) {
        std::cout << "Metadata for manager node was not added because " + errMsg << std::endl;
    } else {
        std::cout << "Metadata for manager node successfully added to catalog." << std::endl;
    }
 
    // registers metadata for worker nodes in the catalog
    makeObjectAllocatorBlock(4 * 1024 * 1024, true);

    for (int i = 0; i < allNodes->size(); i++) {

       nodeName = "worker_" + std::to_string(numNodes);
       hostName = (*allNodes)[i]->getAddress().c_str();
       portValue = (*allNodes)[i]->getPort();

       // register the worker
       if(!frontEnd.getFunctionality<pdb::CatalogServer>().registerNode(std::make_shared<pdb::PDBCatalogNode>(hostName + ":" + std::to_string(portValue),
                                                                                                                            hostName,
                                                                                                                            portValue,
                                                                                                                            "worker"), errMsg)) {
          std::cout << "Metadata for worker node was not added because " + errMsg << std::endl;
       } else {
          std::cout << "Metadata for worker node successfully added to catalog. "
                    << hostName << " | " << std::to_string(portValue) << " | " << nodeName << " | "
                    << std::endl;
       }

       numNodes++;
    }

    frontEnd.addFunctionality<pdb::DispatcherServer>(myLogger, statisticsDB);
    frontEnd.getFunctionality<pdb::DispatcherServer>().registerStorageNodes(allNodes);

    frontEnd.addFunctionality<pdb::QuerySchedulerServer>(
        port, myLogger, conf, statisticsDB, pseudoClusterMode, partitionToCoreRatio);
    frontEnd.startServer(nullptr);
}

#endif
