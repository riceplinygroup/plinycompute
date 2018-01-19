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
    std::string masterIp;
    std::string pemFile = "conf/pdb.key";
    bool pseudoClusterMode = false;
    double partitionToCoreRatio = 0.75;
    if (argc == 3) {
        masterIp = argv[1];
        port = atoi(argv[2]);
    } else if ((argc == 4) || (argc == 5) || (argc == 6)) {
        masterIp = argv[1];
        port = atoi(argv[2]);
        std::string isPseudoStr(argv[3]);
        if (isPseudoStr.compare(std::string("Y")) == 0) {
            pseudoClusterMode = true;
            std::cout << "Running in pseudo cluster mode" << std::endl;
        }
        if ((argc == 5) || (argc == 6)) {
            pemFile = argv[4];
        }
        if (argc == 6) {
            partitionToCoreRatio = stod(argv[5]);
        }

    } else {
        std::cout << "[Usage] #masterIp #port #runPseudoClusterOnOneNode (Y for running a "
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

    frontEnd.addFunctionality<pdb::CatalogServer>("CatalogDir", true, masterIp, port);
    frontEnd.addFunctionality<pdb::CatalogClient>(port, "localhost", myLogger);

    // to register node metadata
    std::string errMsg = " ";
    pdb::Handle<pdb::CatalogNodeMetadata> nodeData =
        pdb::makeObject<pdb::CatalogNodeMetadata>(String("localhost:" + std::to_string(port)),
                                                  String("localhost"),
                                                  port,
                                                  String("master"),
                                                  String("master"),
                                                  1);
    if (!frontEnd.getFunctionality<pdb::CatalogServer>().addNodeMetadata(nodeData, errMsg)) {
        std::cout << "Node metadata was not added because " + errMsg << std::endl;
    } else {
        std::cout << "Node metadata successfully added." << std::endl;
    }

    string serverListFile = "";

    if (pseudoClusterMode==true) serverListFile = "conf/serverlist.test";
    else serverListFile = "conf/serverlist";

    std::ifstream infile;
    infile.open(serverListFile.c_str());

    int numNodes = 1;
    string line = "";
    string nodeName = "";
    string hostName = "";
    int portValue = 8108;

    if (infile.is_open()) {
        while (getline(infile, line)) {

           std::size_t pos = line.find(":");

           if (pos!=std::string::npos) {
              hostName = line.substr(0, pos);
              portValue = std::atoi(line.substr(pos+1,line.length()-1).c_str());
           } else {
              hostName = line;
              portValue = 8108;
           }

           nodeName = "worker_" + std::to_string(numNodes);
           makeObjectAllocatorBlock(4 * 1024 * 1024, true);
           pdb::Handle<pdb::CatalogNodeMetadata> workerNode =
               pdb::makeObject<pdb::CatalogNodeMetadata>(String(hostName + ":" + std::to_string(portValue)),
                                                  String(hostName),
                                                  portValue,
                                                  String(nodeName),
                                                  String("worker"),
                                                  1);
           if (!frontEnd.getFunctionality<pdb::CatalogServer>().addNodeMetadata(workerNode, errMsg)) {
               std::cout << "Node metadata was not added because " + errMsg << std::endl;
           } else {
               std::cout << "Node metadata successfully added." 
                         << hostName << " | " << std::to_string(portValue) << " | " << nodeName << " | "
                         << std::endl;
           }
           numNodes++;
       }
    } else {
        cout << "conf/serverlist file not found." << endl;
    }
    infile.close();
    infile.clear();
 
    frontEnd.addFunctionality<pdb::ResourceManagerServer>(
        "conf/serverlist", port, pseudoClusterMode, pemFile);
    frontEnd.addFunctionality<pdb::DistributedStorageManagerServer>(myLogger);
    auto allNodes = frontEnd.getFunctionality<pdb::ResourceManagerServer>().getAllNodes();
    frontEnd.addFunctionality<pdb::DispatcherServer>(myLogger);
    frontEnd.getFunctionality<pdb::DispatcherServer>().registerStorageNodes(allNodes);

    frontEnd.addFunctionality<pdb::QuerySchedulerServer>(
        port, myLogger, conf, pseudoClusterMode, partitionToCoreRatio);
    frontEnd.startServer(nullptr);
}

#endif
