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
#ifndef TEST_404_CC
#define TEST_404_CC

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

int main (int argc, char * argv[]) {
    int port = 8108;
    string masterIp;
    if (argc >= 3) {
        masterIp = argv[1];
        port = atoi(argv[2]);
    } else {
        std :: cout << "[Usage] #masterIp #port" << std :: endl;
        exit (-1);
    }
  
    std::cout << "Starting up a distributed storage manager server\n";
    pdb::PDBLoggerPtr myLogger = make_shared<pdb::PDBLogger>("frontendLogFile.log");
    pdb::PDBServer frontEnd(port, 100, myLogger);
    
    ConfigurationPtr conf = make_shared < Configuration > ();
    SharedMemPtr shm = make_shared< SharedMem > (conf->getShmSize(), myLogger);
    conf->printOut();
    
    frontEnd.addFunctionality <pdb :: CatalogServer> ("/tmp/CatalogDir", true, masterIp, port);
    frontEnd.addFunctionality<pdb::CatalogClient>(port, "localhost", myLogger);

    // to register this node
    string nodeName = "master";
    string nodeType = "master";
/*    pdb :: Handle<pdb :: CatalogNodeMetadata> nodeData = pdb :: makeObject<pdb :: CatalogNodeMetadata>(String("localhost:" + std::to_string(port)), String("localhost"), port, String(nodeName), String(nodeType), 1);
    std :: string errMsg;
    if (!frontEnd.getFunctionality<pdb::CatalogClient>().registerNodeMetadata (nodeData, errMsg)) {
             std :: cout << "Not able to register node metadata: " + errMsg << std::endl;
             std :: cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status."<<std::endl;
    } else {
             std :: cout << "Node metadata successfully added. \n";
    }
*/
    frontEnd.addFunctionality<pdb::ResourceManagerServer>("conf/serverlist", port);
    frontEnd.addFunctionality<pdb::DistributedStorageManagerServer>(myLogger);

    auto allNodes = frontEnd.getFunctionality<pdb::ResourceManagerServer>().getAllNodes();
    frontEnd.addFunctionality<pdb::DispatcherServer>(myLogger);
    frontEnd.getFunctionality<pdb::DispatcherServer>().registerStorageNodes(allNodes);
    
    frontEnd.addFunctionality<pdb::QuerySchedulerServer>(myLogger);
    frontEnd.startServer(nullptr);

    /*
    string nodeName = "master1";
    string nodeType = "master";

    bool isMasterCatalog = true;

    pdb :: Handle<pdb :: CatalogNodeMetadata> nodeData = pdb :: makeObject<pdb :: CatalogNodeMetadata>(String("localhost:" + std::to_string(port)), String("localhost"), port, String(nodeName), String(nodeType), 1);

    std::string errMsg;
    // if it's not the master catalog node, use a client to remotely register this node metadata
    if ( isMasterCatalog == false ) {
        pdb :: CatalogClient catClient (port, "localhost", make_shared <pdb :: PDBLogger> ("clientCatalogLog"));

        if (!catClient.registerNodeMetadata (nodeData, errMsg)) {
            std :: cout << "Not able to register node metadata: " + errMsg << std::endl;
            std :: cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status."<<std::endl;
        } else {
            std :: cout << "Node metadata successfully added.\n";
        }
    } else {
        // if it's the master catalog node, register the metadata in the local catalog
        if (frontEnd.getFunctionality<pdb::CatalogServer>().addNodeMetadata( nodeData, errMsg)){
            std :: cout << "Not able to register node metadata: " + errMsg << std::endl;
            std :: cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status."<<std::endl;
        } else {
            std :: cout << "Node metadata successfully added.\n";
        }
    }
     */
}

#endif
