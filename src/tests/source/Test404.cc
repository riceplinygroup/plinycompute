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
#include "DistributedStorageManagerServer.h"

#include "StorageAddDatabase.h"
#include "SharedEmployee.h"

int main (int argc, char * argv[]) {
    int port = 8108;
    if (argc >= 2) {
        port = atoi(argv[1]);
    }

    std::cout << "Starting up a distributed storage manager server\n";
    pdb::PDBLoggerPtr myLogger = make_shared<pdb::PDBLogger>("frontendLogFile.log");
    pdb::PDBServer frontEnd(port, 10, myLogger);
    frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir", true);
    frontEnd.addFunctionality<pdb::CatalogClient>(port, "localhost", myLogger);
    frontEnd.addFunctionality<pdb::ResourceManagerServer>("conf/serverlist", port);
    frontEnd.addFunctionality<pdb::DistributedStorageManagerServer>(myLogger);
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
