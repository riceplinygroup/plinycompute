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
#ifndef MASTER_SERVER_TEST_CC
#define MASTER_SERVER_TEST_CC

#include "PDBServer.h"
#include "CatalogServer.h"
#include "CatalogClient.h"
#include "ResourceManagerServer.h"
#include "DistributedStorageManagerServer.h"
#include "DispatcherServer.h"

/**
 * Starts up a server with all of the master functionalities and performs all the proper initializations for the various
 * servers
 *
 * Usage: ./MasterServer <port>
 */
int main (int argc, char * argv[]) {

    int port = 8108;
    if (argc >= 2) {
        port = atoi(argv[1]);
    }

    std::cout << "Starting up a master server" << std::endl;
    pdb::PDBLoggerPtr myLogger = make_shared<pdb::PDBLogger>("frontendLogFile.log");
    pdb::PDBServer frontEnd(port, 10, myLogger);
    frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir", true);
    frontEnd.addFunctionality<pdb::CatalogClient>(port, "localhost", myLogger);
    frontEnd.addFunctionality<pdb::ResourceManagerServer>("conf/serverlist", port);
    frontEnd.addFunctionality<pdb::DistributedStorageManagerServer>(myLogger);
    frontEnd.addFunctionality<pdb::DispatcherServer>(myLogger);
    // Register the nodes into the dispatcher
    auto allNodes = frontEnd.getFunctionality<pdb::ResourceManagerServer>().getAllNodes();
    frontEnd.getFunctionality<pdb::DispatcherServer>().registerStorageNodes(allNodes);
    frontEnd.startServer(nullptr);
}

#endif
