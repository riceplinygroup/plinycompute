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

#ifndef TEST_401_CC
#define TEST_401_CC

#include "PDBServer.h"
#include "ResourceManagerServer.h"
#include "CatalogServer.h"
#include "CatalogClient.h"
#include "DispatcherServer.h"

/**
 * This program starts up a Dispatcher Server at the port specified by the program argument. Default value is 8108.
 */
int main (int argc, char * argv[]) {


    int port = 8108;
    if (argc >= 2) {
        port = atoi(argv[1]);
    }

    std::cout << "Starting up a dispatcher server\n";
    pdb::PDBLoggerPtr myLogger = make_shared<pdb::PDBLogger>("frontendLogFile.log");
    pdb::PDBServer frontEnd(port, 10, myLogger);
    frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir", false, "localhost", port);
    frontEnd.addFunctionality<pdb::CatalogClient>(port, "localhost", myLogger);
    frontEnd.addFunctionality<pdb::ResourceManagerServer>("conf/serverlist", port);
    auto allNodes = frontEnd.getFunctionality<pdb::ResourceManagerServer>().getAllNodes();
    frontEnd.addFunctionality<pdb::DispatcherServer>(myLogger);

    /*
    pdb::Handle<pdb::NodeDispatcherData> node1 = pdb::makeObject<pdb::NodeDispatcherData>(0, 8108, "localhost");
    pdb::Handle<pdb::NodeDispatcherData> node2 = pdb::makeObject<pdb::NodeDispatcherData>(1, 8109, "localhost");

    pdb::Handle<pdb::Vector<pdb::Handle<pdb::NodeDispatcherData>>>  allNodes =
            pdb::makeObject<pdb::Vector<pdb::Handle<pdb::NodeDispatcherData>>>();

    allNodes->push_back(node1);
    allNodes->push_back(node2);
     */

    frontEnd.getFunctionality<pdb::DispatcherServer>().registerStorageNodes(allNodes);

    std::cout << "Starting the dispatcher server at port " << port << std::endl;

    frontEnd.startServer(nullptr);

}

#endif
