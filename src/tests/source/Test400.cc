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
#ifndef TEST_400_CC
#define TEST_400_CC

#include "UseTemporaryAllocationBlock.h"
#include "DispatcherServer.h"
#include "NodeDispatcherData.h"
#include "RandomPolicy.h"
#include "TypeName.h"

#include <iostream>

#include "SharedEmployee.h"

/**
 * Test that runs with Test23. It tests running a FairPolicy dispatcher for a cluster of 3 storage nodes. These nodes
 * must be run at IP address 8108, 8109, and 8110.
 */

int main (int argc, char * argv[]) {

    std:: cout << "Make sure to run bin/test23 in a different window to provide a catalog/storage server.\n";

    bool usePangaea = true;

    int numPorts = argc == 2 ? 1 : argc - 1;
    int ports[numPorts];

    if (argc < 2) {
        ports[0] = 8108;
        std::cout << "new port at 8108" << std::endl;
    } else {
        for (int i = 0; i < numPorts; i++) {
            ports[i] = atoi(argv[i + 1]);
            std::cout << "new port at " << ports[i] << std::endl;
        }
    }

    for (int i = 0; i < numPorts; i++) {
        int port = ports[i];

        std::cout << "Registering types for storage node at port: " << port << std::endl;

        pdb :: StorageClient temp (port, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), usePangaea);
        string errMsg;
        // Register the shared employee class

        if (!temp.registerType ("libraries/libSharedEmployee.so", errMsg)) {
            cout << "Not able to register type: " + errMsg<<std::endl;
        } else {
            cout << "Registered type.\n";
        }

        // Now, create a new database
        if (!temp.createDatabase ("dispatch_test_db", errMsg)) {
            cout << "Not able to create database: " + errMsg;
        } else {
            cout << "Created database.\n";
        }

        // Now, create a new set in that database
        if (!temp.createSet <SharedEmployee> ("dispatch_test_db", "dispatch_test_set", errMsg)) {
            cout << "Not able to create set: " + errMsg;
        } else {
            cout << "Created set.\n";
        }
    }


    // TODO: Is this causing errors
    const pdb::UseTemporaryAllocationBlock tempBlock {1024 * 96};


    auto logger = make_shared <pdb :: PDBLogger> ("Test400log");
    pdb::DispatcherServer dispatcherServer = pdb::DispatcherServer(logger);

    /*
    pdb::RandomPolicyPtr policy = std::make_shared<pdb::RandomPolicy>();

    // Register a set with the dispatcher server
    dispatcherServer.registerSet(std::pair<std::string, std::string>("dispatch_test_set", "dispatch_test_db"), policy);
     */

    pdb::Handle<pdb::NodeDispatcherData> node1 = pdb::makeObject<pdb::NodeDispatcherData>(0, 8108, "localhost");
    pdb::Handle<pdb::NodeDispatcherData> node2 = pdb::makeObject<pdb::NodeDispatcherData>(1, 8109, "localhost");

    pdb::Handle<pdb::Vector<pdb::Handle<pdb::NodeDispatcherData>>>  newNodes =
            pdb::makeObject<pdb::Vector<pdb::Handle<pdb::NodeDispatcherData>>>();

    newNodes->push_back(node1);
    newNodes->push_back(node2);
    // Register two nodes with the dispatcher
    dispatcherServer.registerStorageNodes(newNodes);
    std::string typeName;

    // now, create a bunch of data
    // void *storage = malloc (1024 * 8);
    {
        // pdb :: makeObjectAllocatorBlock (storage, 1024 * 8, true);
        pdb :: Handle <pdb :: Vector <pdb :: Handle <pdb :: Object>>> storeMe = pdb :: makeObject <pdb :: Vector <pdb :: Handle <pdb::Object>>> ();
        try {

            for (int i = 0; true; i++) {
                pdb :: Handle <SharedEmployee> myData = pdb :: makeObject <SharedEmployee> ("Joe Johnson" + to_string (i), i + 45);
                typeName = pdb :: getTypeName<SharedEmployee>();
                storeMe->push_back (myData);
            }

        } catch (pdb :: NotEnoughSpace &n) {
            std::string err;
            for (int i = 0; i < 10; i++) {
                // TODO: Place this line back in

                dispatcherServer.dispatchData(std::pair<std::string, std::string>("dispatch_test_set", "dispatch_test_db"), typeName, storeMe);
            }
        }
    }

    for (int i = 0; i < numPorts; i++) {
        int port = ports[i];
        pdb::StorageClient temp(port, "localhost", make_shared<pdb::PDBLogger>("clientLog"), usePangaea);
        string errMsg;
        if (!temp.shutDownServer (errMsg)) {
            std::cout << "Shut down not clean: " << errMsg << "\n";
        }
    }

    // TODO: Use round robin policy with multiple StorageServers and check the availability of the data

    // TODO: It might be easier to Unit Test all of these functionalities. Unit Test RoundRobinPolicy.h and FairPolicy.h
}

#endif
