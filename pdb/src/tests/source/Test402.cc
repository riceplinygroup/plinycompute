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
#ifndef TEST_402_CC
#define TEST_402_CC

#include "DispatcherClient.h"
#include "UseTemporaryAllocationBlock.h"
#include "SharedEmployee.h"
#include "StorageClient.h"

#include "PartitionPolicy.h"

/**
 * Test: Dispatcher Server and Client
 *
 * Prerequisites: Have the DispatcherServer (Test401) running at some Port B. Have at least one
 * StorageServer (Test28)
 *                running at some port A.  Make sure that both of these Server's have completely
 * clean CatalogDir's, else
 *                the program may segfault.
 *
 * Args: ./bin/Test401 dispatcher_port [storage_node_port ... ]
 *
 */

int main(int argc, char* argv[]) {

    std::string err;

    std::cout << "Make sure to run bin/test28 in a different window to provide a catalog/storage "
                 "server.\n";

    // TODO: Add in a resourcemanager, so that we can read in the proper IP addresses.

    bool usePangaea = true;

    int dispatchPort = atoi(argv[1]);
    int numPorts = argc - 2;
    int ports[numPorts];

    pdb::CatalogClient catalogClient =
        pdb::CatalogClient(dispatchPort, "localhost", make_shared<pdb::PDBLogger>("clientLog"));
    pdb::theVTable->setCatalogClient(&catalogClient);

    if (argc == 1) {
        std::cout << "See Test402 usage: ./bin/Test401 dispatcher_port [storage_node_port ... ]"
                  << std::endl;
        return 1;
    }

    for (int i = 0; i < numPorts; i++) {
        ports[i] = atoi(argv[i + 2]);
    }

    {
        pdb::StorageClient temp(
            dispatchPort, "localhost", make_shared<pdb::PDBLogger>("clientLog"), usePangaea);

        string errMsg;
        std::cout << "Registering types for dispatcher node at port: " << dispatchPort << std::endl;

        if (!temp.registerType("libraries/libSharedEmployee.so", errMsg)) {
            cout << "Not able to register type: " + errMsg << std::endl;
        } else {
            cout << "Registered type.\n";
        }
    }

    for (int i = 0; i < numPorts; i++) {
        int port = ports[i];

        std::cout << "Registering types for storage node at port: " << port << std::endl;

        pdb::StorageClient temp(
            port, "localhost", make_shared<pdb::PDBLogger>("clientLog"), usePangaea);
        string errMsg;

        if (!temp.registerType("libraries/libSharedEmployee.so", errMsg)) {
            cout << "Not able to register type: " + errMsg << std::endl;
        } else {
            cout << "Registered type.\n";
        }

        // Now, create a new database
        if (!temp.createDatabase("dispatch_test_db", errMsg)) {
            cout << "Not able to create database: " + errMsg;
        } else {
            cout << "Created database.\n";
        }

        // Now, create a new set in that database
        if (!temp.createSet<SharedEmployee>("dispatch_test_db", "dispatch_test_set", errMsg)) {
            cout << "Not able to create set: " + errMsg;
        } else {
            cout << "Created set.\n";
        }
    }

    pdb::DispatcherClient temp =
        pdb::DispatcherClient(dispatchPort, "localhost", make_shared<pdb::PDBLogger>("Test402log"));

    temp.registerSet(std::pair<std::string, std::string>("dispatch_test_set", "dispatch_test_db"),
                     pdb::PartitionPolicy::Policy::RANDOM,
                     err);
    void* storage = malloc(96 * 1024);
    pdb::makeObjectAllocatorBlock(storage, 1024 * 96, true);

    {
        pdb::Handle<pdb::Vector<pdb::Handle<pdb::Object>>> storeMe =
            pdb::makeObject<pdb::Vector<pdb::Handle<pdb::Object>>>();
        try {
            for (int i = 0; i < 512; i++) {
                pdb::Handle<SharedEmployee> myData =
                    pdb::makeObject<SharedEmployee>("Joe Johnson" + to_string(i), i + 45);
                storeMe->push_back(myData);
            }
        } catch (pdb::NotEnoughSpace& n) {
        }
        for (int i = 0; i < 10; i++) {
            std::cout << "Dispatching a vector of size " << storeMe->size() << std::endl;
            if (!temp.sendData<pdb::Object>(
                    std::pair<std::string, std::string>("dispatch_test_set", "dispatch_test_db"),
                    storeMe,
                    err)) {
                std::cout << "Failed to send data to dispatcher server" << std::endl;
                return 1;
            }
        }
    }
    free(storage);

    // Shut down the Storage Nodes to flush their data
    for (int i = 0; i < numPorts; i++) {
        int port = ports[i];
        pdb::StorageClient temp2(
            port, "localhost", make_shared<pdb::PDBLogger>("clientLog"), usePangaea);
        string errMsg;
        if (!temp2.shutDownServer(errMsg)) {
            std::cout << "Shut down not clean: " << errMsg << "\n";
        } else {
            std::cout << "Shut down " << port << "\n";
        }
    }
}

#endif