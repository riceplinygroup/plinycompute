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
#ifndef TEST_403_CC
#define TEST_403_CC

#include "DispatcherClient.h"
#include "UseTemporaryAllocationBlock.h"
#include "SharedEmployee.h"
#include "StorageClient.h"
#include "ResourceManagerServer.h"

#include <iostream>

/**
 * Test: Dispatcher Server and Client
 *
 * Prerequisites: Have the DispatcherServer (Test401) running at some Port B. Have at least one StorageServer (Test28)
 *                running at some port A.  Make sure that both of these Server's have completely clean CatalogDir's, else
 *                the program may segfault.
 *
 * Args: ./bin/Test401 dispatcher_port [storage_node_port ... ]
 *
 */

int main (int argc, char * argv[]) {


    const std::string DISPATCHER_IP = "10.134.96.45";
    const int PORT = 8108;

    std:: cout << "Make sure to run bin/test28 in a different window to provide a catalog/storage server.\n";

    // TODO: Add in a resourcemanager, so that we can read in the proper IP addresses.

    bool usePangaea = true;

    pdb::CatalogClient catalogClient = pdb::CatalogClient(PORT, DISPATCHER_IP, make_shared<pdb::PDBLogger>("clientLog"));

    // Register the new types on the dispatcher
    {
        pdb::StorageClient temp(PORT, DISPATCHER_IP, make_shared<pdb::PDBLogger>("clientLog"), usePangaea);

        string errMsg;
        std::cout << "Registering types for dispatcher node at port: " << DISPATCHER_IP << std::endl;

        if (!temp.registerType("libraries/libSharedEmployee.so", errMsg)) {
            cout << "Not able to register type: " + errMsg << std::endl;
        } else {
            cout << "Registered type.\n";
        }
    }

    pdb::ResourceManagerServer resourceManagerServer = pdb::ResourceManagerServer("conf/serverlist", PORT);
    auto allNodes = resourceManagerServer.getAllNodes();

    for (int i = 0; i < allNodes->size(); i++) {

        auto node = (* allNodes)[i];

        std::cout << "Registering types for storage node at port: " << node->getAddress() << ":" << node->getPort() << std::endl;

        pdb :: StorageClient temp (node->getPort(), node->getAddress(), make_shared <pdb::PDBLogger> ("TempStorageClientLog"), usePangaea);
        string errMsg;

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

    pdb::DispatcherClient temp = pdb::DispatcherClient(PORT, DISPATCHER_IP, make_shared <pdb :: PDBLogger> ("Test403log"));

    void *storage = malloc (96 * 1024);
    // const pdb::UseTemporaryAllocationBlock tempBlock {storage, 1024 * 96, true};
    pdb :: makeObjectAllocatorBlock(storage, 1024 * 96, true);

    {
        // pdb :: makeObjectAllocatorBlock (storage, 1024 * 8, true);
        pdb::Handle<pdb::Vector<pdb::Handle<SharedEmployee>>> storeMe =
                pdb::makeObject<pdb::Vector<pdb::Handle<SharedEmployee>>> ();
        try {
            for (int i = 0; i < 512; i++) {
                pdb :: Handle <SharedEmployee> myData =
                        pdb::makeObject <SharedEmployee> ("Joe Johnson" + to_string (i), i + 45);
                storeMe->push_back (myData);
            }
        } catch (pdb :: NotEnoughSpace &n) {

        }
        std::string err;
        for (int i = 0; i < 10; i++) {
            std::cout << "Dispatching a vector of size " <<  storeMe->size() << std::endl;
            if (!temp.sendData<SharedEmployee>(std::pair<std::string, std::string>("dispatch_test_set", "dispatch_test_db"), storeMe, err)) {
                std::cout << "Failed to send data to dispatcher server" << std::endl;
                return 1;
            }
        }
    }
    free(storage);

    // Shut down the Storage Nodes to flush their data
    for (int i = 0; i < allNodes->size(); i++) {
        auto node = (* allNodes)[i];
        pdb::StorageClient temp2(node->getPort(), node->getAddress(), make_shared<pdb::PDBLogger>("TempStorageClientLog"), usePangaea);
        string errMsg;
        if (!temp2.shutDownServer (errMsg)) {
            std::cout << "Shut down not clean: " << errMsg << "\n";
        } else {
            std::cout << "Shut down " << node->getPort() << "\n";
        }
    }
}

#endif