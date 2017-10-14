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
#ifndef TEST_405_CC
#define TEST_405_CC

#include "PDBServer.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "CatalogClient.h"

#include "SharedEmployee.h"
#include "StorageAddDatabase.h"

int main(int argc, char* argv[]) {
    int port = 8108;
    if (argc < 3) {
        std::cout << "Please specify a port and cmd: ./Test405 <port> <cmd>" << endl;
        return 1;
    }
    port = atoi(argv[1]);
    pdb::PDBLoggerPtr logger = make_shared<pdb::PDBLogger>("frontendLogFile.log");
    pdb::DistributedStorageManagerClient client =
        pdb::DistributedStorageManagerClient(port, "localhost", logger);

    // Add this in so the dispatcher can build the correct type
    pdb::CatalogClient catalogClient = pdb::CatalogClient(port, "localhost", logger);

    std::string err;

    if (argv[2][0] == 't') {
        pdb::CatalogClient catalogClient = pdb::CatalogClient(port, "localhost", logger);
        if (!catalogClient.registerType("libraries/libSharedEmployee.so", err)) {
            std::cout << "Not able to register type: " << err << std::endl;
        } else {
            std::cout << "Registered type" << std::endl;
        }
    }

    if (argv[2][0] == 'd') {
        if (client.createDatabase("joseph_db", err)) {
            std::cout << "Success" << std::endl;
            return 0;
        } else {
            std::cout << "Failure: " << err << std::endl;
            return 1;
        }
    }

    if (argv[2][0] == 's') {
        if (client.createSet("joseph_db", "joseph_set", "SharedEmployee", err)) {
            std::cout << "Success" << std::endl;
            return 0;
        } else {
            std::cout << "Failure: " << err << std::endl;
            return 1;
        }
    }

    if (argv[2][0] == 'r') {
        if (client.removeDatabase("joseph_db", err)) {
            std::cout << "Success" << std::endl;
            return 0;
        } else {
            std::cout << "Failure: " << err << std::endl;
            return 1;
        }
    }

    if (argv[2][0] == 'm') {
        if (client.removeSet("joseph_db", "joseph_set", err)) {
            std::cout << "Success" << std::endl;
            return 0;
        } else {
            std::cout << "Failure: " << err << std::endl;
            return 1;
        }
    }

    if (argv[2][0] == 'e') {

        pdb::DispatcherClient dispatcherClient =
            pdb::DispatcherClient(port, "localhost", make_shared<pdb::PDBLogger>("Test405log"));
        // dispatcherClient.registerSet(std::pair<std::string, std::string>("joseph_set",
        // "joseph_db"), pdb::PartitionPolicy::Policy::RANDOM, err);

        void* storage = malloc(96 * 1024 * 1024);
        pdb::makeObjectAllocatorBlock(storage, 96 * 1024 * 1024, true);

        pdb::Handle<SharedEmployee> test = pdb::makeObject<SharedEmployee>("Joe Johnson1", 45);
        std::cout << test.getTypeCode() << std::endl;

        {
            pdb::Handle<pdb::Vector<pdb::Handle<SharedEmployee>>> storeMe =
                pdb::makeObject<pdb::Vector<pdb::Handle<SharedEmployee>>>();
            try {
                for (int i = 0; i < 50000; i++) {
                    pdb::Handle<SharedEmployee> myData =
                        pdb::makeObject<SharedEmployee>("Joe Johnson" + to_string(i), i + 45);
                    storeMe->push_back(myData);
                }
            } catch (pdb::NotEnoughSpace& n) {
            }
            for (int i = 0; i < 10; i++) {
                std::cout << "Dispatching a vector of size " << storeMe->size() << std::endl;
                if (!dispatcherClient.sendData<SharedEmployee>(
                        std::pair<std::string, std::string>("joseph_set", "joseph_db"),
                        storeMe,
                        err)) {
                    std::cout << "Failed to send data to dispatcher server" << std::endl;
                    return 1;
                }
            }
        }
        free(storage);
    }
}

#endif
