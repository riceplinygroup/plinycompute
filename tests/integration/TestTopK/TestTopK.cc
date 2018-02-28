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

#ifndef TOP_K_CC
#define TOP_K_CC

#include "PDBDebug.h"
#include "PDBClient.h"
#include "TopKTest.h"
#include "EmpWithVector.h"
#include "ScanEmpWithVector.h"
#include "WriteEmpWithVector.h"
#include "QueryOutput.h"
#include "Set.h"

using namespace pdb;
int main(int argc, char* argv[]) {

    if (argc != 5) {
        std::cout << "Usage: #dataSizeToAdd[MB] #masterIP #registerSharedLibs[Y/N] ";
        std::cout << "#createDBAndSet[Y/N]\n";
        return 0;
    }

    // here is how many MB to add
    unsigned numOfMb = atoi(argv[1]);
    std::cout << "Will add " << numOfMb << " MB of data.\n";

    // the IP address of the master
    std::string masterIP = argv[2];
    std::cout << "IP address of master node is " << masterIP << "\n";

    // whether or not to add the shared libraries
    bool registerSharedLibs;
    if (strcmp(argv[3], "N") == 0) {
        registerSharedLibs = false;
        std::cout << "I will not regsiter the shared libraries.\n";
    } else {
        registerSharedLibs = true;
        std::cout << "And I will regsiter the shared libraries.\n";
    }

    // whether to create a new DB and set
    bool createNewDBAndSet;
    if (strcmp(argv[4], "N") == 0) {
        createNewDBAndSet = false;
        std::cout << "I will not create a new database and data set.\n";
    } else {
        createNewDBAndSet = true;
        std::cout << "And I will create a new database and data set.\n";
    }


    // for logging

    // for connecting to the catalog; necessary to use non-built-in PDB types
    PDBClient pdbClient(
            8108,
            masterIP,
            false,
            true);


    // if we register the types we are going to use to execute the query
    if (registerSharedLibs) {

        std::string errMsg;
        bool result = true;
        result = result && pdbClient.registerType("libraries/libEmpWithVector.so", errMsg);
        result = result && pdbClient.registerType("libraries/libTopKTest.so", errMsg);
        result = result && pdbClient.registerType("libraries/libScanEmpWithVector.so", errMsg);
        result = result && pdbClient.registerType("libraries/libWriteEmpWithVector.so", errMsg);
        if (!result) {
            std::cout << "Registering type failed: " << errMsg << "\n";
            exit(1);
        }
    }

    // if we add data
    std::string errMsg;

    // this will allow us to add data

    if (numOfMb > 0) {

        if (createNewDBAndSet) {

            if (!pdbClient.createDatabase("topK_db", errMsg)) {
                cout << "Not able to create database: " + errMsg;
                exit(-1);
            } else {
                cout << "Created database.\n";
            }

            if (!pdbClient.createSet<EmpWithVector>("topK_db", "topK_set", errMsg)) {
                cout << "Not able to create data set: " + errMsg;
                exit(-1);
            } else {
                cout << "Created data set.\n";
            }
        }

        for (int i = 0; i < numOfMb; i++) {

            // in each iteration, we will create a new allocation block
            pdb::makeObjectAllocatorBlock(1024 * 1024, true);
            pdb::Handle<pdb::Vector<pdb::Handle<EmpWithVector>>> storeMe =
                pdb::makeObject<pdb::Vector<pdb::Handle<EmpWithVector>>>();

            // now, fill it up
            int j = 0;
            while (true) {

                try {

                    // try to create another EmpWithVector object
                    Supervisor temp("Joe Johnson", 20 + (j % 29));
                    for (int k = 0; k < 10; k++) {
                        Handle<Employee> nextGuy =
                            makeObject<Employee>("Steve Stevens", 20 + ((j + k) % 29));
                        temp.addEmp(nextGuy);
                    }
                    Vector<double> myVec;
                    for (int k = 0; k < 10; k++) {
                        myVec.push_back(drand48());
                    }
                    pdb::Handle<EmpWithVector> addMe = makeObject<EmpWithVector>(temp, myVec);
                    storeMe->push_back(addMe);
                    j++;

                } catch (NotEnoughSpace& e) {

                    // add the next MB of data
                    if (!pdbClient.sendData<EmpWithVector>(
                            std::pair<std::string, std::string>("topK_set", "topK_db"),
                            storeMe,
                            errMsg)) {
                        std::cout << "Failed to send data to dispatcher server" << std::endl;
                        return -1;
                    } else {

                        std::cout << "Added " << j << " EmpWithVector objects to the database.\n";
                        break;
                    }
                }
            }
        }
    }

    // now we create the output set
    if (!pdbClient.removeSet("topK_db", "topKOutput_set", errMsg)) {
        cout << "Not able to delete output data set: " + errMsg;
    }
    if (!pdbClient.createSet<TopKQueue<double, Handle<EmpWithVector>>>(
            "topK_db", "topKOutput_set", errMsg)) {
        cout << "Not able to create output data set: " + errMsg;
    }

    // for building the query
    pdb::makeObjectAllocatorBlock(1024 * 1024, true);

    // here is the query vector
    Vector<double> query;
    for (int i = 0; i < 10; i++) {
        query.push_back(0.0);
    }

    // connect to the query client

    // make the query graph
    Handle<Computation> myInitialScanSet = makeObject<ScanEmpWithVector>("topK_db", "topK_set");
    Handle<Computation> myQuery = makeObject<TopKTest>(query, 10);
    myQuery->setInput(myInitialScanSet);
    Handle<Computation> myWriter = makeObject<WriteEmpWithVector>("topK_db", "topKOutput_set");
    myWriter->setInput(myQuery);

    // execute the query
    if (!pdbClient.executeComputations(errMsg, myWriter)) {
        std::cout << "Query failed. Message was: " << errMsg << "\n";
        return 0;
    }

    // now iterate through the result
    SetIterator<TopKQueue<double, Handle<EmpWithVector>>> result =
        pdbClient.getSetIterator<TopKQueue<double, Handle<EmpWithVector>>>("topK_db",
                                                                          "topKOutput_set");
    for (auto& a : result) {
        std::cout << "Got back " << a->size() << " items from the top-k query.\n";
        std::cout << "These items are:\n";
        for (int i = 0; i < a->size(); i++) {
            std::cout << "score: " << (*a)[i].getScore() << "\n";
            std::cout << "vector: ";
            for (int j = 0; j < (*a)[i].getValue()->getVector().size(); j++) {
                std::cout << ((*a)[i].getValue()->getVector())[j] << " ";
            }
            std::cout << "\nemp ";
            (*a)[i].getValue()->getEmp().print();
            std::cout << "\n\n";
        }
    }

    // now, remove the output set
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
}

#endif
