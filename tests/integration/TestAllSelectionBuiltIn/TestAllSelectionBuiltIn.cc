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

#ifndef TEST_81_BUILTIN_H
#define TEST_81_BUILTIN_H


// by Binhang, May 2017
// to test Write set for sharedLibray

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "PDBClient.h"
#include "ScanEmployeeSet.h"
#include "WriteBuiltinEmployeeSet.h"
#include "EmployeeBuiltInIdentitySelection.h"
#include "SharedEmployee.h"
#include "Employee.h"
#include "Set.h"
#include "DataTypes.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


using namespace pdb;
int main(int argc, char* argv[]) {
    bool printResult = true;
    bool clusterMode = false;
    std::cout << "Usage: #printResult[Y/N] #clusterMode[Y/N] #dataSize[MB] #managerIp #addData[Y/N]"
              << std::endl;
    if (argc > 1) {
        if (strcmp(argv[1], "N") == 0) {
            printResult = false;
            std::cout << "You successfully disabled printing result." << std::endl;
        } else {
            printResult = true;
            std::cout << "Will print result." << std::endl;
        }
    } else {
        std::cout << "Will print result. If you don't want to print result, you can add N as the "
                     "first parameter to disable result printing."
                  << std::endl;
    }

    if (argc > 2) {
        if (strcmp(argv[2], "Y") == 0) {
            clusterMode = true;
            std::cout << "You successfully set the test to run on cluster." << std::endl;
        } else {
            clusterMode = false;
        }
    } else {
        std::cout << "Will run on local node. If you want to run on cluster, you can add any "
                     "character as the second parameter to run on the cluster configured by "
                     "$PDB_HOME/conf/serverlist."
                  << std::endl;
    }

    int numOfMb = 1024;  // by default we add 1024MB data
    if (argc > 3) {
        numOfMb = atoi(argv[3]);
    }
    std::cout << "To add data with size: " << numOfMb << "MB" << std::endl;

    std::string managerIp = "localhost";
    if (argc > 4) {
        managerIp = argv[4];
    }
    std::cout << "Manager IP Address is " << managerIp << std::endl;

    bool whetherToAddData = true;
    if (argc > 5) {
        if (strcmp(argv[5], "N") == 0) {
            whetherToAddData = false;
        }
    }

    PDBClient pdbClient(8108, managerIp);

    string errMsg;

    if (whetherToAddData == true) {
        // Step 1. Create Database and Set
        // now, register a type for user data
        pdbClient.registerType("libraries/libSharedEmployee.so");

        // now, create a new database
        pdbClient.createDatabase("by8_db");

        // now, create a new set in that database
        pdbClient.createSet<SharedEmployee>("by8_db", "input_set");


        // Step 2. Add data

        int total = 0;
        if (numOfMb > 0) {
            int numIterations = numOfMb / 64;
            int remainder = numOfMb - 64 * numIterations;
            if (remainder > 0) {
                numIterations = numIterations + 1;
            }
            for (int num = 0; num < numIterations; num++) {
                int blockSize = 64;
                if ((num == numIterations - 1) && (remainder > 0)) {
                    blockSize = remainder;
                }
                pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                pdb::Handle<pdb::Vector<pdb::Handle<SharedEmployee>>> storeMe =
                    pdb::makeObject<pdb::Vector<pdb::Handle<SharedEmployee>>>();
                try {
                    for (int i = 0; true; i++) {
                        pdb::Handle<SharedEmployee> myData;
                        if (i % 100 == 0) {
                            myData = pdb::makeObject<SharedEmployee>("Frank", i);
                        } else {
                            myData = pdb::makeObject<SharedEmployee>("Joe Johnson" + to_string(i),
                                                                     i + 45);
                        }
                        storeMe->push_back(myData);
                        total++;
                    }
                } catch (pdb::NotEnoughSpace& n) {
                    pdbClient.sendData<SharedEmployee>(
                            std::pair<std::string, std::string>("input_set", "by8_db"),
                            storeMe);
                }
                PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std::endl;
            }

            std::cout << "total=" << total << std::endl;

            // to write back all buffered records
            pdbClient.flushData();
        }
    }
    // now, create a new set in that database to store output data
    PDB_COUT << "to create a new set for storing output data" << std::endl;
    pdbClient.createSet<Employee>("by8_db", "output_set");

    // Step 3. To execute a Query
    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};

    // register this query class
    pdbClient.registerType("libraries/libScanEmployeeSet.so");
    pdbClient.registerType("libraries/libEmployeeBuiltInIdentitySelection.so");
    pdbClient.registerType("libraries/libWriteBuiltinEmployeeSet.so");

    // connect to the query client
    Handle<Computation> myScanSet = makeObject<ScanEmployeeSet>("by8_db", "input_set");
    std::cout << "Scan declared successfully" << std::endl;
    Handle<Computation> myQuery = makeObject<EmployeeBuiltInIdentitySelection>();
    std::cout << "Selection declared successfully" << std::endl;
    myQuery->setInput(myScanSet);
    std::cout << "Selection input set successfully" << std::endl;
    Handle<Computation> myWriteSet = makeObject<WriteBuiltinEmployeeSet>("by8_db", "output_set");
    std::cout << "Write declared successfully" << std::endl;
    myWriteSet->setInput(myQuery);
    std::cout << "Write input set successfully" << std::endl;

    auto begin = std::chrono::high_resolution_clock::now();

    pdbClient.executeComputations(myWriteSet);
    std::cout << std::endl;

    auto end = std::chrono::high_resolution_clock::now();

    std::cout << std::endl;
    // print the resuts
    if (printResult == true) {
        std::cout << "to print result..." << std::endl;
        SetIterator<Employee> result = pdbClient.getSetIterator<Employee>("by8_db", "output_set");
        std::cout << "Query results: ";
        int count = 0;
        for (auto a : result) {
            count++;
            if(count % 1000 == 0) {
                std::cout << count << ":" << std::endl;
                    a->print();
            }
        }
        std::cout << "selection output count:" << count << "\n";
    }

    if (clusterMode == false) {
        // and delete the sets
        pdbClient.deleteSet("by8_db", "output_set");
    } else {
        pdbClient.removeSet("by8_db", "output_set");
    }
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
    std::cout << "Time Duration: "
              << std::chrono::duration_cast<std::chrono::duration<float>>(end - begin).count()
              << " secs." << std::endl;
}

#endif
