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
#ifndef TEST_89_H
#define TEST_89_H


#include "Handle.h"
#include "Lambda.h"
#include "PDBClient.h"
#include "Supervisor.h"
#include "Employee.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "ScanSupervisorSet.h"
#include "DepartmentEmployees.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>
#include "SimpleGroupBy.h"


//to test iterative execution of aggregations

using namespace pdb;

int main(int argc, char* argv[]) {


    bool printResult = true;
    bool clusterMode = false;
    std::cout << "Usage: #printResult[Y/N] #clusterMode[Y/N] #dataSize[MB] #masterIp #addData[Y/N]"
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
            std::cout << "ERROR: cluster mode must be true" << std::endl;
            exit(1);
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

    std::string masterIp = "localhost";
    if (argc > 4) {
        masterIp = argv[4];
    }
    std::cout << "Master IP Address is " << masterIp << std::endl;

    bool whetherToAddData = true;
    if (argc > 5) {
        if (strcmp(argv[5], "N") == 0) {
            whetherToAddData = false;
        }
    }

    PDBClient pdbClient(8108, masterIp, false, true);

    string errMsg;

    if (whetherToAddData == true) {


        // now, create a new database
        pdbClient.createDatabase("test89_db");

        // now, create a new set in that database
        pdbClient.createSet<Supervisor>("test89_db", "test89_set");


        // Step 2. Add data

        int total = 0;
        if (numOfMb > 0) {
            int numIterations = numOfMb / 128;
            int remainder = numOfMb - 128 * numIterations;
            if (remainder > 0) {
                numIterations = numIterations + 1;
            }
            for (int num = 0; num < numIterations; num++) {
                int blockSize = 128;
                if ((num == numIterations - 1) && (remainder > 0)) {
                    blockSize = remainder;
                }
                makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                Handle<Vector<Handle<Supervisor>>> storeMe =
                    makeObject<Vector<Handle<Supervisor>>>();
                char first = 'A', second = 'B', third = 'C', fourth = 'D';
                char myString[5];
                myString[4] = 0;
                int i;
                try {
                    for (i = 0; true; i++) {

                        myString[0] = first;
                        myString[1] = second;
                        myString[2] = third;
                        myString[3] = fourth;

                        first++;
                        if (first == 'Z') {
                            first = 'A';
                            second++;
                            if (second == 'Z') {
                                second = 'A';
                                third++;
                                if (third == 'Z') {
                                    third = 'A';
                                    fourth++;
                                    if (fourth == 'Z') {
                                        fourth = 'A';
                                    }
                                }
                            }
                        }

                        Handle<Supervisor> myData = makeObject<Supervisor>(
                            "Steve Stevens", 20 + (i % 29), std::string(myString), 3.54);
                        storeMe->push_back(myData);
                        total++;
                        for (int j = 0; j < 10; j++) {
                            Handle<Employee> temp;
                            if (i % 3 == 0) {
                                temp = makeObject<Employee>("Steve Stevens",
                                                            20 + ((i + j) % 29),
                                                            std::string(myString),
                                                            3.54);
                            } else {
                                temp = makeObject<Employee>("Albert Albertson",
                                                            20 + ((i + j) % 29),
                                                            std::string(myString),
                                                            3.54);
                            }
                            (*storeMe)[i]->addEmp(temp);
                        }
                    }

                } catch (pdb::NotEnoughSpace& n) {
                    pdbClient.sendData<Supervisor>(
                            std::pair<std::string, std::string>("test89_set", "test89_db"),
                            storeMe);
                }
                PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std::endl;
            }

            std::cout << "total=" << total << std::endl;

            // to write back all buffered records
            pdbClient.flushData(errMsg);
        }
    }

    PDB_COUT << "to create a new set for storing output data" << std::endl;
    pdbClient.createSet<DepartmentEmployeeAges>("test89_db", "output_set");

    // this is the object allocation block where all of this stuff will reside
    // register this query class
    pdbClient.registerType("libraries/libScanSupervisorSet.so");
    pdbClient.registerType("libraries/libSimpleGroupBy.so");


    for (int i = 0; i < 10; i++) {
    
        const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};

        // create all of the computation objects
        Handle<Computation> myScanSet = makeObject<ScanSupervisorSet>("test89_db", "test89_set");
        Handle<Computation> myAgg = makeObject<SimpleGroupBy>("test89_db", "output_set");
        myAgg->setAllocatorPolicy(noReuseAllocator);
        myAgg->setInput(myScanSet);


        pdbClient.executeComputations(myAgg);
        std::cout << std::endl;


        std::cout << std::endl;

        // print the resuts of the output set
        if (printResult == true) {
            std::cout << "to print result..." << std::endl;
            SetIterator<DepartmentEmployeeAges> result =
                pdbClient.getSetIterator<DepartmentEmployeeAges>("test89_db", "output_set");
            std::cout << "Query results: ";
            int count = 0;
            for (auto a : result) {
                count++;
                std::cout << count << ":";
                a->print();
            }
            std::cout << "aggregation output count:" << count << "\n";
        }

        pdbClient.clearSet("test89_db", "output_set", "pdb::DepartmentEmployeeAges");
    }
    if (clusterMode == false) {
        // and delete the sets
        pdbClient.deleteSet("test89_db", "output_set");
    } else {
        pdbClient.removeSet("test89_db", "output_set");
    }
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
}


#endif
