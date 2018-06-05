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
#ifndef TEST_LA_16_CC
#define TEST_LA_16_CC


// by Binhang, May 2017
// to test matrix colSum implemented by aggregation;
#include <ctime>
#include <chrono>

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "PDBClient.h"
#include "LAScanMatrixBlockSet.h"
#include "LAWriteMatrixBlockSet.h"
#include "MatrixBlock.h"
#include "Set.h"
#include "DataTypes.h"
#include "LAColSumAggregate.h"


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

    int numOfMb = 64;  // by default we add 64MB data
    if (argc > 3) {
        numOfMb = atoi(argv[3]);
    }
    numOfMb = 64;  // Force it to be 64 by now.


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
        // TODO: once sharedLibrary is supported, add this line back!!!
        pdbClient.registerType("libraries/libMatrixMeta.so");
        pdbClient.registerType("libraries/libMatrixData.so");
        pdbClient.registerType("libraries/libMatrixBlock.so");

        // now, create a new database
        pdbClient.createDatabase("LA17_db");

        // now, create a new set in that database
        pdbClient.createSet<MatrixBlock>("LA17_db", "LA_input_set");


        // Step 2. Add data

        int total = 0;
        if (numOfMb > 0) {
            int numIterations = numOfMb / 64;
            std::cout << "Number of MB: " << numOfMb << "Number of Iterations: " << numIterations
                      << std::endl;
            int remainder = numOfMb - 64 * numIterations;
            if (remainder > 0) {
                numIterations = numIterations + 1;
            }
            for (int num = 0; num < numIterations; num++) {
                std::cout << "Iterations: " << num << std::endl;
                int blockSize = 64;
                if ((num == numIterations - 1) && (remainder > 0)) {
                    blockSize = remainder;
                }
                pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                pdb::Handle<pdb::Vector<pdb::Handle<MatrixBlock>>> storeMe =
                    pdb::makeObject<pdb::Vector<pdb::Handle<MatrixBlock>>>();
                try {
                    // Write 100 Matrix of size 50 * 50
                    int matrixRowNums = 4;
                    int matrixColNums = 4;
                    int blockRowNums = 10;
                    int blockColNums = 5;
                    for (int i = 0; i < matrixRowNums; i++) {
                        for (int j = 0; j < matrixColNums; j++) {
                            pdb::Handle<MatrixBlock> myData =
                                pdb::makeObject<MatrixBlock>(i, j, blockRowNums, blockColNums);
                            // Foo initialization
                            for (int ii = 0; ii < blockRowNums; ii++) {
                                for (int jj = 0; jj < blockColNums; jj++) {
                                    (*(myData->getRawDataHandle()))[ii * blockColNums + jj] =
                                        i + j + ii + jj + 0.0;
                                }
                            }
                            std::cout << "New block: " << total << std::endl;
                            myData->print();
                            storeMe->push_back(myData);
                            total++;
                        }
                    }
                    for (int i = 0; i < storeMe->size(); i++) {
                        (*storeMe)[i]->print();
                    }
                    pdbClient.sendData<MatrixBlock>(
                            std::pair<std::string, std::string>("LA_input_set", "LA17_db"),
                            storeMe);
                } catch (pdb::NotEnoughSpace& n) {
                    pdbClient.sendData<MatrixBlock>(
                            std::pair<std::string, std::string>("LA_input_set", "LA17_db"),
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
    pdbClient.createSet<MatrixBlock>("LA17_db", "LA_colSum_set");

    // Step 3. To execute a Query
    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};

    // register this query class
    pdbClient.registerType("libraries/libLAColSumAggregate.so");
    pdbClient.registerType("libraries/libLAScanMatrixBlockSet.so");
    pdbClient.registerType("libraries/libLAWriteMatrixBlockSet.so");



    Handle<Computation> myScanSet = makeObject<LAScanMatrixBlockSet>("LA17_db", "LA_input_set");
    Handle<Computation> myQuery = makeObject<LAColSumAggregate>();
    myQuery->setInput(myScanSet);
    // myQuery->setOutput("LA17_db", "LA_colSum_set");

    Handle<Computation> myWriteSet = makeObject<LAWriteMatrixBlockSet>("LA17_db", "LA_colSum_set");
    myWriteSet->setInput(myQuery);

    auto begin = std::chrono::high_resolution_clock::now();

    pdbClient.executeComputations(myWriteSet);
    std::cout << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    // std::cout << "Time Duration: " <<
    //      std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." <<
    //      std::endl;

    std::cout << std::endl;
    // print the resuts
    if (printResult == true) {
        std::cout << "to print result..." << std::endl;
        SetIterator<MatrixBlock> input =
            pdbClient.getSetIterator<MatrixBlock>("LA17_db", "LA_input_set");
        std::cout << "Query input: " << std::endl;
        int countIn = 0;
        for (auto a : input) {
            countIn++;
            std::cout << countIn << ":";
            a->print();
            std::cout << std::endl;
        }
        std::cout << "Matrix input block nums:" << countIn << "\n";


        SetIterator<MatrixBlock> result =
            pdbClient.getSetIterator<MatrixBlock>("LA17_db", "LA_colSum_set");
        std::cout << "ColSum query results: " << std::endl;
        int countOut = 0;
        for (auto a : result) {
            countOut++;
            std::cout << countOut << ":";
            a->print();

            std::cout << std::endl;
        }
        std::cout << "ColSum output count:" << countOut << "\n";
    }

    if (clusterMode == false) {
        // and delete the sets
        pdbClient.deleteSet("LA17_db", "LA_colSum_set");
    } else {
        pdbClient.removeSet("LA17_db", "LA_colSum_set");
    }
    int code = system("scripts/cleanupSoFiles.sh force");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
    std::cout << "Time Duration: "
              << std::chrono::duration_cast<std::chrono::duration<float>>(end - begin).count()
              << " secs." << std::endl;
}

#endif
