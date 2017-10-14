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
#ifndef TEST_KMEANS_MLLIB_COMPLIANT_CC
#define TEST_KMEANS_MLLIB_COMPLIANT_CC

// JiaNote: this file is based on functions developed by Shangyu, but just a bit refactored to be
// fully compliant with MLLib
// K-means clustering;

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"

#include "Sampler.h"

#include "WriteKMeansSet.h"
#include "KMeansAggregate.h"
#include "KMeansDataCountAggregate.h"
#include "ScanDoubleVectorSet.h"
#include "ScanDoubleArraySet.h"
#include "KMeansAggregateOutputType.h"
#include "KMeansCentroid.h"
#include "KMeansDataCountAggregate.h"
#include "KMeansSampleSelection.h"
#include "KMeansNormVectorMap.h"
#include "WriteDoubleVectorSet.h"

#include "DispatcherClient.h"
#include "Set.h"
#include "DataTypes.h"
#include "DoubleVector.h"
#include "SumResult.h"
#include "WriteSumResultSet.h"

#include <ctime>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>
#include <cstddef>
#include <iostream>
#include <fstream>
#include <math.h>
#include <random>
#include <sstream>
#include <vector>

#define COUT std::cout
//#define COUT term

using namespace pdb;

#ifndef NUM_KMEANS_DIMENSIONS
#define NUM_KMEANS_DIMENSIONS 1000
#endif

#ifndef KMEANS_CONVERGE_THRESHOLD
#define KMEANS_CONVERGE_THRESHOLD 0.00001
#endif
int main(int argc, char* argv[]) {
    bool printResult = true;
    bool clusterMode = false;
    //    freopen("output.txt","w",stdout);
    freopen("/dev/tty", "w", stdout);

    //    std::ofstream out("output.txt");
    //    std::streambuf *COUTbuf = COUT.rdbuf(); //save old buf
    //    COUT.rdbuf(out.rdbuf()); //redirect std::COUT to out.txt!

    //    std::ofstream term("/dev/tty", std::ios_base::out);

    const std::string red("\033[0;31m");
    const std::string green("\033[1;32m");
    const std::string yellow("\033[1;33m");
    const std::string blue("\033[1;34m");
    const std::string cyan("\033[0;36m");
    const std::string magenta("\033[0;35m");
    const std::string reset("\033[0m");

    if (argc <= 5) {
        double eps = 1.0;
        while ((1.0 + (eps / 2.0)) != 1.0) {
            eps = eps / 2.0;
        }
        std::cout << "EPSILON=" << eps << std::endl;
        return 0;
    }
    COUT << "Usage: #printResult[Y/N] #clusterMode[Y/N] #dataSize[MB] #masterIp #addData[Y/N]"
         << std::endl;
    if (argc > 1) {
        if (strcmp(argv[1], "N") == 0) {
            printResult = false;
            COUT << "You successfully disabled printing result." << std::endl;
        } else {
            printResult = true;
            COUT << "Will print result." << std::endl;
        }
    } else {
        COUT << "Will print result. If you don't want to print result, you can add N as the first "
                "parameter to disable result printing."
             << std::endl;
    }

    if (argc > 2) {
        if (strcmp(argv[2], "Y") == 0) {
            clusterMode = true;
            COUT << "You successfully set the test to run on cluster." << std::endl;
        } else {
            clusterMode = false;
        }
    } else {
        COUT << "Will run on local node. If you want to run on cluster, you can add any character "
                "as the second parameter to run on the cluster configured by "
                "$PDB_HOME/conf/serverlist."
             << std::endl;
    }

    int numOfMb = 64;  // by default we add 64MB data
    if (argc > 3) {
        numOfMb = atoi(argv[3]);
    }
    numOfMb = 64;  // Force it to be 64 by now.


    COUT << "To add data with size: " << numOfMb << "MB" << std::endl;

    std::string masterIp = "localhost";
    if (argc > 4) {
        masterIp = argv[4];
    }
    COUT << "Master IP Address is " << masterIp << std::endl;

    bool whetherToAddData = true;
    if (argc > 5) {
        if (strcmp(argv[5], "N") == 0) {
            whetherToAddData = false;
        }
    }


    COUT << blue << std::endl;
    COUT << "*****************************************" << std::endl;
    COUT << "K-means starts : " << std::endl;
    COUT << "*****************************************" << std::endl;
    COUT << reset << std::endl;


    COUT << "The K-means paramers are: " << std::endl;
    COUT << std::endl;

    int iter = 1;
    int k = 3;
    int dim = 3;
    int numData = 10;
    bool addDataFromFile = true;

    if (argc > 6) {
        iter = std::stoi(argv[6]);
    }
    COUT << "The number of iterations: " << iter << std::endl;

    if (argc > 7) {
        k = std::stoi(argv[7]);
    }
    COUT << "The number of clusters: " << k << std::endl;

    /*
    if (argc > 8) {
    numData = std::stoi(argv[8]);
    }
    COUT << "The number of data points: " << numData << std :: endl;
    */

    if (argc > 8) {
        dim = std::stoi(argv[8]);
    }
    COUT << "The dimension of each data point: " << dim << std::endl;

    std::string fileName = "/mnt/data_generator_kmeans/gaussian_pdb/kmeans_data";
    if (argc > 9) {
        fileName = argv[9];
    }
    COUT << "Input file: " << fileName << std::endl;
    COUT << std::endl;


    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

    pdb::DistributedStorageManagerClient temp(8108, masterIp, clientLogger);

    pdb::CatalogClient catalogClient(8108, masterIp, clientLogger);

    string errMsg;

    // Some meta data
    pdb::makeObjectAllocatorBlock(1 * 1024 * 1024, true);

    if ((numOfMb > 0) && (whetherToAddData == false)) {
        // Step 1. Create Database and Set
        // now, register a type for user data
        // TODO: once sharedLibrary is supported, add this line back!!!
        catalogClient.registerType("libraries/libKMeansAggregateOutputType.so", errMsg);
        catalogClient.registerType("libraries/libKMeansCentroid.so", errMsg);
        catalogClient.registerType("libraries/libWriteSumResultSet.so", errMsg);
        // register this query class
        catalogClient.registerType("libraries/libKMeansAggregate.so", errMsg);
        catalogClient.registerType("libraries/libKMeansDataCountAggregate.so", errMsg);
        catalogClient.registerType("libraries/libScanDoubleVectorSet.so", errMsg);
        catalogClient.registerType("libraries/libScanDoubleArraySet.so", errMsg);
        catalogClient.registerType("libraries/libWriteKMeansSet.so", errMsg);
        catalogClient.registerType("libraries/libKMeansDataCountAggregate.so", errMsg);
        catalogClient.registerType("libraries/libKMeansSampleSelection.so", errMsg);
        catalogClient.registerType("libraries/libKMeansNormVectorMap.so", errMsg);
        catalogClient.registerType("libraries/libWriteDoubleVectorSet.so", errMsg);
        // now, create a new database
        if (!temp.createDatabase("kmeans_db", errMsg)) {
            COUT << "Not able to create database: " + errMsg;
            exit(-1);
        } else {
            COUT << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet<double[]>("kmeans_db", "kmeans_input_set", errMsg)) {
            COUT << "Not able to create set: " + errMsg;
            exit(-1);
        } else {
            COUT << "Created set.\n";
        }
    }

    if (whetherToAddData == true) {
        // Step 1. Create Database and Set
        // now, register a type for user data
        // TODO: once sharedLibrary is supported, add this line back!!!
        catalogClient.registerType("libraries/libKMeansAggregateOutputType.so", errMsg);
        catalogClient.registerType("libraries/libKMeansCentroid.so", errMsg);
        catalogClient.registerType("libraries/libWriteSumResultSet.so", errMsg);
        // register this query class
        catalogClient.registerType("libraries/libKMeansAggregate.so", errMsg);
        catalogClient.registerType("libraries/libKMeansDataCountAggregate.so", errMsg);
        catalogClient.registerType("libraries/libScanDoubleVectorSet.so", errMsg);
        catalogClient.registerType("libraries/libScanDoubleArraySet.so", errMsg);
        catalogClient.registerType("libraries/libWriteKMeansSet.so", errMsg);
        catalogClient.registerType("libraries/libKMeansDataCountAggregate.so", errMsg);
        catalogClient.registerType("libraries/libKMeansSampleSelection.so", errMsg);
        catalogClient.registerType("libraries/libKMeansNormVectorMap.so", errMsg);
        catalogClient.registerType("libraries/libWriteDoubleVectorSet.so", errMsg);
        // now, create a new database
        if (!temp.createDatabase("kmeans_db", errMsg)) {
            COUT << "Not able to create database: " + errMsg;
            exit(-1);
        } else {
            COUT << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet<double[NUM_KMEANS_DIMENSIONS]>(
                "kmeans_db", "kmeans_input_set", errMsg)) {
            COUT << "Not able to create set: " + errMsg;
            exit(-1);
        } else {
            COUT << "Created set.\n";
        }


        // Step 2. Add data
        DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);


        if (numOfMb >= 0) {
            if (addDataFromFile) {
                int blockSize = 256;
                // std :: ifstream inFile("/mnt/data_generator_kmeans/gaussian_pdb/kmeans_data");
                std::ifstream inFile(fileName.c_str());
                std::string line;
                bool rollback = false;
                bool end = false;

                while (!end) {
                    pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                    pdb::Handle<pdb::Vector<pdb::Handle<double[NUM_KMEANS_DIMENSIONS]>>> storeMe =
                        pdb::makeObject<pdb::Vector<pdb::Handle<double[NUM_KMEANS_DIMENSIONS]>>>(
                            544418);
                    try {

                        while (1) {
                            if (!rollback) {
                                //      std::istringstream iss(line);
                                if (!std::getline(inFile, line)) {
                                    end = true;
                                    break;
                                } else {
                                    pdb::Handle<double[NUM_KMEANS_DIMENSIONS]> myData =
                                        pdb::makeObject<double[NUM_KMEANS_DIMENSIONS]>();
                                    std::stringstream lineStream(line);
                                    double value;
                                    int index = 0;
                                    while (lineStream >> value) {
                                        (*myData)[index] = value;
                                        index++;
                                    }
                                    storeMe->push_back(myData);
                                }
                            } else {
                                rollback = false;
                                pdb::Handle<double[NUM_KMEANS_DIMENSIONS]> myData =
                                    pdb::makeObject<double[NUM_KMEANS_DIMENSIONS]>();
                                std::stringstream lineStream(line);
                                double value;
                                int index = 0;
                                while (lineStream >> value) {
                                    (*myData)[index] = value;
                                    index++;
                                }
                                storeMe->push_back(myData);
                            }
                        }

                        end = true;

                        if (!dispatcherClient.sendData<double[NUM_KMEANS_DIMENSIONS]>(
                                std::pair<std::string, std::string>("kmeans_input_set",
                                                                    "kmeans_db"),
                                storeMe,
                                errMsg)) {
                            COUT << "Failed to send data to dispatcher server" << std::endl;
                            return -1;
                        }
                        std::cout << "Dispatched " << storeMe->size() << " data in the last patch!"
                                  << std::endl;
                        temp.flushData(errMsg);
                    } catch (pdb::NotEnoughSpace& n) {
                        if (!dispatcherClient.sendData<double[NUM_KMEANS_DIMENSIONS]>(
                                std::pair<std::string, std::string>("kmeans_input_set",
                                                                    "kmeans_db"),
                                storeMe,
                                errMsg)) {
                            COUT << "Failed to send data to dispatcher server" << std::endl;
                            return -1;
                        }
                        std::cout << "Dispatched " << storeMe->size()
                                  << " data when allocated block is full!" << std::endl;
                        rollback = true;
                    }
                    PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std::endl;

                }  // while not end
                inFile.close();
            }
        }
    }
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        COUT << "Can't cleanup so files" << std::endl;
    }
}

#endif
