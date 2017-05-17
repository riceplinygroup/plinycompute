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
#ifndef TEST_78_H
#define TEST_78_H

//by Jia, May 2017

#include "Handle.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SelectionComp.h"
#include "AggregateComp.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "StringIntPair.h"
#include "ScanIntSet.h"
#include "ScanStringIntPairSet.h"
#include "IntSillyJoin.h"
#include "IntAggregation.h"
#include "StringSelectionOfStringIntPair.h"
#include "WriteSumResultSet.h"
#include "SumResult.h"
#include "PDBString.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

/* distributed join test case */
using namespace pdb;


int main (int argc, char * argv[]) {


       bool printResult = true;
       bool clusterMode = false;
       std :: cout << "Usage: #printResult[Y/N] #clusterMode[Y/N] #dataSize[MB] #masterIp #addData[Y/N]" << std :: endl;
       if (argc > 1) {
           if (strcmp(argv[1],"N") == 0) {
               printResult = false;
               std :: cout << "You successfully disabled printing result." << std::endl;
           } else {
               printResult = true;
               std :: cout << "Will print result." << std :: endl;
           }

       } else {
           std :: cout << "Will print result. If you don't want to print result, you can add N as the first parameter to disable result printing." << std :: endl;
       }

       if (argc > 2) {
           if (strcmp(argv[2],"Y") == 0) {
               clusterMode = true;
               std :: cout << "You successfully set the test to run on cluster." << std :: endl;
           } else {
               clusterMode = false;
           }
       } else {
           std :: cout << "Will run on local node. If you want to run on cluster, you can add any character as the second parameter to run on the cluster configured by $PDB_HOME/conf/serverlist." << std :: endl;
       }

       int numOfMb = 128; //by default we add 1024MB data
       if (argc > 3) {
           numOfMb = atoi(argv[3])/16;
       }
       std :: cout << "To add data with size: " << numOfMb << "MB" << std :: endl;

       std :: string masterIp = "localhost";
       if (argc > 4) {
           masterIp = argv[4];
       }
       std :: cout << "Master IP Address is " << masterIp << std :: endl;

       bool whetherToAddData = true;
       if (argc > 5) {
           if (strcmp(argv[5],"N") == 0) {
              whetherToAddData = false;
           }
       }

       PDBLoggerPtr clientLogger = make_shared<PDBLogger>("clientLog");

       DistributedStorageManagerClient temp (8108, masterIp, clientLogger);

       CatalogClient catalogClient (8108, masterIp, clientLogger);

       string errMsg;

       if (whetherToAddData == true) {


            // now, create a new database
            if (!temp.createDatabase ("test78_db", errMsg)) {
                cout << "Not able to create database: " + errMsg;
                exit (-1);
            } else {
                cout << "Created database.\n";
            }

            // now, create the int set in that database
            if (!temp.createSet<int> ("test78_db", "test78_set1", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
            } else {
                cout << "Created input set 1.\n";
            }

            // now, create the StringIntPair set in that database
            if (!temp.createSet<StringIntPair> ("test78_db", "test78_set2", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
            } else {
                cout << "Created input set 2.\n";
            }


            DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);

            //Step 2. Add data to set1
            int total = 0;
            int i = 0;
            if (numOfMb > 0) {
                int numIterations = numOfMb/64;
                int remainder = numOfMb - 64*numIterations;
                if (remainder > 0) { numIterations = numIterations + 1; }
                for (int num = 0; num < numIterations; num++) {
                    int blockSize = 64;
                    if ((num == numIterations - 1) && (remainder > 0)){
                        blockSize = remainder;
                    }
                    makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                    Handle<Vector<Handle<int>>> storeMe =
                        makeObject<Vector<Handle<int>>> ();
                    try {
                        for (i=0; true ; i++) {

                            Handle <int> myData = makeObject <int> (i);
                            storeMe->push_back(myData);
                            total++;
                        }
                         
                    } catch (pdb :: NotEnoughSpace &n) {
                        std :: cout << "got to " << i << " when producing data for input set 1.\n";
                        if (!dispatcherClient.sendData<int>(std::pair<std::string, std::string>("test78_set1", "test78_db"), storeMe, errMsg)) {
                            std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                            return -1;
                        }
                   }
                   PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
                }

                std :: cout << "input set 1: total=" << total << std :: endl;

                //to write back all buffered records        
                temp.flushData( errMsg );
            }

            //Step 3. Add data to set2
            total = 0;
            i = 0;
            if (numOfMb > 0) {
                int numIterations = numOfMb/64;
                int remainder = numOfMb - 64*numIterations;
                if (remainder > 0) { numIterations = numIterations + 1; }
                for (int num = 0; num < numIterations; num++) {
                    int blockSize = 64;
                    if ((num == numIterations - 1) && (remainder > 0)){
                        blockSize = remainder;
                    }
                    makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                    Handle<Vector<Handle<StringIntPair>>> storeMe =
                        makeObject<Vector<Handle<StringIntPair>>> ();
                    try {
                        for (i=0; true ; i++) {
                            std :: ostringstream oss;
                            oss << "My string is " << i;
                            oss.str(); 
                            Handle <StringIntPair> myData = makeObject <StringIntPair> (oss.str(), i);
                            storeMe->push_back(myData);
                            total++;
                        }

                    } catch (pdb :: NotEnoughSpace &n) {
                        std :: cout << "got to " << i << " when producing data for input set 2.\n";
                        if (!dispatcherClient.sendData<StringIntPair>(std::pair<std::string, std::string>("test78_set2", "test78_db"), storeMe, errMsg)) {
                            std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                            return -1;
                        }
                   }
                   PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
                }

                std :: cout << "input set 2: total=" << total << std :: endl;

                //to write back all buffered records        
                temp.flushData( errMsg );
            }

        }
        // now, create a new set in that database to store output data
        PDB_COUT << "to create a new set for storing output data" << std :: endl;
        if (!temp.createSet<SumResult> ("test78_db", "output_set1", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
        } else {
                cout << "Created set.\n";
        }

        QueryClient myClient (8108, "localhost", clientLogger, true);
	
	// this is the object allocation block where all of this stuff will reside
        const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};
        // register this query class
        catalogClient.registerType ("libraries/libIntSillyJoin.so", errMsg);
        catalogClient.registerType ("libraries/libScanIntSet.so", errMsg);
        catalogClient.registerType ("libraries/libScanStringIntPairSet.so", errMsg);
        catalogClient.registerType ("libraries/libStringSelectionOfStringIntPair.so", errMsg);
        catalogClient.registerType ("libraries/libIntAggregation.so", errMsg);
	catalogClient.registerType ("libraries/libWriteSumResultSet.so", errMsg);
	// create all of the computation objects
	Handle <Computation> myScanSet1 = makeObject <ScanIntSet> ("test78_db", "test78_set1");
        Handle <Computation> myScanSet2 = makeObject <ScanStringIntPairSet> ("test78_db", "test78_set2");
	Handle <Computation> mySelection = makeObject <StringSelectionOfStringIntPair> ();
        mySelection->setInput(myScanSet2);
        Handle <Computation> myJoin = makeObject <IntSillyJoin> ();
        myJoin->setInput(0, myScanSet1);
        myJoin->setInput(1, myScanSet2);
        myJoin->setInput(2, mySelection);
        Handle <Computation> myAggregation = makeObject <IntAggregation> ("test78_db", "output_set1");
        myAggregation->setInput(myJoin);
        //Handle <Computation> myWriter = makeObject<WriteSumResultSet>("test78_db", "output_set1");
        //myWriter->setInput(myAggregation);
        auto begin = std :: chrono :: high_resolution_clock :: now();

        if (!myClient.executeComputations(errMsg, myAggregation)) {
            std :: cout << "Query failed. Message was: " << errMsg << "\n";
            return 1;
        }
        std :: cout << std :: endl;

        auto end = std::chrono::high_resolution_clock::now();
        //std::cout << "Time Duration: " <<
          //      std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;

        std::cout << std::endl;
        // print the resuts
        if (printResult == true) {
            std :: cout << "to print result..." << std :: endl;
            SetIterator <SumResult> result = myClient.getSetIterator <SumResult> ("test78_db", "output_set1");

            std :: cout << "Query results: ";
            int count = 0;
            for (auto a : result)
            {
                     count ++;
                     std :: cout << count << ":" << (*a).total << ";";
            }
            std :: cout << "join output count:" << count << "\n";
        }

        if (clusterMode == false) {
            // and delete the sets
            myClient.deleteSet ("test78_db", "output_set1");
        } else {
            if (!temp.removeSet ("test78_db", "output_set1", errMsg)) {
                cout << "Not able to remove set: " + errMsg;
                exit (-1);
            } else {
                cout << "Removed set.\n";
            }
        }
        int code = system ("scripts/cleanupSoFiles.sh");
        if (code < 0) {
            std :: cout << "Can't cleanup so files" << std :: endl;
        }
        std::cout << "Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;
}


#endif