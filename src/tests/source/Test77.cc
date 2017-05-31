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
#ifndef TEST_77_H
#define TEST_77_H

//by Jia, May 2017

#include "Handle.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SillySelection.h"
#include "SelectionComp.h"
#include "FinalSelection.h"
#include "AggregateComp.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "StringIntPair.h"
#include "ScanIntSet.h"
#include "ScanStringSet.h"
#include "ScanStringIntPairSet.h"
#include "CartesianJoin.h"
#include "WriteStringIntPairSet.h"
#include "PDBString.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


using namespace pdb;

/* distributed join test case */
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
               std :: cout << "ERROR: cluster mode must be Y" << std :: endl;
               exit (1);
           }
       } else {
           std :: cout << "Will run on local node. If you want to run on cluster, you can add any character as the second parameter to run on the cluster configured by $PDB_HOME/conf/serverlist." << std :: endl;
       }

       int numOfObjects = 1024; //by default we add 1024MB data
       if (argc > 3) {
           numOfObjects = atoi(argv[3]); //this is a Cartesian join and results will be exploaded!
       }

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
            if (!temp.createDatabase ("test77_db", errMsg)) {
                cout << "Not able to create database: " + errMsg;
                exit (-1);
            } else {
                cout << "Created database.\n";
            }

            // now, create the int set in that database
            if (!temp.createSet<int> ("test77_db", "test77_set1", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
            } else {
                cout << "Created input set 1.\n";
            }

            // now, create the String set in that database
            if (!temp.createSet<String> ("test77_db", "test77_set2", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
            } else {
                cout << "Created input set 2.\n";
            }


            DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);

            //Step 2. Add data to set1
            size_t totalSize = 256 * numOfObjects;
            if (totalSize > 1024 * 1024) {
                std :: cout << "Too many objects which may cause Cartesian join run too long time" << std :: endl;
                exit (-1);
            } 
            makeObjectAllocatorBlock(totalSize, true);
            Handle<Vector<Handle<int>>> storeMe =
                        makeObject<Vector<Handle<int>>> ();
            int i = 0;
            for (; i < numOfObjects ; i++) {

                   Handle <int> myData = makeObject <int> (i);
                   storeMe->push_back(myData);
            }
                         
            std :: cout << "got to " << i << " when producing data for input set 1.\n";
            if (!dispatcherClient.sendData<int>(std::pair<std::string, std::string>("test77_set1", "test77_db"), storeMe, errMsg)) {
                   std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                   return -1;
            }

            std :: cout << "input set 1: total=" << i << std :: endl;

            //to write back all buffered records        
            temp.flushData( errMsg );


            //Step 3. Add data to set2
            totalSize = 512 * numOfObjects;
            if (totalSize > 1024 * 1024) {
                std :: cout << "Too many objects which may cause Cartesian join run too long time" <<
 std :: endl;
                exit (-1);
            }
            makeObjectAllocatorBlock(totalSize, true);
            Handle<Vector<Handle<String>>> storeMeStr = makeObject<Vector<Handle<String>>> ();
            i = 0;
            for (; i < numOfObjects ; i++) {
                    std :: ostringstream oss;
                    oss << "My string is " << i;
                    oss.str();
                    Handle <String> myData = makeObject <String> (oss.str());
                    storeMeStr->push_back(myData);
            }

            std :: cout << "got to " << i << " when producing data for input set 2.\n";
            if (!dispatcherClient.sendData<String>(std::pair<std::string, std::string>("test77_set2", "test77_db"), storeMeStr, errMsg)) {
                            std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                            return -1;
            }

            std :: cout << "input set 2: total=" << i << std :: endl;

            //to write back all buffered records        
            temp.flushData( errMsg );

        }

        // now, create a new set in that database to store output data
        PDB_COUT << "to create a new set for storing output data" << std :: endl;
        if (!temp.createSet<StringIntPair> ("test77_db", "output_set1", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
        } else {
                cout << "Created set.\n";
        }

        QueryClient myClient (8108, "localhost", clientLogger, true);
	
	// this is the object allocation block where all of this stuff will reside
        const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};
        // register this query class
        catalogClient.registerType ("libraries/libCartesianJoin.so", errMsg);
        catalogClient.registerType ("libraries/libScanIntSet.so", errMsg);
        catalogClient.registerType ("libraries/libScanStringSet.so", errMsg);
	catalogClient.registerType ("libraries/libWriteStringIntPairSet.so", errMsg);
	// create all of the computation objects
	Handle <Computation> myScanSet1 = makeObject <ScanIntSet> ("test77_db", "test77_set1");
        myScanSet1->setBatchSize(100);
        Handle <Computation> myScanSet2 = makeObject <ScanStringSet> ("test77_db", "test77_set2");
        myScanSet2->setBatchSize(16);
	Handle <Computation> myJoin = makeObject <CartesianJoin> ();
        myJoin->setInput(0, myScanSet1);
        myJoin->setInput(1, myScanSet2);
        Handle <Computation> myWriter = makeObject<WriteStringIntPairSet>("test77_db", "output_set1");
        myWriter->setInput(myJoin);
        auto begin = std :: chrono :: high_resolution_clock :: now();

        if (!myClient.executeComputations(errMsg, myWriter)) {
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
            SetIterator <StringIntPair> result = myClient.getSetIterator <StringIntPair> ("test77_db", "output_set1");

            std :: cout << "Query results: ";
            int count = 0;
            for (auto a : result)
            {
                     count ++;
                     std :: cout << count << ":" << *(a->myString) << ", " << a->myInt << ";";
            }
            std :: cout << "join output count:" << count << "\n";
        }

        if (clusterMode == false) {
            // and delete the sets
            myClient.deleteSet ("test77_db", "output_set1");
        } else {
            if (!temp.removeSet ("test77_db", "output_set1", errMsg)) {
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
