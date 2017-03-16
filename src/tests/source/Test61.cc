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
#ifndef TEST_61_H
#define TEST_61_H

//by Jia, Mar 2017

#include "Handle.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "Supervisor.h"
#include "Employee.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SillySelection.h"
#include "SelectionComp.h"
#include "AggregateComp.h"
#include "ScanSupervisorSet.h"
#include "SillyAggregation.h"
#include "SillySelection.h"
#include "ZA_DepartmentTotal.h"
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


// to run the aggregate, the system first passes each through the hash operation...
// then the system 
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

       int numOfMb = 1024; //by default we add 1024MB data
       if (argc > 3) {
           numOfMb = atoi(argv[3]);
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
            if (!temp.createDatabase ("chris_db", errMsg)) {
                cout << "Not able to create database: " + errMsg;
                exit (-1);
            } else {
                cout << "Created database.\n";
            }

            // now, create a new set in that database
            if (!temp.createSet<Supervisor> ("chris_db", "chris_set", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
            } else {
                cout << "Created set.\n";
            }


            //Step 2. Add data
            DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);

           int total = 0;
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
                    Handle<Vector<Handle<Supervisor>>> storeMe =
                        makeObject<Vector<Handle<Supervisor>>> ();
                    char first = 'A', second = 'B', third = 'C', fourth = 'D';
                    char myString[5];
                    myString[4]=0;
                    try {
                        for (int i = 0; true ; i++) {

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
                            Handle <Supervisor> myData = makeObject <Supervisor> ("Steve Stevens", 20 + (i % 29), std :: string (myString), i * 34.4);
                            storeMe->push_back(myData);
                            for (int j = 0; j < 10; j++) {
                                 Handle <Employee> temp;
                                 if (i % 2 == 0) {
                                     temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29), std :: string (myString), j * 3.54);
                                 }
                                 else {
                                     temp = makeObject <Employee> ("Albert Albertson", 20 + ((i + j) % 29), std :: string (myString), j * 3.54);
                                 }
                                 (*storeMe)[i]->addEmp (temp);
                            }
                       }
                         
                    } catch (pdb :: NotEnoughSpace &n) {
                        if (!dispatcherClient.sendData<Supervisor>(std::pair<std::string, std::string>("chris_set", "chris_db"), storeMe, errMsg)) {
                            std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                            return -1;
                        }
                   }
                   PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
                }

                std :: cout << "total=" << total << std :: endl;

                //to write back all buffered records        
                temp.flushData( errMsg );
          }
        }
        // now, create a new set in that database to store output data
        PDB_COUT << "to create a new set for storing output data" << std :: endl;
        if (!temp.createSet<ZA_DepartmentTotal> ("chris_db", "output_set1", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
        } else {
                cout << "Created set.\n";
        }

        QueryClient myClient (8108, "localhost", clientLogger, true);
	
	// this is the object allocation block where all of this stuff will reside
       	makeObjectAllocatorBlock (1024 * 1024, true);

        // register this query class
        catalogClient.registerType ("libraries/libSillySelection.so", errMsg);
        catalogClient.registerType ("libraries/libScanSupervisorSet.so", errMsg);
        catalogClient.registerType ("libraries/libSillyAggregation.so", errMsg);

	// here is the list of computations
	Handle<Vector <Handle <Computation>>> myComputations;
	
	// create all of the computation objects
	Handle <Computation> myScanSet = makeObject <ScanSupervisorSet> ("chris_db", "chris_set");
	Handle <Computation> myFilter = makeObject <SillySelection> ();
	Handle <Computation> myAgg = makeObject <SillyAggregation> ("chris_db", "output_set1");
	
	// put them in the list of computations
	myComputations->push_back (myScanSet);
	myComputations->push_back (myFilter);
	myComputations->push_back (myAgg);

        auto begin = std :: chrono :: high_resolution_clock :: now();

        if (!myClient.executeQuery(errMsg, myComputations, true)) {
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
            SetIterator <ZA_DepartmentTotal> result = myClient.getSetIterator <ZA_DepartmentTotal> ("chris_db", "output_set1");

            std :: cout << "Query results: ";
            int count = 0;
            for (auto a : result)
            {
                     count ++;
                     std :: cout << count << ":";
                     a->print();
            }
            std :: cout << "selection output count:" << count << "\n";
        }

        if (clusterMode == false) {
            // and delete the sets
            myClient.deleteSet ("chris_db", "output_set1");
        } else {
            if (!temp.removeSet ("chris_db", "output_set1", errMsg)) {
                cout << "Not able to remove set: " + errMsg;
                exit (-1);
            } else {
                cout << "Removed set.\n";
            }
        }
        system ("scripts/cleanupSoFiles.sh");
        std::cout << "Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;
}


#endif
