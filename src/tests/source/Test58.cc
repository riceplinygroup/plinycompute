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

#ifndef TEST_58_H
#define TEST_58_H

//by Jia, Jan 4th, 2017

#ifndef MAX_THREADS
   #define MAX_THREADS 8
#endif

#ifndef K
   #define K 100
#endif

#include "PDBDebug.h"
#include "SharedEmployee.h"
#include "InterfaceFunctions.h"
#include "BuiltinTopKInput.h"
#include "BuiltinTopKResult.h"
#include "BuiltinTopKQuery.h"
#include "Query.h"
#include "Lambda.h"
#include "Selection.h"
#include "QueryClient.h"
#include "QueryOutput.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "Set.h"
#include "DataTypes.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

/* This test uses data and selection of builtInType to demonstrate a distributed query with distributed storage */


using namespace pdb;
int main (int argc, char * argv[]) {


       std::cout << "Usage: #printResult[Y/N] #dataSize[MB] #masterIp #addData[Y/N]" << std :: endl;        

       bool printResult = true;

       if (argc > 1) {
           if (strcmp(argv[1],"N") == 0) {
               printResult = false;
               std :: cout << "You successfully disabled printing result." << std::endl;
           } else {
               printResult = true;
               std :: cout << "Will print result." << std :: endl;
           }

       } else {
           std :: cout << "Will print result. If you don't want to print result, you can add N as the second parameter to disable result printing." << std :: endl;
       }

       int numOfMb = 1024; //by default we add 1024MB data
       if (argc > 2) {
           numOfMb = atoi(argv[2]);
       }
       std :: cout << "To add data with size: " << numOfMb << "MB" << std :: endl;

       std :: string masterIp = "localhost";
       if (argc > 3) {
           masterIp = argv[3];
       }
       std :: cout << "Master IP Address is " << masterIp << std :: endl;

       bool whetherToAddData = true;
       if (argc > 4) {
           if (strcmp(argv[4],"N") == 0) {
              whetherToAddData = false;
           }
       }

       pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");

       pdb :: DistributedStorageManagerClient temp (8108, masterIp, clientLogger);

       pdb :: CatalogClient catalogClient (8108, masterIp, clientLogger);


       string errMsg;

       if (whetherToAddData == true) {

            
            //Step 1. Create Database and Set

            // now, create a new database
            if (!temp.createDatabase ("topK_db", errMsg)) {
                cout << "Not able to create database: " + errMsg;
                exit (-1);
            } else {
                cout << "Created database.\n";
            }

            // now, create a new set in that database
            if (!temp.createSet<BuiltinTopKInput> ("topK_db", "topK_set", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
            } else {
                cout << "Created set.\n";
            }


            //Step 2. Add data
            catalogClient.registerType ("libraries/libSharedEmployee.so", errMsg);
            DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);


            int total = 0;       
            srand ((unsigned int)(time(NULL)));
            if (numOfMb > 0) {
                int numIterations = numOfMb/64;
                int remainder = numOfMb - 64*numIterations;
                if (remainder > 0) { numIterations = numIterations + 1; }
                for (int num = 0; num < numIterations; num++) {
                    int blockSize = 64;
                    if ((num == numIterations - 1) && (remainder > 0)){
                        blockSize = remainder;
                    }
                    pdb :: makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                    pdb::Handle<pdb::Vector<pdb::Handle<BuiltinTopKInput>>> storeMe =
                        pdb::makeObject<pdb::Vector<pdb::Handle<BuiltinTopKInput>>> ();
                    try {
                        for (int i = 0; true ; i++) {
                            pdb :: Handle<SharedEmployee> employee = pdb :: makeObject<SharedEmployee>("steve", i%100); 
                            double score = rand()/double(RAND_MAX);
                            pdb :: Handle <BuiltinTopKInput> myData =
                                pdb::makeObject <BuiltinTopKInput> (score, employee);
                            storeMe->push_back (myData);
                            total++;
                        }
                    } catch (pdb :: NotEnoughSpace &n) {
                        if (!dispatcherClient.sendData<BuiltinTopKInput>(std::pair<std::string, std::string>("topK_set", "topK_db"), storeMe, errMsg)) {
                            std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                            return -1;
                        }
                   }
                   std :: cout << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
                }
                std :: cout << "######Data Creation Finished#######" << std :: endl;           
                std :: cout << "total=" << total << std :: endl;
                std :: cout << "###################################" << std :: endl;

                //to write back all buffered records        
                temp.flushData( errMsg );
          }
        }
        // now, create a new set in that database to store output data
        std :: cout << "to create a new set for storing output data" << std :: endl;



        if (!temp.createSet<BuiltinTopKResult> ("topK_db", "output_set", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
        } else {
                cout << "Created set.\n";
        }

        //Step 3. To execute a Query
	// for allocations
	const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};


	// connect to the query client
	QueryClient myClient (8108, "localhost", clientLogger, true);
	// make the query graph
	Handle <Set <BuiltinTopKInput>> myInputSet = myClient.getSet <BuiltinTopKInput> ("topK_db", "topK_set");
	Handle <BuiltinTopKQuery> myQuery = makeObject <BuiltinTopKQuery> ();
        myQuery->initialize();
        std :: cout << "To set input" << std :: endl;
	myQuery->setInput (myInputSet);
        std :: cout << "To make output object" << std :: endl;
	Handle <QueryOutput <BuiltinTopKResult>> outputOne = makeObject <QueryOutput <BuiltinTopKResult>> ("topK_db", "output_set", myQuery);
        std :: cout << "made query graph" << std :: endl;
        
        auto begin = std :: chrono :: high_resolution_clock :: now();

        std :: cout << "to execute query graph" << std :: endl;
        if (!myClient.execute(errMsg, outputOne)) {
            std :: cout << "Query failed. Message was: " << errMsg << "\n";
            return 0;
        }
        std :: cout << std :: endl;

        auto queryEnd = std::chrono::high_resolution_clock::now();
        std::cout << "Time Duration for Query Execution: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(queryEnd-begin).count() << " ns." << std::endl;
	std::cout << std::endl;

        //to aggregate all top-K
        SetIterator <BuiltinTopKResult> partialResults = myClient.getSetIterator <BuiltinTopKResult> ("topK_db", "output_set");
        Handle<BuiltinTopKResult> finalResult = makeObject<BuiltinTopKResult>();
        finalResult->initialize();
        for (Handle<BuiltinTopKResult> a : partialResults) {
            Handle<Vector<Handle<BuiltinTopKInput>>> elements = a->getTopK();
            std::cout << "to process a partialResult that contains "<< elements->size() << " elements" << std::endl;
            int i;
            for (i = 0; i < elements->size(); i ++) {
                std::cout << "score=" << (*elements)[i]->getScore() << std::endl;
                finalResult->updateTopK((*elements)[i]);

            }

        }

        auto aggregationEnd = std :: chrono :: high_resolution_clock :: now();
        std::cout << "Time Duration for Aggregation: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(aggregationEnd-queryEnd).count() << " ns." << std::endl;
        std::cout << std::endl;
        std::cout << "Time Duration for Total: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(aggregationEnd-begin).count() << " ns." << std::endl;
        std::cout << std::endl;

        //to print top-K
        if (printResult == true) {
            Handle<Vector<Handle<BuiltinTopKInput>>> elements = finalResult->getTopK();
            int i;
            for (i = 0; i < elements->size(); i ++) {

                std::cout << i <<":score=" << (*elements)[i]->getScore()<< std::endl;
                Handle<Object> object = (*elements)[i]->getObject();
                Handle<SharedEmployee> employee = unsafeCast<SharedEmployee, Object>(object);                
                employee->print();
                std::cout << std::endl;
                  
            }
        }

        //to delete the output set and create a new output set
        if (!temp.removeSet ("topK_db", "output_set", errMsg)) {
                cout << "Not able to remove set: " + errMsg;
                exit (-1);
        } else {
                cout << "Removed set.\n";
        }


        system ("scripts/cleanupSoFiles.sh");
}

#endif
