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

#ifndef TEST_56_H
#define TEST_56_H

//by Jia, Jan 4th, 2017

#ifndef MAX_THREADS
   #define MAX_THREADS 8
#endif

#ifndef NUM_DIMENSIONS
#define NUM_DIMENSIONS 100
#endif

#ifndef NUM_CLUSTERS
#define NUM_CLUSTERS 10
#endif



#include "Query.h"
#include "Lambda.h"
#include "Selection.h"
#include "QueryClient.h"
#include "QueryOutput.h"
#include "DistributedStorageManagerClient.h"
#include "ChrisSelection.h"
#include "StringSelection.h"
#include "DispatcherClient.h"
#include "Set.h"
#include "DataTypes.h"
#include "BuiltinKMeansQuery.h"
#include "BuiltinPartialResult.h"
#include "Centroid.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

/* This test uses data and selection of builtInType to demonstrate a distributed query with distributed storage */


using namespace pdb;
int main (int argc, char * argv[]) {


       std :: cout << "Usage: #numIterations #printResult[Y/N] #dataSize[MB] #masterIp #addData[Y/N]" << std :: endl;        

       int numIterations = 5;
       if (argc > 1) {
           numIterations = atoi(argv[1]);
       }
       std :: cout << "To run KMeans with " << numIterations << " iterations" << std :: endl;

       bool printResult = true;

       if (argc > 2) {
           if (strcmp(argv[2],"N") == 0) {
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

       pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");

       pdb :: DistributedStorageManagerClient temp (8108, masterIp, clientLogger);

       pdb :: CatalogClient catalogClient (8108, masterIp, clientLogger);


       string errMsg;

       if (whetherToAddData == true) {

            
            //Step 1. Create Database and Set

            // now, create a new database
            if (!temp.createDatabase ("kmeans_db", errMsg)) {
                cout << "Not able to create database: " + errMsg;
                exit (-1);
            } else {
                cout << "Created database.\n";
            }

            // now, create a new set in that database
            if (!temp.createSet<double [NUM_DIMENSIONS]> ("kmeans_db", "kmeans_set", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
            } else {
                cout << "Created set.\n";
            }


            //Step 2. Add data
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
                    pdb::Handle<pdb::Vector<pdb::Handle<double [NUM_DIMENSIONS]>>> storeMe =
                        pdb::makeObject<pdb::Vector<pdb::Handle<double [NUM_DIMENSIONS]>>> ();
                    try {
                        for (int i = 0; true ; i++) {
                            pdb :: Handle <double [NUM_DIMENSIONS]> myData =
                                pdb::makeObject <double [NUM_DIMENSIONS]> ();
                            for (int j = 0; j<NUM_DIMENSIONS; j++) {
                                (*myData)[j] = rand()/double(RAND_MAX);
                            }
                            storeMe->push_back (myData);
                            total++;
                        }
                    } catch (pdb :: NotEnoughSpace &n) {
                        if (!dispatcherClient.sendData<double [NUM_DIMENSIONS]>(std::pair<std::string, std::string>("kmeans_set", "kmeans_db"), storeMe, errMsg)) {
                            std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                            return -1;
                        }
                   }
                   std :: cout << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
                }
           
                std :: cout << "total=" << total << std :: endl;

                //to write back all buffered records        
                temp.flushData( errMsg );
          }
        }
        // now, create a new set in that database to store output data
        std :: cout << "to create a new set for storing output data" << std :: endl;



        if (!temp.createSet<BuiltinPartialResult> ("kmeans_db", "output_set1", errMsg)) {
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
	Handle <Set <double [NUM_DIMENSIONS]>> myInputSet = myClient.getSet <double [NUM_DIMENSIONS]> ("kmeans_db", "kmeans_set");
	Handle <BuiltinKMeansQuery> myQuery = makeObject <BuiltinKMeansQuery> ();
        myQuery->initialize();
        std :: cout << "To set input" << std :: endl;
	myQuery->setInput (myInputSet);
        std :: cout << "To make output object" << std :: endl;
	Handle <QueryOutput <BuiltinPartialResult>> outputOne = makeObject <QueryOutput <BuiltinPartialResult>> ("kmeans_db", "output_set1", myQuery);
        std :: cout << "made query graph" << std :: endl;
        
        auto begin = std :: chrono :: high_resolution_clock :: now();

        std :: cout << "to execute query graph" << std :: endl;
        if (!myClient.execute(errMsg, outputOne)) {
            std :: cout << "Query failed. Message was: " << errMsg << "\n";
            return 0;
        }
        std :: cout << std :: endl;

        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time Duration for 1st iteration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;
	std::cout << std::endl;
        int numIterated = 1;

        while (numIterated < numIterations) {
            auto begin = std :: chrono :: high_resolution_clock :: now();

            //to compute the new centroids
            SetIterator <BuiltinPartialResult> result = myClient.getSetIterator <BuiltinPartialResult> ("kmeans_db", "output_set1");
            Centroid newCentroids[NUM_CLUSTERS];
            int i;
            for (i = 0; i < NUM_CLUSTERS; i ++) {
                newCentroids[i].initialize();
            }
            for (Handle<BuiltinPartialResult> a : result) {
                Centroid * currentCentroids = a->getCentroids();
                for (i = 0; i < NUM_CLUSTERS; i ++) {
                    newCentroids[i].updateMulti(currentCentroids[i].getSum(), currentCentroids[i].getCount());
                }
            }
            for (i = 0; i < NUM_CLUSTERS; i ++) {
                newCentroids[i].aggregate();
            }

            auto aggregationEnd = std :: chrono :: high_resolution_clock :: now();

            //to delete the output set and create a new output set
            if (!temp.removeSet ("kmeans_db", "output_set1", errMsg)) {
                cout << "Not able to remove set: " + errMsg;
                exit (-1);
            } else {
                cout << "Removed set.\n";
            }
            auto removeSetEnd = std :: chrono :: high_resolution_clock :: now();
            if (!temp.createSet<BuiltinPartialResult> ("kmeans_db", "output_set1", errMsg)) {
                cout << "Not able to create set: " + errMsg;
                exit (-1);
            } else {
                cout << "Created set.\n";
            }
            auto createSetEnd = std :: chrono :: high_resolution_clock :: now();
            //to create the new KMeansQuery object
            // make the query graph
            Handle <Set <double [NUM_DIMENSIONS]>> myInputSet = myClient.getSet <double [NUM_DIMENSIONS]> ("kmeans_db", "kmeans_set");
            Handle <BuiltinKMeansQuery> myQuery = makeObject <BuiltinKMeansQuery> ();
            myQuery->initialize(newCentroids);
            myQuery->setInput (myInputSet);
            Handle <QueryOutput <BuiltinPartialResult>> outputOne = makeObject <QueryOutput <BuiltinPartialResult>> ("kmeans_db", "output_set1", myQuery);
            auto createQueryEnd = std :: chrono :: high_resolution_clock :: now();
            //to execute the new KMeansQuery object
            if (!myClient.execute(errMsg, outputOne)) {
                std :: cout << "Query failed. Message was: " << errMsg << "\n";
                return 0;
            }

            auto end = std::chrono::high_resolution_clock::now();
            numIterated ++;

            std::cout << "Time Duration for the " << numIterated << "-th iteration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;
            std::cout << "Aggregation Duration for the "<< numIterated << "-th iteration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(aggregationEnd-begin).count() << " ns." << std::endl;
            std::cout << "Remove Set Duration for the "<< numIterated << "-th iteration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(removeSetEnd-aggregationEnd).count() << " ns." << std::endl;
            std::cout << "Create Set Duration for the "<< numIterated << "-th iteration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(createSetEnd-removeSetEnd).count() << " ns." << std::endl;
            std::cout << "Create Query Duration for the "<< numIterated << "-th iteration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(createQueryEnd-createSetEnd).count() << " ns." << std::endl;
            std::cout << "Execute Query Duration for the "<< numIterated << "-th iteration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(end-createQueryEnd).count() << " ns." << std::endl;
            std::cout << std::endl;

        }

        end = std::chrono::high_resolution_clock::now();
	// print the resuts
        if (printResult == true) {
            std :: cout << "to print result..." << std :: endl;
	    SetIterator <BuiltinPartialResult> result = myClient.getSetIterator <BuiltinPartialResult> ("kmeans_db", "output_set1");
	    std :: cout << "Query results: ";
            int count = 0;
	    for (auto a : result) 
            {
                     count ++;
		     a->printCentroids();
            }
	    std :: cout << "selection output count:" << count << "\n";
	}

        if (!temp.removeSet ("kmeans_db", "output_set1", errMsg)) {
                cout << "Not able to remove set: " + errMsg;
                exit (-1);
        } else {
                cout << "Removed set.\n";
        }

        system ("scripts/cleanupSoFiles.sh");
        std::cout << "Total Time Duration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;
}

#endif
