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

#ifndef TEST_49_H
#define TEST_49_H

#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "Selection.h"
#include "QueryClient.h"
#include "QueryOutput.h"
#include "DistributedStorageManagerClient.h"
#include "CheckEmployees.h"
#include "DispatcherClient.h"
#include "Supervisor.h"
#include "Employee.h"
#include "Set.h"
#include "QueryOutput.h"
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


       bool printResult = true;
       bool clusterMode = false;
       std :: cout << "Usage: #printResult[Y/N] #clusterMode[Y/N] #dataSize[MB] masterIp" << std :: endl;        
       if (argc > 1) {

           printResult = false;
           std :: cout << "You successfully disabled printing result." << std::endl;

       } else {
           std :: cout << "Will print result. If you don't want to print result, you can add any character as the first parameter to disable result printing." << std :: endl;
       }

       if (argc > 2) {
       
           clusterMode = true;
           std :: cout << "You successfully set the test to run on cluster." << std :: endl;

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

       pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");

       //Step 1. Create Database and Set
       pdb :: DistributedStorageManagerClient temp (8108, masterIp, clientLogger);

       string errMsg;

        // now, create a new database
        if (!temp.createDatabase ("chris_db", errMsg)) {
                cout << "Not able to create database: " + errMsg;
        } else {
                cout << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet ("chris_db", "chris_set", "pdb::Supervisor", errMsg)) {
                cout << "Not able to create set: " + errMsg;
        } else {
                cout << "Created set.\n";
        }

        //Step 2. Add data
        DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);
        //dispatcherClient.registerSet(std::pair<std::string, std::string>("chris_db", "chris_set"), pdb::PartitionPolicy::Policy::RANDOM, errMsg);

        int numIterations = numOfMb;
       
        for (int num = 0; num < numIterations; num++) {
            pdb :: makeObjectAllocatorBlock(1024 * 1024, true);
            pdb::Handle<pdb::Vector<pdb::Handle<pdb::Supervisor>>> storeMe =
                    pdb::makeObject<pdb::Vector<pdb::Handle<pdb::Supervisor>>> ();
            try {
                for (int i = 0; true ; i++) {
                    pdb :: Handle <Supervisor> myData =
                            pdb::makeObject <pdb::Supervisor> ("Joe Johnson" + to_string (i), i + 45);
                    for (int j = 0; j < 10; j++) {
                         pdb :: Handle <Employee> myEmp =
                                 pdb :: makeObject<Employee> ("Joe Johnson" + to_string(j), j + 45);
                         myData->addEmp(myEmp);
                    }
                    storeMe->push_back (myData);
                    //cout << myData.getTypeCode() << std::endl;
                }
            } catch (pdb :: NotEnoughSpace &n) {
                if (!dispatcherClient.sendData<Supervisor>(std::pair<std::string, std::string>("chris_set", "chris_db"), storeMe, errMsg)) {
                    std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                    return -1;
                }
            }
            std :: cout << "1MB data sent to dispatcher server~~" << std :: endl;
        }

        //Step 3. To execute a Query
	// for allocations
	const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};

	// register this query class
	// connect to the query client
	QueryClient myClient (8108, "localhost", clientLogger, true);

	// make the query graph
	Handle <Set <pdb::Supervisor>> myInputSet = myClient.getSet <pdb::Supervisor> ("chris_db", "chris_set");
	Handle <pdb::CheckEmployee> myQuery = makeObject <pdb::CheckEmployee> (std :: string ("Steve Stevens"));
	myQuery->setInput (myInputSet);
	Handle <QueryOutput <pdb::Employee>> outputOne = makeObject <QueryOutput <pdb::Employee>> ("chris_db", "output_set1", myQuery);

        
        auto begin = std :: chrono :: high_resolution_clock :: now();

        if (!myClient.execute(errMsg, outputOne)) {
            std :: cout << "Query failed. Message was: " << errMsg << "\n";
            return 0;
        }
        std :: cout << std :: endl;

        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time Duration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;

	std::cout << std::endl;
	// print the resuts

        if ((printResult == true) && (clusterMode == false)) {
	    SetIterator <String> result = myClient.getSetIterator <String> ("chris_db", "output_set1");
	    std :: cout << "First set of query results: ";
	    for (auto a : result) 
            {
		     std :: cout << (*a) << "; ";
            }
	    std :: cout << "\n";
	}

        if (clusterMode == false) {
	    // and delete the sets
	    myClient.deleteSet ("chris_db", "output_set1");
        }
        system ("scripts/cleanupSoFiles.sh");
        
}

#endif
