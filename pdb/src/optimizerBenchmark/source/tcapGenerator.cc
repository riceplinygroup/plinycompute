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
#ifndef TCAP_STRING_GENERATOR_CC
#define TCAP_STRING_GENERATOR_CC

#include "CatalogClient.h"

#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <map>
#include <chrono>
#include <sstream>
#include <vector>
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

#include "PDBDebug.h"
#include "PDBString.h"
#include "PDBClient.h"
#include "Query.h"
#include "Lambda.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "DataTypes.h"
#include "InterfaceFunctions.h"


// ss107: Headers written by me:
#include "MovieStar.h"
#include "StarsIn.h"
#include "ScanMovieStarSet.h"
#include "ScanStarsInSet.h"
#include "SimpleMovieJoin.h"
#include "SimpleMovieSelection.h"
#include "SimpleMovieWrite.h"


#include "Part.h"
#include "Supplier.h"
#include "LineItem.h"
#include "Order.h"
#include "Customer.h"
#include "CustomerMultiSelection.h"
#include "OrderWriteSet.h"
#include "CustomerSupplierPartWriteSet.h"
#include "ScanCustomerSet.h"

#include "Handle.h"
#include "Lambda.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SelectionComp.h"
#include "WriteBuiltinEmployeeSet.h"
#include "SupervisorMultiSelection.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"

#include "QueryOutput.h"
#include "DataTypes.h"

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

#include "CustomerSupplierPart.h"

// TODO: why should I include WriteStringSet when I want to use DispatcherClient?
#include "WriteStringSet.h"

using namespace std;

// Run a Cluster on Localhost
// ./bin/pdb-cluster localhost 8108 Y
// ./bin/pdb-server 1 512 localhost:8108 localhost:8109
// ./bin/pdb-server 1 512 localhost:8108 localhost:8110

#define KB 1024
#define MB (1024 * KB)
#define GB (1024 * MB)


int main() {

    int noOfCopies = 1;

    // Connection info
    string masterHostname = "localhost";
    int masterPort = 8108;

    // register the shared employee class
    pdb::PDBClient pdbClient(
            masterPort, masterHostname, false, true);

    string errMsg;
    if (!pdbClient.registerType("libraries/libMovieStar.so", errMsg))
        cout << "Not able to register type.\n";

    if (!pdbClient.registerType("libraries/libStarsIn.so", errMsg))
        cout << "Not able to register type.\n";

    if (!pdbClient.registerType("libraries/libScanMovieStarSet.so", errMsg))
        cout << "Not able to register type.\n";

    if (!pdbClient.registerType("libraries/libScanStarsInSet.so", errMsg))
        cout << "Not able to register type.\n";

    if (!pdbClient.registerType("libraries/libSimpleMovieJoin.so", errMsg))
        cout << "Not able to register type.\n";

    if (!pdbClient.registerType("libraries/libSimpleMovieSelection.so", errMsg))
        cout << "Not able to register type.\n";

    if (!pdbClient.registerType("libraries/libSimpleMovieWrite.so", errMsg))
        cout << "Not able to register type.\n";


    // Create a new database:
    if (!pdbClient.createDatabase("TCAP_db", errMsg)) {
        cout << "Not able to create database: " + errMsg;
        exit(-1);
    } else {
        cout << "Created database.\n";
    }

    // Create the sets for storing MovieStar data:
    if (!pdbClient.createSet<MovieStar>(
            "TCAP_db", "tcap_bench_set1", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    // Create the sets for storing StarsIn data:
    if (!pdbClient.createSet<StarsIn>("TCAP_db", "tcap_bench_set2", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    pdb::makeObjectAllocatorBlock((size_t)200 * MB, true);


    // Trying to create the Query Graph:
    // For allocations:
    const UseTemporaryAllocationBlock tempBlock{(size_t)128 * MB};

    // Make the Query Graph:
    Handle<Computation> myScanSet1 = makeObject<ScanMovieStarSet>("TCAP_db", "tpch_bench_set1");
    Handle<Computation> myScanSet2 = makeObject<ScanStarsInSet>("TCAP_db", "tpch_bench_set2");


    // Plan 1 follows:
    /*

    Handle <Computation> myJoin = makeObject <SimpleMovieJoin> ();
    myJoin->setInput(0, myScanSet1);
    myJoin->setInput(1, myScanSet2);

    Handle <Computation> myFilter = makeObject <SimpleMovieSelection> ();
    myFilter->setInput(myJoin);

    //Handle <Computation> myWriter = makeObject <SimpleMovieWrite> ();
    //myWriter->setInput(myFilter);

    vector <Handle<Computation>> queryGraph;
    queryGraph.push_back(myFilter);
    QueryGraphAnalyzer queryAnalyzer(queryGraph);

    string tcapString = queryAnalyzer.parseTCAPString();
    cout << "TCAP OUTPUT:" << endl;
    cout << tcapString << endl;

    */


    // Plan 2 follows:
    Handle<Computation> myFilter = makeObject<SimpleMovieSelection>();
    myFilter->setInput(myScanSet1);


    Handle<Computation> myJoin = makeObject<SimpleMovieJoin>();
    myJoin->setInput(0, myFilter);
    myJoin->setInput(1, myScanSet2);

    // Handle <Computation> myWriter = makeObject <SimpleMovieWrite> ();
    // myWriter->setInput(myFilter);

    vector<Handle<Computation>> queryGraph;
    queryGraph.push_back(myJoin);
    QueryGraphAnalyzer queryAnalyzer(queryGraph);

    string tcapString = queryAnalyzer.parseTCAPString();
    cout << "TCAP OUTPUT:" << endl;
    cout << tcapString << endl;


    cout << "Sourav: Done till here!\n";

    /*
        //
        // Generate the data
        // TPCH Data file scale - Data should be in folder named "tables_scale_"+"scaleFactor"
        string scaleFactor = "0.1";
    //	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>  storeMeCustomerList =
    dataGenerator(scaleFactor);
        pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>  storeMeCustomerList =
    generateSmallDataset(4);

        pdb::Record<Vector<Handle<Customer>>>*myBytes = getRecord <Vector <Handle <Customer>>>
    (storeMeCustomerList);
        size_t sizeOfCustomers = myBytes->numBytes();
        cout << "Size of Customer Vector is: " << sizeOfCustomers << endl;

        // store copies of the same dataset.
        for (int i = 1; i <= noOfCopies; ++i) {
            cout << "Storing Vector of Customers - Copy Number : " << i << endl;

            if (!dispatcherClient.sendData<Customer>(std::pair<std::string,
    std::string>("tpch_bench_set1", "TCAP_db"), storeMeCustomerList, errMsg)) {
                std::cout << "Failed to send data to dispatcher server" << std::endl;
                return -1;
            }
        }
        // flush to disk
        distributedStorageManagerClient.flushData(errMsg);

        if (!pdbClient.registerType("libraries/libCustomerMultiSelection.so", errMsg))
            cout << "Not able to register type libCustomerMultiSelection.\n";

        if (!pdbClient.registerType("libraries/libOrderMultiSelection.so", errMsg))
            cout << "Not able to register type  libOrderMultiSelection.\n";

        if (!pdbClient.registerType("libraries/libCustomerSupplierPartWriteSet.so", errMsg))
            cout << "Not able to register type libOrderWriteSet.\n";

        if (!pdbClient.registerType("libraries/libScanCustomerSet.so", errMsg))
            cout << "Not able to register type libScanCustomerSet. \n";

        // now, create the sets for storing Customer Data
        if (!distributedStorageManagerClient.createSet<CustomerSupplierPart>("TCAP_db",
    "t_output_se1", errMsg)) {
            cout << "Not able to create set: " + errMsg;
            exit(-1);
        } else {
            cout << "Created set.\n";
        }

        // for allocations
        const UseTemporaryAllocationBlock tempBlock {(size_t) 128 * MB };

        // make the query graph
        Handle<Computation> myScanSet = makeObject<ScanCustomerSet>("TCAP_db", "tpch_bench_set1");

        Handle<Computation> myFlatten = makeObject<CustomerMultiSelection>();
        myFlatten->setInput(myScanSet);

        Handle<Computation> myWriteSet = makeObject<CustomerSupplierPartWriteSet>("TCAP_db",
    "t_output_se1");
        myWriteSet->setInput(myFlatten);



        auto begin = std::chrono::high_resolution_clock::now();
        if (!queryClient.executeComputations(errMsg, myWriteSet)) {
            std::cout << "Query failed. Message was: " << errMsg << "\n";
            return 1;
        }

        std::cout << std::endl;
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time Duration: " << std::chrono::duration_cast<std::chrono::nanoseconds> (end
    - begin).count() << " ns." << std::endl;

        std::cout << "to print result..." << std::endl;
        SetIterator<CustomerSupplierPart> result =
    queryClient.getSetIterator<CustomerSupplierPart>("TCAP_db", "t_output_se1");

        std::cout << "Query results: ";
        int count = 0;
        for (auto a : result) {
            count++;
    //		if (count % 10 == 0) {
                std::cout << count << endl;
                cout << a->getPartKey() << endl;
                cout << a->getCustomerName()->c_str() << endl;
                cout << a->getSupplierName()->c_str() << endl;

    //		}
        }
        std::cout << "multi-selection output count:" << count << "\n";

    */
    // Clean up the SO files.
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {

        std::cout << "Can't cleanup so files" << std::endl;
    }
}

#endif
