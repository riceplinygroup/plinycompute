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

#ifndef TPCH_DATA_PARTITIONER_CC
#define TPCH_DATA_PARTITIONER_CC

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
#include "Query.h"
#include "Lambda.h"
#include "PDBClient.h"
#include "DataTypes.h"
#include "InterfaceFunctions.h"
#include "TPCHSchema.h"
#include "Handle.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SelectionComp.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "QueryOutput.h"
#include "DataTypes.h"
#include "LineItemPartitionComp.h"
#include "LineItemPartitionTransformationComp.h"
#include "TPCHSchema.h"
#include "Query01.h"

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

using namespace pdb;
using namespace tpch;


#define KB 1024
#define MB (1024 * KB)
#define GB (1024 * MB)

#define BLOCKSIZE (64 * MB)



void registerPartitionLibraries (PDBClient & pdbClient) {

   pdbClient.registerType ("libraries/libLineItemPartitionComp.so");

}


void createPartitionSets (PDBClient & pdbClient) {

    pdbClient.removeSet("tpch", "lineitem_pt");
    std::cout << "to create set for TPCHLineItem" << std::endl;
    pdbClient.createSet<TPCHLineItem>("tpch", "lineitem_pt", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.createSet<TPCHLineItem>("tpch", "lineitem_pt_1", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.createSet<TPCHLineItem>("tpch", "conflict_lineitem_pt_1", (size_t)64*(size_t)1024*(size_t)1024);
}

void removePartitionedSets (PDBClient & pdbClient) {


    pdbClient.removeSet("tpch", "conflict_lineitem_pt");
    pdbClient.removeSet("tpch", "conflict_lineitem_pt_1");
    pdbClient.removeSet("tpch", "lineitem_pt_1");
    //pdbClient.removeSet("tpch", "lineitem_pt");

}

void partitionData (PDBClient & pdbClient) {

    Handle<LineItemPartitionComp> partitionComp
       = makeObject<LineItemPartitionComp>();
    pdbClient.partitionSet<int, TPCHLineItem>(std::pair<std::string, std::string>("tpch", "lineitem"),
                        std::pair<std::string, std::string>("tpch", "lineitem_pt"),
                        partitionComp, true, false);

}

void recoverData (PDBClient & pdbClient) {


    std::vector<int> * nodesToRecover = new std::vector<int>();
    nodesToRecover->push_back(0);
    Handle<LineItemPartitionComp> partitionComp
       = makeObject<LineItemPartitionComp>();
    pdbClient.partitionSet<int, TPCHLineItem>(std::pair<std::string, std::string>("tpch", "lineitem"),
                        std::pair<std::string, std::string>("tpch", "lineitem_pt_1"),
                        partitionComp, false, true, nodesToRecover);
    pdbClient.partitionSet<int, TPCHLineItem>(std::pair<std::string, std::string>("tpch", "conflict_lineitem_pt"),
                        std::pair<std::string, std::string>("tpch", "conflict_lineitem_pt_1"),
                        partitionComp, false, true, nodesToRecover);


}



int main(int argc, char* argv[]) {

    bool whetherToRegisterLibraries = true;
    if (argc > 1) {
        if (strcmp(argv[1], "N") == 0) {
            whetherToRegisterLibraries = false;
        }
    }

    bool whetherToCreateSets = true;
    if (argc > 2) {
        if (strcmp(argv[2], "N") == 0) {
           whetherToCreateSets = false;
        }
    }

    bool whetherToPartitionData = true;
    if (argc > 3) {
        if (strcmp(argv[3], "N") == 0) {
           whetherToPartitionData = false;
        }
    }

    bool whetherToRemoveData = false;
    if (argc > 4) {
        if (strcmp(argv[4], "Y") == 0) {
           whetherToRemoveData = true;
        }
    }

    if ((argc > 6) || (argc == 1)) {
       std::cout << "Usage: #whetherToRegisterLibraries (Y/N)" 
                 << " #whetherToCreateSets (Y/N) #whetherToPartitionData (Y/N)"
                 << " #whetherToRemoveData (Y/N)" << std::endl;
    }

    // Connection info
    string managerHostname = "localhost";
    int managerPort = 8108;

    // register the shared employee class
    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

    pdb::PDBClient pdbClient(
            managerPort, managerHostname);


    if (whetherToRegisterLibraries == true) {
        registerPartitionLibraries (pdbClient);
    }

    if (whetherToCreateSets == true) {
        createPartitionSets (pdbClient);
    }

    if (whetherToPartitionData == true) {
        partitionData(pdbClient);
        pdbClient.flushData();
    }

    if (whetherToRemoveData == true) {
        removePartitionedSets(pdbClient);
    }    

/*
    // now, create the sets for storing query output data
    pdbClient.removeSet("tpch", "q01_output_set");
    if (!pdbClient.createSet<Q01AggOut>(
            "tpch", "q01_output_set")) {
        cout << "Not able to create set.";
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 256};



    std::cout << "###########################################" << std::endl;
    std::cout << "Run Query01 on non-partitioned lineitem set" << std::endl;
    std::cout << "###########################################" << std::endl;


    // make the query graph
    Handle<Computation> myTPCHLineItemScanner = makeObject<ScanUserSet<TPCHLineItem>>("tpch", "lineitem");
    Handle<Computation> myQ01Agg = makeObject<Q01Agg>();
    Handle<Computation> myQ01Writer = makeObject<WriteUserSet<Q01AggOut>> ("tpch", "q01_output_set");

    myQ01Agg->setInput(myTPCHLineItemScanner);
    myQ01Writer->setInput(myQ01Agg);


    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myQ01Writer)) {
        std::cout << "Query failed. " << "\n";
        return 1;
    }

    std::cout << std::endl;
    auto end = std::chrono::high_resolution_clock::now();

    float timeDifference =
        (float(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())) /
        (float)1000000000;

    // Printing results to double check
    std::cout << "to print result..." << std::endl;


    SetIterator<Q01AggOut> result =
            pdbClient.getSetIterator<Q01AggOut>("tpch", "q01_output_set");



    std::cout << "################################" << std::endl;
    std::cout << "Non-partitioned Query results: "  << std::endl;     
    std::cout << "################################" << std::endl;
    int count = 0;
    for (auto a : result) {
        Q01ValueClass r = a->getValue();
        std::cout << "count=" << r.count << ", sum_qty=" << r.sum_qty << ", sum_base_price=" << r.sum_base_price << ", sum_disc_price=" << r.sum_disc_price
            << ", sum_charge=" << r.sum_charge << ", sum_disc=" << r.sum_disc << ", avg_qty=" << r.getAvgQty()
            << ", avg_price=" << r.getAvgPrice() << ", avg_disc=" << r.getAvgDiscount() << std::endl;
        count++;
    }
    std::cout << "Non-partitioned Query output count:" << count << "\n";
    std::cout << "#TimeDuration for non-partitioned query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q01_output_set")) {
        cout << "Not able to remove the set";
        exit(-1);
    } else {
        cout << "Set removed. \n";
    }




    pdbClient.removeSet("tpch", "q01_output_set_partitioned");
    if (!pdbClient.createSet<Q01AggOut>(
            "tpch", "q01_output_set_partitioned")) {
        cout << "Not able to create set.";
        exit(-1);
    } else {
        cout << "Created set.\n";
    }


    std::cout << "###########################################" << std::endl;
    std::cout << "Run Query01 on partitioned lineitem set" << std::endl;
    std::cout << "###########################################" << std::endl;

    Handle<Computation> myTPCHLineItem_ptScanner = makeObject<ScanUserSet<TPCHLineItem>>("tpch", "lineitem_pt");
    Handle<Computation> myQ01Agg_partitioned = makeObject<Q01Agg>();
    Handle<Computation> myQ01Writer_partitioned = makeObject<WriteUserSet<Q01AggOut>> ("tpch", "q01_output_set_partitioned");

    myQ01Agg_partitioned->setInput(myTPCHLineItem_ptScanner);
    myQ01Writer_partitioned->setInput(myQ01Agg_partitioned);


    // Query Execution and Time Calculation

    begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myQ01Writer_partitioned)) {
        std::cout << "Query failed. " << "\n";
        return 1;
    }

    std::cout << std::endl;
    end = std::chrono::high_resolution_clock::now();

    timeDifference =
        (float(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())) /
        (float)1000000000;

    // Printing results to double check
    std::cout << "to print result..." << std::endl;


    SetIterator<Q01AggOut> result_partitioned = pdbClient.getSetIterator<Q01AggOut>("tpch", "q01_output_set_partitioned");

    std::cout << "###########################" << std::endl;
    std::cout << "Partitioned Query results: " << std::endl;
    std::cout << "###########################" << std::endl;
    count = 0;
    for (auto a : result_partitioned) {
        Q01ValueClass r = a->getValue();
        std::cout << "count=" << r.count << ", sum_qty=" << r.sum_qty << ", sum_base_price=" << r.sum_base_price << ", sum_disc_price=" << r.sum_disc_price
            << ", sum_charge=" << r.sum_charge << ", sum_disc=" << r.sum_disc << ", avg_qty=" << r.getAvgQty()
            << ", avg_price=" << r.getAvgPrice() << ", avg_disc=" << r.getAvgDiscount() << std::endl;
        count++;
    }
    std::cout << "Output count:" << count << "\n";
    std::cout << "#TimeDuration for query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q01_output_set_partitioned")) {
        cout << "Not able to remove the set";
        exit(-1);
    } else {
        cout << "Set removed. \n";
    }
*/
    if (!pdbClient.removeSet("tpch", "lineitem_pt")) {
        cout << "Not able to remove the set";
        exit(-1);
    } else {
        cout << "Set removed. \n";
    }  
  

    auto begin = std::chrono::high_resolution_clock::now();

    recoverData(pdbClient);    

    std::cout << std::endl;
    auto end = std::chrono::high_resolution_clock::now();

    float timeDifference =
        (float(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())) /
        (float)1000000000;

    std::cout << "#TimeDuration for recovery: " << timeDifference << " Second " << std::endl;



    // Clean up the SO files.
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
}

#endif
