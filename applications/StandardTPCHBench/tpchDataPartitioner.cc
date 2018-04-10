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

    //TODO

}


void createPartitionSets (PDBClient & pdbClient) {

    pdbClient.removeSet("tpch", "customer_p");
    std::cout << "to create set for TPCHCustomer" << std::endl;
    pdbClient.createSet<TPCHCustomer>("tpch", "customer_p", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "lineitem_p");
    std::cout << "to create set for TPCHLineItem" << std::endl;
    pdbClient.createSet<TPCHLineItem>("tpch", "lineitem_p", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "nation_p");
    std::cout << "to create set for TPCHNation" << std::endl;
    pdbClient.createSet<TPCHNation>("tpch", "nation_p", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "order_p");
    std::cout << "to create set for TPCHOrder" << std::endl;
    pdbClient.createSet<TPCHOrder>("tpch", "order_p", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "part_p");
    std::cout << "to create set for TPCHPart" << std::endl;
    pdbClient.createSet<TPCHPart>("tpch", "part_p", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "partsupp_p");
    std::cout << "to create set for TPCHTPCHPartSupp" << std::endl;
    pdbClient.createSet<TPCHTPCHPartSupp>("tpch", "partsupp_p", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "region_p");
    std::cout << "to create set for TPCHRegion" << std::endl;
    pdbClient.createSet<TPCHRegion>("tpch", "region_p", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "supplier_p");
    std::cout << "to create set for TPCHSupplier" << std::endl;
    pdbClient.createSet<TPCHSupplier>("tpch", "supplier_p", (size_t)64*(size_t)1024*(size_t)1024);

}

void removePartitionedSets (PDBClient & pdbClient) {


    pdbClient.removeSet("tpch", "customer_p");
    pdbClient.removeSet("tpch", "lineitem_p");
    pdbClient.removeSet("tpch", "nation_p");
    pdbClient.removeSet("tpch", "order_p");
    pdbClient.removeSet("tpch", "part_p");
    pdbClient.removeSet("tpch", "partsupp_p");
    pdbClient.removeSet("tpch", "region_p");
    pdbClient.removeSet("tpch", "supplier_p");

}

void partitionData (PDBClient & pdbClient) {

    //TODO


}


int main(int argc, char* argv[]) {

    std::string tpchDirectory = "";
    if (argc > 1) {
        tpchDirectory = std::string(argv[1]);
    }

    bool whetherToRegisterLibraries = true;
    if (argc > 2) {
        if (strcmp(argv[2], "N") == 0) {
            whetherToRegisterLibraries = false;
        }
    }

    bool whetherToCreateSets = true;
    if (argc > 3) {
        if (strcmp(argv[3], "N") == 0) {
           whetherToCreateSets = false;
        }
    }

    bool whetherToPartitionData = true;
    if (argc > 4) {
        if (strcmp(argv[4], "N") == 0) {
           whetherToPartitionData = false;
        }
    }

    bool whetherToRemoveData = false;
    if (argc > 5) {
        if (strcmp(argv[5], "Y") == 0) {
           whetherToRemoveData = true;
        }
    }

    if ((argc > 6) || (argc == 1)) {
       std::cout << "Usage: #tpchDirectory #whetherToRegisterLibraries (Y/N)" 
                 << " #whetherToCreateSets (Y/N) #whetherToPartitionData (Y/N)"
                 << " #whetherToRemoveData (Y/N)" << std::endl;
    }

    // Connection info
    string masterHostname = "localhost";
    int masterPort = 8108;

    // register the shared employee class
    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

    pdb::PDBClient pdbClient(
            masterPort, masterHostname);


    if (whetherToRegisterLibraries == true) {
        registerPartitionLibraries (pdbClient);
    }

    if (whetherToCreateSets == true) {
        createPartitionSets (pdbClient);
    }

    if (whetherToPartitionData == true) {
        partitionData(pdbClient);
    }

    if (whetherToRemoveData == true) {
        removePartitionedSets(pdbClient);
    }    


    // Clean up the SO files.
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
}

#endif
