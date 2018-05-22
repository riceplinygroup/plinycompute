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
#ifndef TPCH_DATA_LOADER_CC
#define TPCH_DATA_LOADER_CC

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

#define BLOCKSIZE (256 * MB)


// A function to parse a Line
std::vector<std::string> parseLine(std::string line) {
    stringstream lineStream(line);
    std::vector<std::string> tokens;
    string token;
    while (getline(lineStream, token, '|')) {
        tokens.push_back(token);
    }
    return tokens;
}

//A function to create an object based on dataType and text
Handle<Object>  createObject(std::string line, std::string dataType) {
   
     
    std::vector<std::string> tokens = parseLine(line);
    Handle<Object> objectToAdd = nullptr;
    if (dataType == "TPCHCustomer") {
        objectToAdd = makeObject<tpch::TPCHCustomer>(atoi(tokens.at(0).c_str()),
                                                        tokens.at(1),
                                                        tokens.at(2),
                                                        atoi(tokens.at(3).c_str()),
                                                        tokens.at(4),
                                                        atof(tokens.at(5).c_str()),
                                                        tokens.at(6),
                                                        tokens.at(7));
    } else if (dataType == "TPCHLineItem") {
       objectToAdd = makeObject<TPCHLineItem>(atoi(tokens.at(0).c_str()),
                                                        atoi(tokens.at(1).c_str()),
                                                        atoi(tokens.at(2).c_str()),
                                                        atoi(tokens.at(3).c_str()),
                                                        atof(tokens.at(4).c_str()),
                                                        atof(tokens.at(5).c_str()),
                                                        atof(tokens.at(6).c_str()),
                                                        atof(tokens.at(7).c_str()),
                                                        tokens.at(8),
                                                        tokens.at(9),
                                                        tokens.at(10),
                                                        tokens.at(11),
                                                        tokens.at(12),
                                                        tokens.at(13),
                                                        tokens.at(14),
                                                        tokens.at(15));

    } else if (dataType == "TPCHNation") {
       objectToAdd = makeObject<TPCHNation>(atoi(tokens.at(0).c_str()),
                                                        tokens.at(1),
                                                        atoi(tokens.at(2).c_str()),
                                                        tokens.at(3));

    } else if (dataType == "TPCHOrder") {
       objectToAdd = makeObject<TPCHOrder>(atoi(tokens.at(0).c_str()),
                                                        atoi(tokens.at(1).c_str()),
                                                        tokens.at(2),
                                                        atof(tokens.at(3).c_str()),
                                                        tokens.at(4),
                                                        tokens.at(5),
                                                        tokens.at(6),
                                                        atoi(tokens.at(7).c_str()),
                                                        tokens.at(8));
    } else if (dataType == "TPCHPart") {
       objectToAdd = makeObject<TPCHPart>(atoi(tokens.at(0).c_str()),
                                                        tokens.at(1),
                                                        tokens.at(2),
                                                        tokens.at(3),
                                                        tokens.at(4),
                                                        atoi(tokens.at(5).c_str()),
                                                        tokens.at(6),
                                                        atof(tokens.at(7).c_str()),
                                                        tokens.at(8));
    } else if (dataType == "TPCHTPCHPartSupp") {
       objectToAdd = makeObject<TPCHTPCHPartSupp>(atoi(tokens.at(0).c_str()),
                                                        atoi(tokens.at(1).c_str()),
                                                        atoi(tokens.at(2).c_str()),
                                                        atof(tokens.at(3).c_str()),
                                                        tokens.at(4));

    } else if (dataType == "TPCHRegion" ) {
       objectToAdd = makeObject<TPCHRegion>(atoi(tokens.at(0).c_str()),
                                                        tokens.at(1),
                                                        tokens.at(2));
    } else if (dataType == "TPCHSupplier" ) {
       objectToAdd = makeObject<TPCHSupplier>(atoi(tokens.at(0).c_str()),
                                                        tokens.at(1),
                                                        tokens.at(2),
                                                        atoi(tokens.at(3).c_str()),
                                                        tokens.at(4),
                                                        atof(tokens.at(5).c_str()),
                                                        tokens.at(6));
    }
    return objectToAdd;
}

void registerLibraries (PDBClient & pdbClient) {



    pdbClient.registerType ("libraries/libTPCHCustomer.so");
    pdbClient.registerType ("libraries/libTPCHLineItem.so");
    pdbClient.registerType ("libraries/libTPCHNation.so");
    pdbClient.registerType ("libraries/libTPCHOrder.so");
    pdbClient.registerType ("libraries/libTPCHPart.so");
    pdbClient.registerType ("libraries/libTPCHPartSupp.so");
    pdbClient.registerType ("libraries/libTPCHRegion.so");
    pdbClient.registerType ("libraries/libTPCHSupplier.so");

    bool query01 = true;

    if (query01) {

       pdbClient.registerType ("libraries/libQ01Agg.so");
       pdbClient.registerType ("libraries/libQ01AggOut.so");
       pdbClient.registerType ("libraries/libQ01KeyClass.so");
       pdbClient.registerType ("libraries/libQ01ValueClass.so");

    }

    bool query02 = true;

    if (query02) {
   
        pdbClient.registerType ("libraries/libMinDouble.so");
        pdbClient.registerType ("libraries/libQ02MinAgg.so");
        pdbClient.registerType ("libraries/libQ02MinCostJoin.so");
        pdbClient.registerType ("libraries/libQ02MinCostJoinOutput.so");
        pdbClient.registerType ("libraries/libQ02MinCostPerTPCHPart.so");
        pdbClient.registerType ("libraries/libQ02MinCostSelection.so");
        pdbClient.registerType ("libraries/libQ02MinCostSelectionOutput.so");
        pdbClient.registerType ("libraries/libQ02TPCHNationJoin.so");
        pdbClient.registerType ("libraries/libQ02TPCHPartJoin.so");
        pdbClient.registerType ("libraries/libQ02TPCHPartJoinOutput.so");
        pdbClient.registerType ("libraries/libQ02TPCHPartSelection.so");
        pdbClient.registerType ("libraries/libQ02TPCHPartSuppJoin.so");
        pdbClient.registerType ("libraries/libQ02TPCHPartSuppJoinOutput.so");
        pdbClient.registerType ("libraries/libQ02TPCHRegionSelection.so");
        pdbClient.registerType ("libraries/libQ02TPCHSupplierJoin.so");
        pdbClient.registerType ("libraries/libQ02TPCHSupplierJoinOutput.so");
        pdbClient.registerType ("libraries/libQ02TPCHPartJoinOutputIdentitySelection.so");

    }

    bool query03 = true;

    if (query03) {
        pdbClient.registerType ("libraries/libQ03Agg.so");
        pdbClient.registerType ("libraries/libQ03AggOut.so");
        pdbClient.registerType ("libraries/libQ03Join.so");
        pdbClient.registerType ("libraries/libQ03JoinOut.so");
        pdbClient.registerType ("libraries/libQ03TPCHCustomerSelection.so");
        pdbClient.registerType ("libraries/libQ03TPCHOrderSelection.so");
        pdbClient.registerType ("libraries/libQ03TPCHLineItemSelection.so");
        pdbClient.registerType ("libraries/libQ03KeyClass.so");
    }

    bool query04 = true;

    if (query04) {

        pdbClient.registerType ("libraries/libQ04Agg.so");
        pdbClient.registerType ("libraries/libQ04AggOut.so");
        pdbClient.registerType ("libraries/libQ04Join.so");
        pdbClient.registerType ("libraries/libQ04TPCHOrderSelection.so");

    }

    bool query06 = true;

    if (query06) {

        pdbClient.registerType ("libraries/libQ06Agg.so");
        pdbClient.registerType ("libraries/libQ06TPCHLineItemSelection.so");

    }

    bool query12 = true;

    if (query12) {

        pdbClient.registerType ("libraries/libQ12TPCHLineItemSelection.so");
        pdbClient.registerType ("libraries/libQ12JoinOut.so");
        pdbClient.registerType ("libraries/libQ12Join.so");
        pdbClient.registerType ("libraries/libQ12ValueClass.so");
        pdbClient.registerType ("libraries/libQ12AggOut.so");
        pdbClient.registerType ("libraries/libQ12Agg.so");

    }


    bool query13 = true;
    
    if (query13) {

        pdbClient.registerType ("libraries/libQ13CountResult.so");
        pdbClient.registerType ("libraries/libQ13TPCHCustomerTPCHOrderJoin.so");
        pdbClient.registerType ("libraries/libQ13TPCHOrderSelection.so");
        pdbClient.registerType ("libraries/libQ13TPCHCustomerDistribution.so");
        pdbClient.registerType ("libraries/libQ13TPCHCustomerTPCHOrders.so");
        pdbClient.registerType ("libraries/libQ13TPCHOrdersPerTPCHCustomer.so");

    }

    bool query14 = true;

    if (query14) {

        pdbClient.registerType ("libraries/libQ14Agg.so");
        pdbClient.registerType ("libraries/libQ14AggOut.so");
        pdbClient.registerType ("libraries/libQ14Join.so");
        pdbClient.registerType ("libraries/libQ14JoinOut.so");
        pdbClient.registerType ("libraries/libQ14ValueClass.so");
        pdbClient.registerType ("libraries/libQ14TPCHLineItemSelection.so");

    }

    bool query17 = true;

    if (query17) {

        pdbClient.registerType ("libraries/libQ17JoinedTPCHPartTPCHLineItem.so");
        pdbClient.registerType ("libraries/libQ17TPCHPartTPCHLineItemAvgJoin.so");
        pdbClient.registerType ("libraries/libQ17TPCHPartSelection.so");
        pdbClient.registerType ("libraries/libQ17TPCHLineItemAvgQuantity.so");
        pdbClient.registerType ("libraries/libQ17TPCHPartTPCHLineItemJoin.so");
        pdbClient.registerType ("libraries/libQ17PriceSum.so");
        pdbClient.registerType ("libraries/libQ17TPCHPartTPCHLineItemIdentitySelection.so");
    }

    bool query22 = true;

    if (query22) {

         pdbClient.registerType("libraries/libQ22AggregatedCntryBal.so");
         pdbClient.registerType("libraries/libQ22TPCHCustomerAccbalAvg.so");
         pdbClient.registerType("libraries/libQ22TPCHOrderCountSelection.so");
         pdbClient.registerType("libraries/libQ22CntryBalAgg.so");
         pdbClient.registerType("libraries/libQ22JoinedCntryBal.so");
         pdbClient.registerType("libraries/libQ22CntryBalJoin.so");
         pdbClient.registerType("libraries/libQ22TPCHOrderCountPerTPCHCustomer.so");

    }


}


void createSets (PDBClient & pdbClient) {

    pdbClient.createDatabase("tpch");
    pdbClient.removeSet("tpch", "customer");
    std::cout << "to create set for TPCHCustomer" << std::endl;
    pdbClient.createSet<TPCHCustomer>("tpch", "customer", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "lineitem");
    std::cout << "to create set for TPCHLineItem" << std::endl;
    pdbClient.createSet<TPCHLineItem>("tpch", "lineitem", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "nation");
    std::cout << "to create set for TPCHNation" << std::endl;
    pdbClient.createSet<TPCHNation>("tpch", "nation", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "order");
    std::cout << "to create set for TPCHOrder" << std::endl;
    pdbClient.createSet<TPCHOrder>("tpch", "order", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "part");
    std::cout << "to create set for TPCHPart" << std::endl;
    pdbClient.createSet<TPCHPart>("tpch", "part", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "partsupp");
    std::cout << "to create set for TPCHTPCHPartSupp" << std::endl;
    pdbClient.createSet<TPCHTPCHPartSupp>("tpch", "partsupp", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "region");
    std::cout << "to create set for TPCHRegion" << std::endl;
    pdbClient.createSet<TPCHRegion>("tpch", "region", (size_t)64*(size_t)1024*(size_t)1024);
    pdbClient.removeSet("tpch", "supplier");
    std::cout << "to create set for TPCHSupplier" << std::endl;
    pdbClient.createSet<TPCHSupplier>("tpch", "supplier", (size_t)64*(size_t)1024*(size_t)1024);

}

void removeSets (PDBClient & pdbClient) {

    pdbClient.removeSet("tpch", "customer");
    pdbClient.removeSet("tpch", "lineitem");
    pdbClient.removeSet("tpch", "nation");
    pdbClient.removeSet("tpch", "order");
    pdbClient.removeSet("tpch", "part");
    pdbClient.removeSet("tpch", "partsupp");
    pdbClient.removeSet("tpch", "region");
    pdbClient.removeSet("tpch", "supplier");

}

void sendData (PDBClient & pdbClient, Handle<Vector<Handle<Object>>> objects, std::string dataType) {
    std::cout << "to send vector with " << objects->size() << std::endl;
    if (dataType == "TPCHCustomer") {
        pdbClient.sendData<Object> (
        std::pair<std::string, std::string>("customer", "tpch"), objects);
    } else if (dataType == "TPCHLineItem") {
        pdbClient.sendData<Object> (
        std::pair<std::string, std::string>("lineitem", "tpch"), objects);
    } else if (dataType == "TPCHNation") {
        pdbClient.sendData<Object> (
        std::pair<std::string, std::string>("nation", "tpch"), objects);
    } else if (dataType == "TPCHOrder") {
        pdbClient.sendData<Object> (
        std::pair<std::string, std::string>("order", "tpch"), objects);
    } else if (dataType == "TPCHPart") {
        pdbClient.sendData<Object> (
        std::pair<std::string, std::string>("part", "tpch"), objects);
    } else if (dataType == "TPCHTPCHPartSupp") {
        pdbClient.sendData<Object> (
        std::pair<std::string, std::string>("partsupp", "tpch"), objects);
    } else if (dataType == "TPCHRegion") {
        pdbClient.sendData<Object> (
        std::pair<std::string, std::string>("region", "tpch"), objects);
    } else if (dataType == "TPCHSupplier") {
        pdbClient.sendData<Object> (
        std::pair<std::string, std::string>("supplier", "tpch"), objects);
    }

}


void loadData(PDBClient & pdbClient, std::string fileName, std::string dataType) {


    std::cout << "to load data from " << fileName << " for type " << dataType << std::endl;
    std::string line;
    std::string delimiter = "|";
    std::ifstream infile;
    bool rollback = false;
    bool end = false;
    int numObjects = 0;
    infile.open(fileName.c_str());
    if (infile.good() == false) {
        cout << "file: " << fileName.c_str() << ", can't be open! "  << endl;
        exit(-1);
    }

    //different types
    Handle<Vector<Handle<TPCHCustomer>>> customers = nullptr; 
    Handle<Vector<Handle<TPCHLineItem>>> lineitems = nullptr;
    Handle<Vector<Handle<TPCHNation>>> nations = nullptr;
    Handle<Vector<Handle<TPCHOrder>>> orders = nullptr;
    Handle<Vector<Handle<TPCHPart>>> parts = nullptr;
    Handle<Vector<Handle<TPCHTPCHPartSupp>>> partsupps = nullptr;
    Handle<Vector<Handle<TPCHRegion>>> regions = nullptr;
    Handle<Vector<Handle<TPCHSupplier>>> suppliers = nullptr;
    Handle<Vector<Handle<Object>>> objects = nullptr;
    while (!end) {
        makeObjectAllocatorBlock((size_t)BLOCKSIZE, true);
        if (dataType == "TPCHCustomer") {
            customers = makeObject<Vector<Handle<TPCHCustomer>>>();
            objects = unsafeCast<Vector<Handle<Object>>, Vector<Handle<TPCHCustomer>>> (customers);
        } else if (dataType == "TPCHLineItem") {
            lineitems = makeObject<Vector<Handle<TPCHLineItem>>>();
            objects = unsafeCast<Vector<Handle<Object>>, Vector<Handle<TPCHLineItem>>> (lineitems);
        } else if (dataType == "TPCHNation") {
            nations = makeObject<Vector<Handle<TPCHNation>>>();
            objects = unsafeCast<Vector<Handle<Object>>, Vector<Handle<TPCHNation>>> (nations);
        } else if (dataType == "TPCHOrder") {
            orders = makeObject<Vector<Handle<TPCHOrder>>>();
            objects = unsafeCast<Vector<Handle<Object>>, Vector<Handle<TPCHOrder>>> (orders);
        } else if (dataType == "TPCHPart") {
            parts = makeObject<Vector<Handle<TPCHPart>>>();
            objects = unsafeCast<Vector<Handle<Object>>, Vector<Handle<TPCHPart>>> (parts);
        } else if (dataType == "TPCHTPCHPartSupp") {
            partsupps = makeObject<Vector<Handle<TPCHTPCHPartSupp>>>();
            objects = unsafeCast<Vector<Handle<Object>>, Vector<Handle<TPCHTPCHPartSupp>>> (partsupps);
        } else if (dataType == "TPCHRegion") {
            regions = makeObject<Vector<Handle<TPCHRegion>>>();
            objects = unsafeCast<Vector<Handle<Object>>, Vector<Handle<TPCHRegion>>> (regions);
        } else if (dataType == "TPCHSupplier") {
            suppliers = makeObject<Vector<Handle<TPCHSupplier>>>();
            objects = unsafeCast<Vector<Handle<Object>>, Vector<Handle<TPCHSupplier>>> (suppliers);
        }
        
        
        try {

            while (1) {
                if (!rollback) {
                    if (!std::getline(infile, line)) {
                         end = true;
                         break;
                    } else {
                         Handle<Object> objectToAdd = createObject(line, dataType);
                         objects->push_back(objectToAdd);
                         numObjects++;
                    }
                } else {
                    rollback = false;
                    Handle<Object> objectToAdd = createObject(line, dataType);
                    objects->push_back(objectToAdd);
                    numObjects++;
                }
            }
            sendData(pdbClient, objects, dataType); 

        } catch (NotEnoughSpace & n) {

            sendData(pdbClient, objects, dataType);
            rollback = true;

        }

    }
    std::cout << "sent " << numObjects << " " << dataType << " objects" << std::endl; 
    infile.close();
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

    bool whetherToAddData = true;
    if (argc > 4) {
        if (strcmp(argv[4], "N") == 0) {
           whetherToAddData = false;
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
                 << " #whetherToCreateSets (Y/N) #whetherToAddData (Y/N)"
                 << " #whetherToRemoveData (Y/N)" << std::endl;
    }

    // Connection info
    string managerHostname = "localhost";
    int managerPort = 8108;

    // register the shared employee class
    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

    pdb::PDBClient pdbClient(
            managerPort, managerHostname);

    CatalogClient catalogClient(
            managerPort,
            managerHostname,
            clientLogger);

    if (whetherToRegisterLibraries == true) {
        registerLibraries (pdbClient);
    }

    if (whetherToCreateSets == true) {
        createSets (pdbClient);
    }

    if (whetherToAddData == true) {
        loadData(pdbClient, tpchDirectory + "/customer.tbl", "TPCHCustomer");
        loadData(pdbClient, tpchDirectory + "/lineitem.tbl", "TPCHLineItem");
        loadData(pdbClient, tpchDirectory + "/nation.tbl", "TPCHNation");
        loadData(pdbClient, tpchDirectory + "/orders.tbl", "TPCHOrder");
        loadData(pdbClient, tpchDirectory + "/part.tbl", "TPCHPart");
        loadData(pdbClient, tpchDirectory + "/partsupp.tbl", "TPCHTPCHPartSupp");
        loadData(pdbClient, tpchDirectory + "/region.tbl", "TPCHRegion");
        loadData(pdbClient, tpchDirectory + "/supplier.tbl", "TPCHSupplier");

        std::cout << "to flush data to disk" << std::endl;
        pdbClient.flushData();

    }

    if (whetherToRemoveData == true) {
        removeSets(pdbClient);
    }    


    // Clean up the SO files.
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
}

#endif
