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

#ifndef TEST_33_CC
#define TEST_33_CC

#include "StorageClient.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "SharedEmployee.h"

// this won't be visible to the v-table map, since it is not in the built in types directory

int main(int argc, char* argv[]) {

    std::cout << "Firstly: Make sure to run bin/test23 or bin/test28 in a different window to "
                 "provide a catalog/storage server.\n";
    std::cout << "Secondly: Make sure to run bin/test24 to register the SharedEmployee type once "
                 "and only once.\n";
    std::cout << "Thirdly: Make sure to run bin/test31 to create the destination set.\n";
    std::string databaseName("Test31_Database123");
    std::string setName("Test31_Set123");
    std::cout << "to remove database with name: " << databaseName << std::endl;
    std::cout << "to remove set with name: " << setName << std::endl;

    // start a client
    bool usePangea = true;
    pdb::StorageClient temp(8108, "localhost", make_shared<pdb::PDBLogger>("clientLog"), usePangea);
    string errMsg;


    // now remove the destination set
    if (!temp.removeSet<SharedEmployee>(databaseName, setName, errMsg)) {
        std::cout << "Not able to remove set: " + errMsg << std::endl;
        std::cout << "Please change a set name, or remove the pdbRoot AND CatalogDir directories "
                     "at where you run test23/test28"
                  << std::endl;
    } else {
        std::cout << "Removed set.\n";
    }
}

#endif
