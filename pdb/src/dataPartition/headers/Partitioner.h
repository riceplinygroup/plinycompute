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

#ifndef PARTITIONER_H
#define PARTITIONER_H

#include "PartitionComp.h"
#include "InterfaceFunctions.h"

namespace pdb {

/* this class implements a partitioner that partitions data stored in a created set using the specified partition computation */

template<class OutputClass, class InputClass>
class Partitioner  {
public:

    /* constructor
     * @param inputDatabaseAndSet: the input pair of database name and set name, the set is expected to be created and often non-empty before being called by this function;
     * @param outputDatabaseAndSet: the output pair of database name and set name, the set is expected to be created and often empty before being called by this function;
     * @param partitionComp: the computation for partition input data
     */
    Partitioner (std::pair<std::string, std::string> inputDatabaseAndSet, 
                 std::pair<std::string, std::string> outputDatabaseAndSet,
                 Handle<PartitionComp<OutputClass, InputClass>> partitionComp) {

        this->inputDatabaseAndSet = inputDatabaseAndSet;
        this->outputDatabaseAndSet = outputDatabaseAndSet;
        this->partitionComp = deepCopyToCurrentAllocationBlock(partitionComp);

    }


    /* to partition the data stored in the inputDatabaseAndSet */
    /* @param errMsg: error message
    /* @return: whether this execution succeeds or not */
    bool partition( std::string & errMsg, std::shared_ptr<pdb::QueryClient> queryClient) {

        /* TODO */
        /* Step 1. to check whether the input set exists, if not we return false
        /* Step 2. to check whether the output set exists, if not, we return false
        /* Step 3. to check whether partitionComp is null, if yes, we return false
        /* Step 4. to check whether queryClient is null, if yes, we return false
        /* Step 5. to make allocation block */
        /* Step 6. to create a scanner computation */
        /* Step 7. to create a writer computation */
        /* Step 8. to compose a query graph */
        /* Step 9. to get TCAP */
        /* Step 10. to update the input-output mapping with TCAP */
        /* Step 11. to execute the query graph */

        return false;

    }


private:

    //the input set identifier
    std::pair<std::string, std::string> inputDatabaseAndSet;

    //the output set identifier
    std::pair<std::string, std::string> outputDatabaseAndSet;

    //the partition computation for this partitioner
    Handle<PartitionComp<OutputClass, InputClass>> partitionComp = nullptr;


};


}

#endif
