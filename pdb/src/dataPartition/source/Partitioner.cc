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

#ifndef PARTITIONER_CC
#define PARTiTIONER_CC

#include "Partitioner.h"

namespace pdb {

Partitioner :: Partitioner (std::pair<std::string, std::string> inputDatabaseAndSet,
                            std::pair<std::string, std::string> outputDatabaseAndSet,
                            Handle<PartitionComp<OutputClass, InputClass>> partitionComp) {

        this->inputDatabaseAndSet = inputDatabaseAndSet;
        this->outputDatabaseAndSet = outputDatabaseAndSet;
        this->partitionComp = deepCopyToCurrentAllocationBlock(partitionComp);

}


Partitioner :: partition ( std::string & errMsg, std::shared_ptr<pdb::QueryClient> queryClient) {

        /* TODO */
        /* Step 1. to check whether the input set exists, if not we return false
        /* Step 2. to check whether the output set exists, if not, we return false
        /* Step 3. to check whether partitionComp is null, if yes, we return false
        /* Step 4. to check whether queryClient is null, if yes, we return false
        /* Step 5. to make allocation block */
        /* Step 6. to create a scanner computation */
        /* Step 7. to create a writer computation */
        /* Step 8. to compose a query graph */
        /* Step 9. to execute the query graph */

        return false;

    }


}

#endif
