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
#include "PartitionTransformationComp.h"
#include "InterfaceFunctions.h"
#include "ScanUserSet.h"
#include "WriteUserSet.h"
#include "QueryClient.h"
#include <cstring>
#include <vector>

namespace pdb {

/* this class implements a partitioner that partitions data stored in a created set using the specified partition computation */

template<class KeyClass, class ValueClass>
class Partitioner  {


public:

    /* constructor
     * @param inputDatabaseAndSet: the input pair of database name and set name, the set is expected to be created and often non-empty before being called by this function;
     * @param outputDatabaseAndSet: the output pair of database name and set name, the set is expected to be created and often empty before being called by this function;
     */
    Partitioner (std::pair<std::string, std::string> inputDatabaseAndSet, 
                 std::pair<std::string, std::string> outputDatabaseAndSet);


    /* to partition the data stored in the inputDatabaseAndSet */
    /* @param errMsg: error message 
     * @param queryClient: the client used to send partition at the server
     * @return: whether this execution succeeds or not */
    bool partition ( std::string & errMsg, 
                     std::shared_ptr<pdb::QueryClient> queryClient,
                     Handle<PartitionComp<KeyClass, ValueClass>> partitionComp);


    /* to extract, partition and store the key in the data stored in the inputDatabaseAndSet*/
    /* @param errMsg: error message 
     * @param queryClient: the client used to send partition at the server
     * @return: whether this execution succeeds or not */
    bool partitionWithTransformation ( std::string & errMsg, 
                     std::shared_ptr<pdb::QueryClient> queryClient,
                     Handle<PartitionTransformationComp<KeyClass, ValueClass>> partitionComp);


private:

    /* the input set identifier */
    std::pair<std::string, std::string> inputDatabaseAndSet;

    /* the output set identifier */
    std::pair<std::string, std::string> outputDatabaseAndSet;



};


}

#include "Partitioner.cc"

#endif
