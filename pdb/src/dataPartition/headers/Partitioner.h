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
#include "ScanUserSet.h"
#include "WriteUserSet.h"
#include "QueryClient.h"

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
                 Handle<PartitionComp<OutputClass, InputClass>> partitionComp);


    /* to partition the data stored in the inputDatabaseAndSet */
    /* @param errMsg: error message
    /* @return: whether this execution succeeds or not */
    bool partition ( std::string & errMsg, std::shared_ptr<pdb::QueryClient> queryClient);

protected:


    /* to create a scanner computation */
    /* @return: the scanner computation reading from the input set
     */
    Handle<ScanUserSet<InputClass>> getScanner ();


    /* to create a writer computation */
    /* @return: the writer computation writing to the output set
     */
    Handle<WriteUserSet<OutputClass>> getWriter (); 



private:

    /* the input set identifier */
    std::pair<std::string, std::string> inputDatabaseAndSet;

    /* the output set identifier */
    std::pair<std::string, std::string> outputDatabaseAndSet;

    /* the partition computation for this partitioner */
    Handle<PartitionComp<OutputClass, InputClass>> partitionComp = nullptr;


};


}

#endif
