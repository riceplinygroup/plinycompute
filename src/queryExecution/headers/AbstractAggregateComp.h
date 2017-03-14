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
#ifndef ABSTRACT_AGG_COMP
#define ABSTRACT_AGG_COMP

//by Jia, Mar 13 2017


#include "Computation.h"
#include "DataTypes.h"
#include "SimpleSingleTableQueryProcessor.h"

namespace pdb {

class AbstractAggregateComp : public Computation {

public:
   
    virtual SimpleSingleTableQueryProcessorPtr getCombinerProcessor (Vector<HashPartitionID> nodePartitionIds) {
        return nullptr;
    }

    virtual SimpleSingleTableQueryProcessorPtr getAggregationProcessor () {
        return nullptr;
    }

    virtual SimpleSingleTableQueryProcessorPtr getAggOutProcessor () {
        return nullptr;
    }

   void setNumPartitions (int numPartitions) {
        this->numPartitions = numPartitions;
    }

    int getNumPartitions () {
        return this->numPartitions;
    }

    void setIterator(PageCircularBufferIteratorPtr iterator) {
        this->outputSetScanner->setIterator(iterator);
    }

    void setProxy(DataProxyPtr proxy) {
        this->outputSetScanner->setProxy(proxy);
    }

    void setDatabaseName (std :: string dbName) {
        this->outputSetScanner->setDatabaseName(dbName);
    }

    void setSetName (std :: string setName) {
        this->outputSetScanner->setSetName(setName);
    }

    std :: string getDatabaseName () {
        return this->outputSetScanner->getDatabaseName();
    }

    std :: string getSetName () {
        return this->outputSetScanner->getSetName();
    }

    void setHashTable (void * hashTable) {
        this->whereHashTableSitsForThePartition = hashTable;
    }

protected:

    //number of partitions in the cluster
    int numPartitions;
    Handle<ScanUserSet<OutputClass>> outputSetScanner = nullptr;
    bool materializeAggOut;
    int batchSize;
    void * whereHashTableSitsForThePartition;

};


}

#endif

