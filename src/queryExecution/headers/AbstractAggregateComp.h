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
#include "DataProxy.h"
#include "PageCircularBufferIterator.h"
#include "ScanUserSet.h"

namespace pdb {

class AbstractAggregateComp : public Computation {

public:
   
    virtual SimpleSingleTableQueryProcessorPtr getCombinerProcessor (Vector<HashPartitionID> nodePartitionIds) = 0;

    virtual SimpleSingleTableQueryProcessorPtr getAggregationProcessor () = 0;

    virtual SimpleSingleTableQueryProcessorPtr getAggOutProcessor () = 0;

   void setNumPartitions (int numPartitions) {
        this->numPartitions = numPartitions;
    }

    int getNumPartitions () {
        return this->numPartitions;
    }

    virtual void setIterator(PageCircularBufferIteratorPtr iterator) = 0;

    virtual void setProxy(DataProxyPtr proxy) = 0;

    virtual void setDatabaseName (std :: string dbName) = 0;

    virtual void setSetName (std :: string setName) = 0;

    virtual std :: string getDatabaseName () = 0;

    virtual std :: string getSetName () = 0;

    void setHashTable (void * hashTable) {
        this->whereHashTableSitsForThePartition = hashTable;
    }

protected:

    //number of partitions in the cluster
    int numPartitions;
    bool materializeAggOut;
    int batchSize;
    void * whereHashTableSitsForThePartition;

};


}

#endif

