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
#ifndef CLUSTER_AGG_COMP
#define CLUSTER_AGG_COMP

//by Jia, Mar 2017


#include "AggregateComp.h"
#include "ScanUserSet.h"
#include "CombinerProcessor.h"
#include "AggregationProcessor.h"
#include "DataTypes.h"

namespace pdb {

template <class OutputClass, class InputClass, class KeyClass, class ValueClass>
class ClusterAggregateComp : public AggregateComp<OutputClass, InputClass, KeyClass, ValueClass> {

public:

    ENABLE_DEEP_COPY

    ClusterAggregateComp () {}

    //Not materialize aggregation output, use MapTupleSetIterator as consumer's ComputeSource
    ClusterAggregateComp (int numPartitions, int batchSize) {
        this->numPartitions = numPartitions;
        this->batchSize = batchSize;
        this->materializeAggOut = false;
        this->outputSetScanner = nullptr;
        this->whereHashTableSitsForThePartition = nullptr;
    }

    //materialize aggregation output, use ScanUserSet to obtain consumer's ComputeSource
    ClusterAggregateComp (int numPartitions, int batchSize, std :: string dbName, std :: string setName) {
        this->numPartitions = numPartitions;
        this->materializeAggOut = true;
        this->outputSetScanner = makeObject<ScanUserSet>();
        this->outputSetScanner->initialize();
        this->outputSetScanner->setBatchSize(batchSize);
        this->batchSize = batchSize;
        this->outputSetScanner->setDatabaseName(dbName);
        this->outputSetScanner->setSetName(setName);
        this->whereHashTableSitsForThePartition = nullptr;
    }

    //intermediate results written to shuffle sink
    ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &projection, ComputePlan &plan) override {
        return std :: make_shared <ShuffleSink <KeyClass, ValueClass>> (numPartitions, consumeMe, projection);
    }

    //aggregation results written to user set
    ComputeSourcePtr getComputeSource (TupleSpec &outputScheme, ComputePlan &plan) override {
        //materialize aggregation result to user set
        if( this->materializeAggOut == true) {
            if (outputSetScanner != nullptr) {
                return outputSetScanner->getComputeSource (outputScheme, plan);
            } 
            return nullptr;

        //not materialize aggregation result, keep them in hash table
        } else {
            if (whereHashTableSitsForThePartition != nullptr) {
                Handle <Object> myHashTable = ((Record <Object> *) whereHashTableSitsForThePartition)->getRootObject();
                return std :: make_shared <MapTupleSetIterator <KeyClass, ValueClass, OutputClass>> (myHashTable, batchSize);
            }
            return nullptr;

        }
    }

    std :: shared_ptr<CombinerProcessor<KeyClass, ValueClass>> getCombinerProcessor(Vector<HashPartitionID> nodePartitionIds) override {
        return make_shared<CombinerProcessor<KeyClass, ValueClass>> (this->numPartitions, nodePartitionIds.size(), nodePartitionIds);
    }

    std :: shared_ptr<AggregationProcessor<KeyClass, ValueClass>> getAggregationProcessor() override {
        return make_shared<AggregationProcessor<KeyClass, ValueClass>> ();
}

    std :: shared_ptr<AggOutProcessor<OutputClass, KeyClass, ValueClass>> getAggOutProcessor() override {
        return make_shared<AggOutProcessor<OutputClass, KeyClass, ValueClass>();

}



    std :: string getComputationType () override {
        return std :: string ("ClusterAggregationComp");
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

private:

    //number of partitions in the cluster
    int numPartitions;
    Handle<ScanUserSet<OutputClass>> outputSetScanner = nullptr;
    bool materializeAggOut;
    int batchSize;
    void * whereHashTableSitsForThePartition;
}

#endif
