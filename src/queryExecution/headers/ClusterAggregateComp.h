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

#include "AggregateComp.h"
#include "ScanUserSet.h"

namespace pdb {

template <class OutputClass, class InputClass, class KeyClass, class ValueClass>
class ClusterAggregateComp : public AggregateComp<OutputClass, InputClass, KeyClass, ValueClass> {

public:

    ENABLE_DEEP_COPY

    ClusterAggregateComp (int numPartitions, int batchSize, std :: string dbName, std :: string setName) {
        this->numPartitions = numPartitions;
        this->outputSetScanner = makeObject<ScanUserSet>();
        this->outputSetScanner->initialize();
        this->outputSetScanner->setBatchSize(batchSize);
        this->outputSetScanner->setDatabaseName(dbName);
        this->outputSetScanner->setSetName(setName);
    }

    //intermediate results written to shuffle sink
    ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &projection, ComputePlan &plan) override {
        return std :: make_shared <ShuffleSink <KeyClass, ValueClass>> (numPartitions, consumeMe, projection);
    }

    //aggregation results written to user set
    ComputeSourcePtr getComputeSource (TupleSpec &outputScheme, ComputePlan &plan) override {
        if (outputSetScanner != nullptr) {
            return outputSetScanner->getComputeSource (outputScheme, plan);
        } 
        return nullptr;
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

private:

    //number of partitions in the cluster
    int numPartitions;
    Handle<ScanUserSet<OutputClass>> outputSetScanner = nullptr;

}

#endif
