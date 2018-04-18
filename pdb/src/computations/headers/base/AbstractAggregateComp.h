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

#include "Computation.h"
#include "DataTypes.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "DataProxy.h"
#include "PageCircularBufferIterator.h"
#include "ScanUserSet.h"
#include "DataTypes.h"
#include <vector>


namespace pdb {

/**
 * This class defines the interfaces for AggregateComp
 * This class is used in backend when type information is unknown
 */
class AbstractAggregateComp : public Computation {

public:

    /**
     * We are using the default constructor
     */
    AbstractAggregateComp() = default;

    /**
     * Used to get combiner processor
     *
     * @param nodePartitionIds the partition ids
     * @return - the combiner processor
     */
    virtual SimpleSingleTableQueryProcessorPtr getCombinerProcessor(std::vector<HashPartitionID> nodePartitionIds) = 0;

    /**
     * Used to get aggregation processor
     *
     * @param id - the id of the hash partition
     * @return the aggregation processor
     */
    virtual SimpleSingleTableQueryProcessorPtr getAggregationProcessor(HashPartitionID id) = 0;

    /**
     * Used to get the agg out processor

     * @return the aggregation out processor
     */
    virtual SimpleSingleTableQueryProcessorPtr getAggOutProcessor() = 0;

    /**
     * Used to set number of partitions
     * @param numPartitions - the new number of partitions
     */
    void setNumPartitions(int numPartitions) {
        this->numPartitions = numPartitions;
    }

    /**
     * Used to get the number of partitions
     *
     * @return the number of partitions
     */
    int getNumPartitions() {
        return this->numPartitions;
    }

    /**
     * Used to set number of nodes
     * @param numNodes - the new number of nodes
     */
    void setNumNodes(int numNodes) {
        this->numNodes = numNodes;
    }

    /**
     * Used to get number of nodes
     *
     * @return the number of nodes
     */
    int getNumNodes() {
        return this->numNodes;
    }

    /**
     * Used to set the batch size
     * @param batchSize - the new batch size
     */
    void setBatchSize(int batchSize) override {
        this->batchSize = batchSize;
    }

    /**
     * Used to get the batch size
     * @return
     */
    int getBatchSize() {
        return this->batchSize;
    }

    /**
     * Used to set iterator for reading from output
     * @param iterator - the iterator
     */
    virtual void setIterator(PageCircularBufferIteratorPtr iterator) = 0;

    /**
     * Used to set proxy for reading from output
     * @param proxy - the proxy
     */
    virtual void setProxy(DataProxyPtr proxy) = 0;

    /**
     * Used to set database name
     * @param dbName
     */
    virtual void setDatabaseName(std::string dbName) = 0;

    /**
     * Used to set the set name
     * @param setName
     */
    virtual void setSetName(std::string setName) = 0;

    /**
     * Used to return the type if of this computation
     * @return the type
     */
    ComputationTypeID getComputationTypeID() override {
        return AbstractAggregateCompTypeID;
    }

    /**
     * Does this aggregation need materialization
     * @return true if it does
     */
    bool needsMaterializeOutput() override {
        return this->materializeAggOut;
    }

    /**
     * Sets the hash table pointer
     * @param hashTableLocation - the pointer to the hash table
     */
    void setHashTable(void* hashTableLocation) {
        this->materializeAggOut = false;
        this->whereHashTableSitsForThePartition = hashTableLocation;
    }

    /**
     * Is this aggregation using a combiner
     * @return true if it does false otherwise
     */
    bool isUsingCombiner() override {
        return useCombinerOrNot;
    }

    /**
     * Set an indicator that says if this aggregation is using a combiner or not
     * @param useCombinerOrNot - true if does false otherwise
     */
    void setUsingCombiner(bool useCombinerOrNot) override {
        this->useCombinerOrNot = useCombinerOrNot;
    }


protected:
    /**
     * Number of partitions in the cluster
     */
    int numPartitions = -1;

    /**
     * Number of nodes in the cluster
     */
    int numNodes = -1;

    /**
     * The size of the batch
     */
    int batchSize = -1;

    /**
     * A pointer to the hash table
     */
    void* whereHashTableSitsForThePartition = nullptr;

    /**
     * Materialize the aggregation output or not
     */
    bool materializeAggOut = false;

    /**
     * Does this aggregation have a combiner
     */
    bool useCombinerOrNot = true;
};
}

#endif
