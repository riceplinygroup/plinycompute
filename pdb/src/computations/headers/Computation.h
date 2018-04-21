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

#ifndef COMPUTATION_H
#define COMPUTATION_H

#include "Object.h"
#include "Lambda.h"
#include "ComputeSource.h"
#include "ComputeSink.h"
#include "SinkMerger.h"
#include "SinkShuffler.h"
#include "InputTupleSetSpecifier.h"
#include "PDBString.h"
#include <map>

namespace pdb {

class ComputePlan;

/**
 * List all the possible computation types
 */
enum ComputationTypeID {
    WriteUserSetTypeID,
    ScanUserSetTypeID,
    SelectionCompTypeID,
    PartitionCompTypeID,
    PartitionTransformationCompTypeID,
    AbstractPartitionCompID,
    AggregateCompTypeID,
    MultiSelectionCompTypeID,
    SetWriterTypeID,
    AbstractAggregateCompTypeID,
    ScanSetTypeID,
    JoinCompBaseTypeID,
    JoinCompTypeID,
    ClusterAggregationCompTypeID
};

/**
 * All nodes in a user-supplied computation are descended from this
 */
class Computation : public Object {

public:

    /**
    * This is implemented by the actual computation object... as the name implies, it is used
    * to extract the lambdas from the computation
    */
    virtual void extractLambdas(std::map<std::string, GenericLambdaObjectPtr>& returnVal) {}

    /**
     * If this particular computation can be used as a compute source in a pipeline, this
     * method will return the compute source object associated with the computation...
     *
     * In the general case, this method accepts the logical plan that this guy is a part of,
     * as well as the actual TupleSpec that this guy is supposed to produce, and then returns
     * a pointer to a ComputeSource object that can actually produce TupleSet objects corresponding
     * to that particular TupleSpec
     */
    virtual ComputeSourcePtr getComputeSource(TupleSpec& produceMe, ComputePlan& plan) {
        return nullptr;
    }

    /**
     * likewise, if this particular computation can be used as a compute sink in a pipeline, this
     * method will return the compute sink object associated with the computation.  It requires the
     * TupleSpec that should be processed, as well as the projection of that TupleSpec that will
     * be put into the sink
     */
    virtual ComputeSinkPtr getComputeSink(TupleSpec& consumeMe,
                                          TupleSpec& projection,
                                          ComputePlan& plan) {
        return nullptr;
    }

    virtual ComputeSinkPtr getComputeSink(TupleSpec& consumeMe,
                                          TupleSpec& whichAttsToOpOn,
                                          TupleSpec& projection,
                                          ComputePlan& plan) {
        return getComputeSink(consumeMe, projection, plan);
    }

    /**
     * interface for merging multiple join map sinks for broadcast join
     */
    virtual SinkMergerPtr getSinkMerger(TupleSpec& consumeMe,
                                        TupleSpec& projection,
                                        ComputePlan& plan) {
        return nullptr;
    }


    virtual SinkMergerPtr getSinkMerger(TupleSpec& consumeMe,
                                        TupleSpec& whichAttsToOpOn,
                                        TupleSpec& projection,
                                        ComputePlan& plan) {
        return getSinkMerger(consumeMe, projection, plan);
    }

    /// JiaNote: add below interface for shuffling multiple join map sinks for hash partitioned join

    virtual SinkShufflerPtr getSinkShuffler(TupleSpec& consumeMe,
                                            TupleSpec& projection,
                                            ComputePlan& plan) {
        return nullptr;
    }


    virtual SinkShufflerPtr getSinkShuffler(TupleSpec& consumeMe,
                                            TupleSpec& whichAttsToOpOn,
                                            TupleSpec& projection,
                                            ComputePlan& plan) {
        return getSinkShuffler(consumeMe, projection, plan);
    }

    // returns the type of this Computation
    virtual std::string getComputationType() = 0;

    // JiaNote: below function returns a TCAP string for this Computation
    virtual std::string toTCAPString(std::vector<InputTupleSetSpecifier>& inputTupleSets,
                                     int computationLabel,
                                     std::string& outputTupleSetName,
                                     std::vector<std::string>& outputColumnNames,
                                     std::string& addedOutputColumnName) = 0;

    /**
     * gets the name of the i^th input type...
     * @param i
     * @return
     */
    virtual std::string getIthInputType(int i) = 0;

    bool hasInput() {
        return !inputs.isNullPtr();
    }

    /**
     * get a handle to the i^th input to this query, which is also a query
     * @param i
     * @return
     */
    Handle<Computation>& getIthInput(int i) const {
        return (*inputs)[i];
    }

    /**
     * get the number of inputs to this query type
     * @return
     */
    virtual int getNumInputs() = 0;

    /**
     * gets the output type of this query as a string
     * @return
     */
    virtual std::string getOutputType() = 0;

    /**
     * gets the output type if of this query
     * @return
     */
    virtual ComputationTypeID getComputationTypeID() = 0;

    /**
     * get the number of consumers of this query
     * @return
     */
    int getNumConsumers() {
        return numConsumers;
    }

    void setNumConsumers(int numConsumers) {
        this->numConsumers = numConsumers;
    }

    /**
     * Set the first slot, by default
     * @param toMe
     * @return
     */
    bool setInput(Handle<Computation> toMe) {
        return setInput(0, toMe);
    }

    /**
     * sets the i^th input to be the output of a specific query... returns true if this is OK, false if it is not
     * @param whichSlot
     * @param toMe
     * @return
     */
    bool setInput(int whichSlot, Handle<Computation> toMe) {

        // set the array of inputs if it is a nullptr
        if (inputs == nullptr) {
            inputs = makeObject<Vector<Handle<Computation>>>(getNumInputs());
            for (int i = 0; i < getNumInputs(); i++) {
                inputs->push_back(nullptr);
            }
        }

        // if we are adding this query to a valid slot
        if (whichSlot < getNumInputs()) {


            // make sure the output type of the guy we are accepting meets the input type
            if (getIthInputType(whichSlot) != toMe->getOutputType()) {
                std::cout << "Cannot set output of query node with output of type "
                          << toMe->getOutputType() << " to be the input";
                std::cout << " of a query with input type " << getIthInputType(whichSlot) << ".\n";
                return false;
            }
            (*inputs)[whichSlot] = toMe;
            toMe->setNumConsumers(toMe->getNumConsumers() + 1);

        } else {

            return false;
        }
        return true;
    }


    /**
     * Whether the node has been traversed or not
     * @return
     */
    bool isTraversed() {

        return traversed;
    }

    /**
     * set the node to have been traversed
     * @param traversed
     */
    void setTraversed(bool traversed) {

        this->traversed = traversed;
    }

    /**
     * get output TupleSet name if the node has been traversed already
     * @return
     */
    std::string getOutputTupleSetName() {

        if (traversed) {
            return outputTupleSetName;
        }
        return "";
    }

    /**
     * set output TupleSet name. This method should be invoked by the TCAP string generation method
     * @param outputTupleSetName
     */
    void setOutputTupleSetName(std::string outputTupleSetName) {
        this->outputTupleSetName = outputTupleSetName;
    }

    /**
     * get output column name to apply if the node has been traversed already
     * @return
     */
    std::string getOutputColumnToApply() {

        if (traversed) {
            return outputColumnToApply;
        }
        return "";
    }

    /**
     * Set output column name to apply. This method should be invoked by the TCAP string generation method
     */
    void setOutputColumnToApply(std::string outputColumnToApply) {
        this->outputColumnToApply = outputColumnToApply;
    }

    /**
     * set user set for output when necessary (e.g. results need to be materialized)
     * by default it do nothing, subclasses shall override this function to handle the case when
     * results need to be materialized.
     */
    virtual void setOutput(std::string dbName, std::string setName) {}

    virtual std::string getDatabaseName() {
        return "";
    }

    virtual std::string getSetName() {
        return "";
    }

    virtual bool needsMaterializeOutput() {
        return false;
    }

    virtual void setBatchSize(int batchSize) {}

    virtual bool isUsingCombiner() {
        std::cout << "Only aggregation needs to set flag for combiner" << std::endl;
        return false;
    }

    virtual void setUsingCombiner(bool useCombinerOrNot) {
        std::cout << "Only aggregation needs to set flag for combiner" << std::endl;
    }


    void setAllocatorPolicy(AllocatorPolicy myPolicy) {
        this->myAllocatorPolicy = myPolicy;
    }

    void setObjectPolicy(ObjectPolicy myPolicy) {
        this->myObjectPolicy = myPolicy;
    }

    AllocatorPolicy getAllocatorPolicy() {
        return this->myAllocatorPolicy;
    }

    ObjectPolicy getObjectPolicy() {
        return this->myObjectPolicy;
    }

    /**
     * to set collectAsMap
     * @param collectAsMapOrNot
     */
    virtual void setCollectAsMap(bool collectAsMapOrNot){};

    /**
     * to check whether to do collectAsMap
     * @return
     */
    virtual bool isCollectAsMap() {
        return false;
    }

    /**
     * to get number of nodes to collect aggregation results
     * @return
     */
    virtual int getNumNodesToCollect() {
        return 0;
    }

    /**
     * to set number of nodes to collect aggregation results
     * @param numNodesToCollect
     */
    virtual void setNumNodesToCollect(int numNodesToCollect) {}

private:

    Handle<Vector<Handle<Computation>>> inputs = nullptr;

    bool traversed = false;

    String outputTupleSetName = "";

    String outputColumnToApply = "";

    int numConsumers = 0;

    AllocatorPolicy myAllocatorPolicy = AllocatorPolicy::defaultAllocator;

    ObjectPolicy myObjectPolicy = ObjectPolicy::defaultObject;
};
}

#endif
