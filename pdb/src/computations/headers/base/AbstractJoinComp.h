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
#ifndef JOINCOMPBASE_H
#define JOINCOMPBASE_H


namespace pdb {

/**
 * This class defines the interfaces for AggregateComp
 * This class is used in backend when type information is unknown
 */
class AbstractJoinComp : public Computation {

public:

    /**
     * Gets an execute that can run a scan join...
     *
     * @param needToSwapAtts - is true if the atts that are currently stored in the hash table need to
     * come SECOND in the output tuple sets...
     * @param hashedInputSchema - hashedInputSchema tells us the schema for the attributes that are
     * currently stored in the hash table...
     * @param pipelinedInputSchema - tells us the schema for the attributes that will be coming through the pipeline...
     * @param pipelinedAttsToOperateOn - is the identity of the hash attribute...
     * @param pipelinedAttsToIncludeInOutput - tells us the set of attributes that are coming through the pipeline
     * that we actually have to write to the output stream
     * @param arg - parameters that are sent into a pipeline when it is built
     * @return - the executor
     */
    virtual ComputeExecutorPtr getExecutor(bool needToSwapAtts,
                                           TupleSpec& hashedInputSchema,
                                           TupleSpec& pipelinedInputSchema,
                                           TupleSpec& pipelinedAttsToOperateOn,
                                           TupleSpec& pipelinedAttsToIncludeInOutput,
                                           ComputeInfoPtr arg) = 0;

    /**
     * Gets an execute that can run a scan join...
     *
     * @param needToSwapAtts - is true if the atts that are currently stored in the hash table need to
     * come SECOND in the output tuple sets...
     * @param hashedInputSchema - hashedInputSchema tells us the schema for the attributes that are
     * currently stored in the hash table...
     * @param pipelinedInputSchema - tells us the schema for the attributes that will be coming through the pipeline...
     * @param pipelinedAttsToOperateOn - is the identity of the hash attribute...
     * @param pipelinedAttsToIncludeInOutput - tells us the set of attributes that are coming through the pipeline
     * that we actually have to write to the output stream
     * @param arg - parameters that are sent into a pipeline when it is built
     * @return - the executor
     */
    virtual ComputeExecutorPtr getExecutor(bool needToSwapAtts,
                                           TupleSpec& hashedInputSchema,
                                           TupleSpec& pipelinedInputSchema,
                                           TupleSpec& pipelinedAttsToOperateOn,
                                           TupleSpec& pipelinedAttsToIncludeInOutput) = 0;

    /**
     * Used to return the type if of this computation
     * @return
     */
    ComputationTypeID getComputationTypeID() override {
        return JoinCompBaseTypeID;
    }
};
}

#endif
