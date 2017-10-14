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
#ifndef PIPELINE_NODE_H
#define PIPELINE_NODE_H


// by Jia, Sept 2016

#include "BlockQueryProcessor.h"
#include "DataTypes.h"
#include "SetIdentifier.h"
#include "DataProxy.h"
#include "PipelineContext.h"
#include "ExecutionOperator.h"
#include "BlockQueryProcessor.h"
#include "GenericBlock.h"
#include "PDBLogger.h"
#include <memory>
#include <vector>


namespace pdb {

class PipelineNode;
typedef std::shared_ptr<PipelineNode> PipelineNodePtr;

// this class encapsulates a pipeline node in the pipeline network

class PipelineNode {

private:
    // the children nodes of this node
    std::vector<PipelineNodePtr>* children;

    // the processor of this node
    Handle<ExecutionOperator> executionOperator;

    // a flag to indicate whether I am the source node
    bool amISource;

    // a flag to indicate whether I am the sink node
    bool amISink;

    // operator Id
    OperatorID id;

    // node id
    NodeID nodeId;

public:
    // destructor
    ~PipelineNode();

    // constructor
    PipelineNode(NodeID nodeId,
                 Handle<ExecutionOperator> executionOperator,
                 bool amISource,
                 bool amISink,
                 OperatorID operatorId);

    // get nodeId
    NodeID getNodeId();

    // return all children
    std::vector<PipelineNodePtr>* getChildren();

    // return whether I am the source node
    bool isSource();

    // return whether I am the sink node
    bool isSink();

    // return the operator Id
    OperatorID getOperatorId();

    // add child
    void addChild(PipelineNodePtr node);

    // running the pipeline
    bool run(PipelineContextPtr context,
             Handle<GenericBlock> inputBatch,
             size_t batchSize,
             PDBLoggerPtr logger);

    // unbundle a block to output vector
    bool unbundle(PipelineContextPtr context, Handle<GenericBlock> inputBatch, PDBLoggerPtr logger);

    // get a processor
    BlockQueryProcessorPtr getProcessor(PipelineContextPtr context);
};
}


#endif
