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


//by Jia, Sept 2016

#include "BlockQueryProcessor.h"
#include "DataTypes.h"
#include "SetIdentifier.h"
#include "RefCountedOnHeapSmallPage.h"
#include "DataProxy.h"
#include "PipelineContext.h"
#include "ExecutionOperator.h"
#include "BlockQueryProcessor.h"
#include "GenericBlock.h"
#include <memory>
#include <vector>

typdef std :: shared_ptr<PipelineNode> PipelineNodePtr;

namespace pdb {

//this class encapsulates a pipeline node in the pipeline network

class PipelineNode {

private:


    // the children nodes of this node
    std :: vector<PipelineNodePtr> * children;

    // the processor of this node
    Handle<ExecutionOperator> executionOperator;

    // a flag to indicate whether I am the source node
    bool amISource;

    // a flag to indicate whether I am the sink node
    bool amISink;

    // the identifier of the set as the input data of this node
    Handle<SetIdentifier> inputSet;

    // the identifier of the set as the output data of this node
    Handle<SetIdentifier> outputSet;

    // operator Id
    OperatorID id;    

   
public:

    // destructor
    ~PipelineNode ();

    // constructor
    PipelineNode (Handle<ExecutionOperator> executionOperator, bool amISource, bool amISink, Handle<SetIdentifier> inputSet, Handle<SetIdentifier> outputSet, OperatorID operatorId);

    // return all children
    std :: vector<PipelineNodePtr> * getChildren();

    // return whether I am the source node
    bool isSource();

    // return whether I am the sink node
    bool isSink();

    // return the set identifier as the input data of this node
    Handle<SetIdentifier> getInputSet();

    // return the set idenifier as the output data of this node
    Handle<SetIdentifier> getOutputSet();

    // return the operator Id
    OperatorID getOperatorId();

    // add child
    void addChild(PipelineNodePtr node);

    // running the pipeline
    bool run (Handle<GenericBlock> inputBatch, size_t batchSize);

    // get a processor
    BlockQueryProcessorPtr getProcessor(PipelineContextPtr context);

   
      
};


}


#endif
