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
#ifndef PIPELINE_NODE_CC
#define PIPELINE_NODE_CC

//by Jia, Sept 2016

#include "PipelineNode.h"

PipelineNode :: ~PipelineNode () {

    delete children;

}

PipelineNode :: PipelineNode (Handle<ExecutionOperator> executionOperator, bool amISource, bool amISink, 
        Handle<SetIdentifier> inputSet, Handle<SetIdentifier> outputSet, OperatorID operatorId) {

    this->children = new std :: vector<PipelineNodePtr>();
    this->executionOperator = executionOperator;
    this->amISource = amISource;
    this->amISink = amISink;
    this->inputSet = inputSet;
    this->outputSet = outputSet;
    this->operatorId = operatorId;
    this->children = new std :: vector<PipelineNodePtr>();
    this->context = nullptr;
}


std :: vector<PipelineNodePtr> PipelineNode :: getChildren() {
    return this->children;
}

bool PipelineNode :: isSource() {
    return this->amISource;
}

bool PipelineNode :: isSink() {
    return this->amISink;
}

Handle<SetIdentifier> PipelineNode :: getInputSet() {
    return this->inputSet;
}

Handle<SetIdentifier> PipelineNode :: getOutputSet() {
    return this->outputSet;
}

OperatorID PipelineNode :: getOperatorId() {
    return this->id;
}

void PipelineNode :: addChild(PipelineNodePtr node) {
    this->children->push_back(node);
}


BlockQueryProcessorPtr PipelineNode::getProcessor(PipelineContextPtr context) {
    BlockQueryProcessorPtr processor = this->executionOperator->getProcessor();
    processor->setContext(context);
    return processor;
}

bool PipelineNode :: unbundle(PipelineContextPtr context, Handle<GenericBlock> inputBatch) {
    SingleTableUnbundleProcessorPtr unbundler = std :: make_shared<SingleTableUnbundleProcessor>();
    unbundler->setContext(context);
    unbundler->initialize();
    unbundler->loadOutput();
    unbundler->loadInputBlock(inputBatch);
    while (processor->fillNextOutputVector()) {
             proxy->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin());
             PDBPagePtr output;
             context->getProxy()->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(),
                       context->getOutputSet()->getSetId(), output);
             makeObjectAllocatorBlock (output->getBytes(), output->getSize(), true);
             context->setOutputFull(false);
             Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
             context->setOutputVec(outputVec);
             context->setPageToUnpin(output);
    }
    unbundler->clearOutputVec();
    unbundler->clearInputBlock();

}

bool PipelineNode :: run(PipelineContextPtr context, Handle<GenericBlock> inputBatch, int batchSize) {
    BlockQueryProcessorPtr processor = this->getProcessor(context);
    processor->initialize();
    processor->loadInputBlock(inputBatch);
    Handle<GenericBlock> outputBlock = processor->loadOutputBlock();
    while (processor->fillNextOutputBlock()) {
        //TODO: we need to unpin the previous output page
        if (context->isOutputFull()) {
             context->setNeedUnpin(true);
             PDBPagePtr output;
             context->getProxy()->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(), 
                       context->getOutputSet()->getSetId(), output);
             makeObjectAllocatorBlock (output->getBytes(), output->getSize(), true);
             context->setOutputFull(false);
             Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
             context->setOutputVec(outputVec);
             context->setOutputFull(false);
        }

        //we assume a run of pipeline will not consume all memory that has just been allocated
        for (i = 0; i < this->children->size(); i ++) {
             children->at(i)->run(context, outputBlock, batchSize);
        }
        if (children->size() == 0) {
             //I am a sink node, run unbundling
             unbundle(context, outputBlock);
             
        }
        
        //now we can unpin the previous page
        if (context->shallWeUnpin()) {
            proxy->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                context->getPageToUnpin()->getSetID(), context->getPageToUnpin());
            context->setPageToUnpin(output);
            context->setNeedUnpin(false);
        }
        outputBlock = processor->loadOutputBlock();
    }
    processor->clearOutputBlock();
    processor->clearInputBlock();
}


#endif
