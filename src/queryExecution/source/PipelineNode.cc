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
#include "SingleTableUnbundleProcessor.h"

namespace pdb {
PipelineNode :: ~PipelineNode () {

    delete children;

}

PipelineNode :: PipelineNode (NodeID nodeId, Handle<ExecutionOperator> executionOperator, bool amISource, bool amISink, 
        Handle<SetIdentifier> inputSet, Handle<SetIdentifier> outputSet, OperatorID operatorId) {

    this->nodeId = nodeId;
    this->children = new std :: vector<PipelineNodePtr>();
    this->executionOperator = executionOperator;
    this->amISource = amISource;
    this->amISink = amISink;
    this->inputSet = inputSet;
    this->outputSet = outputSet;
    this->id = operatorId;
    this->children = new std :: vector<PipelineNodePtr>();
}


std :: vector<PipelineNodePtr> * PipelineNode :: getChildren() {
    return this->children;
}

NodeID PipelineNode :: getNodeId() {
    return this->nodeId;
}

bool PipelineNode :: isSource() {
    return this->amISource;
}

bool PipelineNode :: isSink() {
    return this->amISink;
}

Handle<SetIdentifier>& PipelineNode :: getInputSet() {
    return this->inputSet;
}

Handle<SetIdentifier>& PipelineNode :: getOutputSet() {
    return this->outputSet;
}

OperatorID PipelineNode :: getOperatorId() {
    return this->id;
}

void PipelineNode :: addChild(PipelineNodePtr node) {
    std::cout << "add child node " << node->getOperatorId() << " to node " << id << std::endl;
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
    unbundler->loadOutputVector();
    unbundler->loadInputBlock(inputBatch);
    DataProxyPtr proxy = context->getProxy();
    while (unbundler->fillNextOutputVector()) {
             std :: cout << "page is full" << std :: endl;
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
    std :: cout << "unbundled an input block!" << std :: endl;
    unbundler->clearOutputVec();
    unbundler->clearInputBlock();
    return true;
}

bool PipelineNode :: run(PipelineContextPtr context, Handle<GenericBlock> inputBatch, size_t batchSize) {
    std :: cout << "running pipeline node with id=" << id << std :: endl;
    BlockQueryProcessorPtr processor = this->getProcessor(context);
    std :: cout << "got processor" << std :: endl;
    processor->initialize();
    processor->loadInputBlock(inputBatch);
    std :: cout << "to load output block" << std :: endl;
    DataProxyPtr proxy = context->getProxy();
    PDBPagePtr output = nullptr;
    Handle<GenericBlock> outputBlock = nullptr;
    try {
        outputBlock = processor->loadOutputBlock();
    } catch (NotEnoughSpace &n) {
        std :: cout << "page is full" << std :: endl;
        context->setNeedUnpin(true);
        proxy->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(),
                       context->getOutputSet()->getSetId(), output);
        makeObjectAllocatorBlock (output->getBytes(), output->getSize(), true);
        Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
        context->setOutputVec(outputVec);
        outputBlock = processor->loadOutputBlock();
    }
    std :: cout << "loaded an output block" << std :: endl;
    while (processor->fillNextOutputBlock()) {
        std :: cout << id << ":written to a block" << std :: endl;
        if (context->isOutputFull()) {
             std :: cout << "page is full" << std :: endl;
             context->setNeedUnpin(true);
             proxy->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(), 
                       context->getOutputSet()->getSetId(), output);
             makeObjectAllocatorBlock (output->getBytes(), output->getSize(), true);
             Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
             context->setOutputVec(outputVec);
             context->setOutputFull(false);
        }

        //we assume a run of pipeline will not consume all memory that has just been allocated
        for (int i = 0; i < this->children->size(); i ++) {
             std :: cout << id << ": run " << i << "-th child" << std :: endl;
             children->at(i)->run(context, outputBlock, batchSize);

        }
        if (children->size() == 0) {
             //I am a sink node, run unbundling
             std :: cout << id << ": I'm a sink node" << std :: endl;
             //unbundle(context, outputBlock);
        }
        
        //now we can unpin the previous page
        if (context->shallWeUnpin()) {
            std :: cout << "page is to be unpinned" << std :: endl;
            proxy->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), false);
            context->setPageToUnpin(output);
            context->setNeedUnpin(false);
        }
        outputBlock = processor->loadOutputBlock();
    }
    std :: cout << id << ": we processed the input block" << std :: endl;
    processor->clearInputBlock();

    //we assume a run of pipeline will not consume all memory that has just been allocated
    for (int i = 0; i < this->children->size(); i ++) {
        std :: cout << id << ": run " << i << "-th child" << std :: endl;
        children->at(i)->run(context, outputBlock, batchSize);
    }

    if (children->size() == 0) {
        //I am a sink node, run unbundling
        std :: cout << id << ": I'm a sink node" << std :: endl;
        //unbundle(context, outputBlock);
   }

   //now we can unpin the previous page
   if (context->shallWeUnpin()) {
        std :: cout << "page is to be unpinned" << std :: endl;
        proxy->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), false);
        context->setPageToUnpin(output);
        context->setNeedUnpin(false);
   }

   processor->clearOutputBlock();

   return true;

}
}

#endif
