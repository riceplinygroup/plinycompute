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
    //std :: cout << "PipelineNode with id=" << id << " is running" << std :: endl;
    executionOperator = nullptr;
    children->clear();
    delete children;

}

PipelineNode :: PipelineNode (NodeID nodeId, Handle<ExecutionOperator> executionOperator, bool amISource, bool amISink, OperatorID operatorId) {

    this->nodeId = nodeId;
    this->children = new std :: vector<PipelineNodePtr>();
    this->executionOperator = executionOperator;
    this->amISource = amISource;
    this->amISink = amISink;
    this->id = operatorId;
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

bool PipelineNode :: unbundle(PipelineContextPtr context, Handle<GenericBlock> inputBatch, PDBLoggerPtr logger) {
//    std :: cout << "PipelineNode: to unbundle a block" << std :: endl;
//    logger->debug(std :: string("PipelineNode: to unbundle a block"));
    SingleTableUnbundleProcessorPtr unbundler = std :: make_shared<SingleTableUnbundleProcessor>();
    unbundler->setContext(context);
    unbundler->initialize();
    unbundler->loadOutputVector();
    unbundler->loadInputBlock(inputBatch);
    DataProxyPtr proxy = context->getProxy();
    while (unbundler->fillNextOutputVector()) {
             std :: cout << "PipelineNode::unbundle(): Current allocation block is full" << std :: endl;
             logger->debug(std :: string( "PipelineNode::unbundle(): Current allocation block is full" )); 
             //copy data to output page
             Record<Vector<Handle<Object>>> * myBytes = getRecord(context->getOutputVec());
             memcpy(context->getPageToUnpin()->getBytes(), myBytes, myBytes->numBytes());

             //std :: cout << "PipelineNode: unpin output page" << std :: endl;
             logger->debug(std :: string("PipelineNode: unpin output page"));
             //unpin output page
             context->getProxy()->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), true);

             //pin a new output page
             PDBPagePtr output;
             std :: cout << "to add user page for output" << std :: endl;
             logger->debug(std :: string("PipelineNode: to add user page for output"));
             context->getProxy()->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(),
                       context->getOutputSet()->getSetId(), output);
             std :: cout << "pinned page in output set with id=" << output->getPageID() << std :: endl;
             logger->debug(std :: string("pinned page in output set with id=") + std :: to_string(output->getPageID()));
             std :: string out = getAllocator().printInactiveBlocks();
             logger->info(out);
             makeObjectAllocatorBlock (output->getSize(), true);
             Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
             context->setOutputVec(outputVec);
             context->setPageToUnpin(output);
             unbundler->loadOutputVector();
             
             context->setOutputFull(false);
    }
  //  std :: cout << "unbundled an input block!" << std :: endl;
  //  logger->debug(std :: string("PipelineNode: unbundled an input block!"));
    unbundler->clearOutputVec();
    unbundler->clearInputBlock();
    unbundler->setContext(nullptr);
    return true;
}

bool PipelineNode :: run(PipelineContextPtr context, Handle<GenericBlock> inputBatch, size_t batchSize, PDBLoggerPtr logger) {
    //std :: cout << "running pipeline node with id=" << id << std :: endl;
    BlockQueryProcessorPtr processor = this->getProcessor(context);
    //std :: cout << "got processor"<< std :: endl;
    processor->initialize();
    processor->loadInputBlock(inputBatch);
    //std :: cout << "to load output block" << std :: endl;
    //logger->debug(string("to load output block"));
    DataProxyPtr proxy = context->getProxy();
    PDBPagePtr output = nullptr;
    Handle<GenericBlock> outputBlock = nullptr;
    try {
        outputBlock = processor->loadOutputBlock();
    } catch (NotEnoughSpace &n) {
        std :: cout << "PipelineNode::run()--loadOutputBlock before loop: current block is full, copy to output page!" << std :: endl;
        logger->debug(std :: string("PipelineNode::run()--loadOutputBlock before loop: current block is full, copy to output page!"));
        //copy data to output page
        Record<Vector<Handle<Object>>> * myBytes = getRecord(context->getOutputVec());
        memcpy(context->getPageToUnpin()->getBytes(), myBytes, myBytes->numBytes());

        //unpin output page
        //std :: cout << "PipelineNode: to unpin the output page" << std :: endl;
        logger->debug(std :: string("PipelineNode: to unpin the output page"));
        context->getProxy()->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), true);

        //pin a new output page
        //std :: cout << "to add user page for output" << std :: endl;
        logger->debug(std :: string("PipelineNode: to add user page for output"));
        proxy->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(),
                       context->getOutputSet()->getSetId(), output);
        //std :: cout << "pinned page in output set with id=" << output->getPageID() << std :: endl;
        logger->debug(std :: string("PipelineNode: pinned page in output set with id=") + std :: to_string(output->getPageID()));
        std :: string out = getAllocator().printInactiveBlocks();
        logger->info(out);
        makeObjectAllocatorBlock (output->getSize(), true);
        Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
        context->setOutputVec(outputVec);
        context->setPageToUnpin(output);
        outputBlock = processor->loadOutputBlock();
    }
    //std :: cout << "loaded an output block" << std :: endl;
    //logger->debug("loaded an output block");
    while (processor->fillNextOutputBlock()) {
        //std :: cout << id << ":written to a block" << std :: endl;
        //logger->debug(std :: to_string(id)+std :: string( ":written to a block"));
        if (context->isOutputFull()) {
             std :: cout << "PipelineNode::run()--fillNextOutputBlock(): current block is full, copy to output page!" << std :: endl;
             logger->debug(std :: string("PipelineNode::run()--fillNextOutputBlock(): current block is full, copy to output page!"));
             //copy data to output page
             Record<Vector<Handle<Object>>> * myBytes = getRecord(context->getOutputVec());        
             memcpy(context->getPageToUnpin()->getBytes(), myBytes, myBytes->numBytes());

             std :: cout << "PipelineNode: to unpin the output page" << std :: endl;
             logger->debug(std :: string("PipelineNode: to unpin the output page"));

             //unpin output page
             context->getProxy()->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), true);

             //pin a new output page
             std :: cout << "to add user page for output" << std :: endl;
             logger->debug(std :: string("PipelineNode: to add user page for output"));
             proxy->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(), 
                       context->getOutputSet()->getSetId(), output);
             std :: cout << "pinned page in output set with id=" << output->getPageID() << std :: endl;
             logger->debug(std :: string("PipelineNode: pinned page in output set with id=") + std :: to_string(output->getPageID()));
             std :: string out = getAllocator().printInactiveBlocks();
             logger->info(out);
             makeObjectAllocatorBlock (output->getSize(), true);
             Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
             context->setOutputVec(nullptr);
             context->setOutputVec(outputVec);
             context->setPageToUnpin(output);
             context->setOutputFull(false);
        }



        //we assume a run of pipeline will not consume all memory that has just been allocated
        for (int i = 0; i < this->children->size(); i ++) {
          //   std :: cout << id << ": run " << i << "-th child" << std :: endl;
          //   logger->debug(std :: to_string(id)+std :: string(":run ")+std :: to_string(i)+std :: string("-th child"));
             children->at(i)->run(context, outputBlock, batchSize, logger);
          //   std :: cout << id <<": done" << i << "-th child" << std :: endl;
          //   logger->debug(std :: to_string(id)+std :: string(":done ")+std :: to_string(i)+std :: string("-th child"));

        }
        if (children->size() == 0) {
             //I am a sink node, run unbundling
            // std :: cout << id << ": I'm a sink node" << std :: endl;
            // logger->debug(std :: to_string(id)+std :: string(": I'm a sink node"));
             unbundle(context, outputBlock, logger);
            // std :: cout << id << ": done a sink node" << std :: endl;
            // logger->debug(std :: to_string(id)+std :: string(": done a sink node"));
        }
        
        try {
            outputBlock = processor->loadOutputBlock();
        } catch (NotEnoughSpace &n) {
            std :: cout << "PipelineNode::run()--loadOutputBlock() in loop: current block is full, copy to output page!" << std :: endl;
            logger->debug(std :: string("PipelineNode::run()--loadOutputBlock() in loop: current block is full, copy to output page!"));
            //copy data to output page
            Record<Vector<Handle<Object>>> * myBytes = getRecord(context->getOutputVec());
            memcpy(context->getPageToUnpin()->getBytes(), myBytes, myBytes->numBytes());

            //std :: cout << "to unpin user page for output" << std :: endl;
            logger->debug(std :: string("PipelineNode: to unpin user page for output"));


            //unpin output page
            context->getProxy()->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), true);

            //pin a new output page
            std :: cout << "to add user page for output" << std :: endl;
            logger->debug(std :: string("PipelineNode: to add user page for output"));
            proxy->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(),
                       context->getOutputSet()->getSetId(), output);
            std :: cout << "pinned page in output set with id=" << output->getPageID() << std :: endl;
            logger->debug(std :: string("PipelineNode: pinned page in output set with id=")+std :: to_string(output->getPageID()));
            std :: string out = getAllocator().printInactiveBlocks();
            logger->info(out);
            makeObjectAllocatorBlock (output->getSize(), true);
            Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
            context->setOutputVec(nullptr);
            context->setOutputVec(outputVec);
            context->setPageToUnpin(output);
            outputBlock = processor->loadOutputBlock();
       }
    }
    //std :: cout << id << ": we processed the input block" << std :: endl;
    //logger->debug(std :: string("PipelineNode:")+std :: to_string(id)+std :: string(": we processed the input block"));
    processor->clearInputBlock();

    //we assume a run of pipeline will not consume all memory that has just been allocated
    for (int i = 0; i < this->children->size(); i ++) {
      //      std :: cout << id << ": run " << i << "-th child" << std :: endl;
      //      logger->debug(std :: to_string(id)+std :: string(":run ")+std :: to_string(i)+std :: string("-th child"));
            children->at(i)->run(context, outputBlock, batchSize, logger);
      //      std :: cout << id <<": done" << i << "-th child" << std :: endl;
      //      logger->debug(std :: to_string(id)+std :: string(":done ")+std :: to_string(i)+std :: string("-th child"));

    }
    if (children->size() == 0) {
            //I am a sink node, run unbundling
        //    std :: cout << id << ": I'm a sink node" << std :: endl;
        //    logger->debug(std :: to_string(id)+std :: string(": I'm a sink node"));
            unbundle(context, outputBlock, logger);
        //    std :: cout << id << ": done a sink node" << std :: endl;
        //    logger->debug(std :: to_string(id)+std :: string(": done a sink node"));
    }

    processor->clearOutputBlock();
    outputBlock = nullptr;
    processor->setContext(nullptr); 
    return true;

}
}

#endif





