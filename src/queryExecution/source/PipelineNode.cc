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
#include "PDBDebug.h"
#include "PipelineNode.h"
#include "SingleTableUnbundleProcessor.h"
#include "Configuration.h"
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
    PDB_COUT << "add child node " << node->getOperatorId() << " to node " << id << std::endl;
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
             PDB_COUT << "PipelineNode::unbundle(): Current allocation block is full" << std :: endl;
             logger->debug(std :: string( "PipelineNode::unbundle(): Current allocation block is full" )); 
             //copy data to output page
             size_t outputVecSize = context->getOutputVec()->size();
             if (outputVecSize > 0) {
                 Record<Vector<Handle<Object>>> * myBytes = getRecord(context->getOutputVec());
                 PDB_COUT << "###############################" << std :: endl;
                 PDB_COUT << "To flush query result objects: " << outputVecSize << std :: endl;
                 PDB_COUT << "###############################" << std :: endl;
                 memcpy(context->getPageToUnpin()->getBytes(), myBytes, myBytes->numBytes());

                 PDB_COUT << "PipelineNode: unpin output page" << std :: endl;
                 logger->debug(std :: string("PipelineNode: unpin output page"));
                 //unpin output page
                 context->getProxy()->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), true);

                 //pin a new output page
                 PDBPagePtr output;
                 PDB_COUT << "to add user page for output" << std :: endl;
                 logger->debug(std :: string("PipelineNode: to add user page for output"));
                 context->getProxy()->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(),
                       context->getOutputSet()->getSetId(), output);
                 PDB_COUT << "pinned page in output set with id=" << output->getPageID() << std :: endl;
                 logger->debug(std :: string("pinned page in output set with id=") + std :: to_string(output->getPageID()));
                 context->setPageToUnpin(output);
             }
             std :: string out = getAllocator().printInactiveBlocks();
             logger->info(out);
             makeObjectAllocatorBlock (DEFAULT_PAGE_SIZE, true);
             Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
             context->setOutputVec(outputVec);
             unbundler->loadOutputVector();
             
             context->setOutputFull(false);
    }

#ifdef DEBUG_PIPELINE
    PDB_COUT << "unbundled an input block!" << std :: endl;
    logger->debug(std :: string("PipelineNode: unbundled an input block!"));
#endif
    unbundler->clearOutputVec();
    unbundler->clearInputBlock();
    unbundler->setContext(nullptr);
    return true;
}

bool PipelineNode :: run(PipelineContextPtr context, Handle<GenericBlock> inputBatch, size_t batchSize, PDBLoggerPtr logger) {
#ifdef DEBUG_PIPELINE
    PDB_COUT << "running pipeline node with id=" << id << std :: endl;
#endif
    BlockQueryProcessorPtr processor = this->getProcessor(context);
#ifdef DEBUG_PIPELINE
    PDB_COUT << "got processor"<< std :: endl;
#endif
    processor->initialize();
    processor->loadInputBlock(inputBatch);
#ifdef DEBUG_PIPELINE
    PDB_COUT << "to load output block" << std :: endl;
    logger->debug(string("to load output block"));
#endif
    DataProxyPtr proxy = context->getProxy();
    PDBPagePtr output = nullptr;
    Handle<GenericBlock> outputBlock = nullptr;
    try {
        outputBlock = processor->loadOutputBlock();
    } catch (NotEnoughSpace &n) {
        PDB_COUT << "PipelineNode::run()--loadOutputBlock before loop: current block is full, copy to output page!" << std :: endl;
        logger->debug(std :: string("PipelineNode::run()--loadOutputBlock before loop: current block is full, copy to output page!"));
        size_t outputVecSize = context->getOutputVec()->size();
        if (outputVecSize > 0) {
            //copy data to output page
            Record<Vector<Handle<Object>>> * myBytes = getRecord(context->getOutputVec());
            PDB_COUT << "###############################" << std :: endl;
            PDB_COUT << "To flush query result objects: " << outputVecSize << std :: endl;               
            PDB_COUT << "###############################" << std :: endl;
            memcpy(context->getPageToUnpin()->getBytes(), myBytes, myBytes->numBytes());

            //unpin output page
            PDB_COUT << "PipelineNode: to unpin the output page" << std :: endl;
            logger->debug(std :: string("PipelineNode: to unpin the output page"));
            context->getProxy()->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), true);

            //pin a new output page
            PDB_COUT << "to add user page for output" << std :: endl;
            logger->debug(std :: string("PipelineNode: to add user page for output"));
            proxy->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(),
                       context->getOutputSet()->getSetId(), output);
            PDB_COUT << "pinned page in output set with id=" << output->getPageID() << std :: endl;
            logger->debug(std :: string("PipelineNode: pinned page in output set with id=") + std :: to_string(output->getPageID()));
            context->setPageToUnpin(output);
        }
        std :: string out = getAllocator().printInactiveBlocks();
        logger->info(out);
        makeObjectAllocatorBlock (DEFAULT_PAGE_SIZE, true);
        Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
        context->setOutputVec(outputVec);
        outputBlock = processor->loadOutputBlock();
    }
#ifdef DEBUG_PIPELINE
    PDB_COUT << "loaded an output block" << std :: endl;
    logger->debug("loaded an output block");
#endif
    while (processor->fillNextOutputBlock()) {
#ifdef DEBUG_PIPELINE
        PDB_COUT << id << ":written to a block" << std :: endl;
        logger->debug(std :: to_string(id)+std :: string( ":written to a block"));
#endif
        if (context->isOutputFull()) {
             PDB_COUT << "PipelineNode::run()--fillNextOutputBlock(): current block is full, copy to output page!" << std :: endl;
             logger->debug(std :: string("PipelineNode::run()--fillNextOutputBlock(): current block is full, copy to output page!"));
             size_t outputVecSize = context->getOutputVec()->size();
             if (outputVecSize > 0) {
                 //copy data to output page
                 Record<Vector<Handle<Object>>> * myBytes = getRecord(context->getOutputVec());  
                 PDB_COUT << "###############################" << std :: endl;
                 PDB_COUT << "To flush query result objects: " << outputVecSize << std :: endl;               
                 PDB_COUT << "###############################" << std :: endl;      
                 memcpy(context->getPageToUnpin()->getBytes(), myBytes, myBytes->numBytes());

                 PDB_COUT << "PipelineNode: to unpin the output page" << std :: endl;
                 logger->debug(std :: string("PipelineNode: to unpin the output page"));

                 //unpin output page
                 context->getProxy()->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), true);

                 //pin a new output page
                 PDB_COUT << "to add user page for output" << std :: endl;
                 logger->debug(std :: string("PipelineNode: to add user page for output"));
                 proxy->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(), 
                       context->getOutputSet()->getSetId(), output);
                 PDB_COUT << "pinned page in output set with id=" << output->getPageID() << std :: endl;
                 logger->debug(std :: string("PipelineNode: pinned page in output set with id=") + std :: to_string(output->getPageID()));
                 context->setPageToUnpin(output);
             }
             std :: string out = getAllocator().printInactiveBlocks();
             logger->info(out);
             makeObjectAllocatorBlock (DEFAULT_PAGE_SIZE, true);
             Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
             context->setOutputVec(nullptr);
             context->setOutputVec(outputVec);
             context->setOutputFull(false);
             try {
                 Handle<GenericBlock> newOutputBlock = processor->loadOutputBlock();
                 //newOutputBlock = deepCopyToCurrentAllocationBlock<GenericBlock>(outputBlock);
                 outputBlock = nullptr;
                 outputBlock = newOutputBlock;
             }
             catch (NotEnoughSpace &n) {
                 PDB_COUT << "PipelineNode: deep copy unfinished object meet problem when deep copy again" << std :: endl;
                 logger->error(std::string("PipelineNode: deep copy unfinished object meet problem when deep copy again"));
                 return false;
             } 

        }


        //we assume a run of pipeline will not consume all memory that has just been allocated
        for (int i = 0; i < this->children->size(); i ++) {
#ifdef DEBUG_PIPELINE 
             PDB_COUT << id << ": run " << i << "-th child" << std :: endl;
             logger->debug(std :: to_string(id)+std :: string(":run ")+std :: to_string(i)+std :: string("-th child"));
#endif
             children->at(i)->run(context, outputBlock, batchSize, logger);
#ifdef DEBUG_PIPELINE
             PDB_COUT << id <<": done" << i << "-th child" << std :: endl;
             logger->debug(std :: to_string(id)+std :: string(":done ")+std :: to_string(i)+std :: string("-th child"));
#endif
        }



        if (children->size() == 0) {
             //I am a sink node, run unbundling
#ifdef DEBUG_PIPELINE
             PDB_COUT << id << ": I'm a sink node" << std :: endl;
             logger->debug(std :: to_string(id)+std :: string(": I'm a sink node"));
#endif
             unbundle(context, outputBlock, logger);
#ifdef DEBUG_PIPELINE
             PDB_COUT << id << ": done a sink node" << std :: endl;
             logger->debug(std :: to_string(id)+std :: string(": done a sink node"));
#endif
        }

        
        try {
            outputBlock = processor->loadOutputBlock();
        } catch (NotEnoughSpace &n) {
            PDB_COUT << "PipelineNode::run()--loadOutputBlock() in loop: current block is full, copy to output page!" << std :: endl;
            logger->debug(std :: string("PipelineNode::run()--loadOutputBlock() in loop: current block is full, copy to output page!"));
            size_t outputVecSize = context->getOutputVec()->size();
            if (outputVecSize > 0) {
                //copy data to output page
                Record<Vector<Handle<Object>>> * myBytes = getRecord(context->getOutputVec());
                PDB_COUT << "###############################" << std :: endl;
                PDB_COUT << "To flush query result objects: " << outputVecSize << std :: endl;
                PDB_COUT << "###############################" << std :: endl;
                memcpy(context->getPageToUnpin()->getBytes(), myBytes, myBytes->numBytes());

                PDB_COUT << "to unpin user page for output" << std :: endl;
                logger->debug(std :: string("PipelineNode: to unpin user page for output"));


                //unpin output page
                context->getProxy()->unpinUserPage(nodeId, context->getPageToUnpin()->getDbID(), context->getPageToUnpin()->getTypeID(),
                       context->getPageToUnpin()->getSetID(), context->getPageToUnpin(), true);

                //pin a new output page
                PDB_COUT << "to add user page for output" << std :: endl;
                logger->debug(std :: string("PipelineNode: to add user page for output"));
                proxy->addUserPage(context->getOutputSet()->getDatabaseId(), context->getOutputSet()->getTypeId(),
                       context->getOutputSet()->getSetId(), output);
                PDB_COUT << "pinned page in output set with id=" << output->getPageID() << std :: endl;
                logger->debug(std :: string("PipelineNode: pinned page in output set with id=")+std :: to_string(output->getPageID()));
                context->setPageToUnpin(output);
            }
            std :: string out = getAllocator().printInactiveBlocks();
            logger->info(out);
            makeObjectAllocatorBlock (DEFAULT_PAGE_SIZE, true);
            Handle<Vector<Handle<Object>>> outputVec = makeObject<Vector<Handle<Object>>>();
            context->setOutputVec(nullptr);
            context->setOutputVec(outputVec);
            outputBlock = processor->loadOutputBlock();
       }
    }
#ifdef DEBUG_PIPELINE
    PDB_COUT << id << ": we processed the input block" << std :: endl;
    logger->debug(std :: string("PipelineNode:")+std :: to_string(id)+std :: string(": we processed the input block"));
#endif
    processor->clearInputBlock();

    //we assume a run of pipeline will not consume all memory that has just been allocated
    for (int i = 0; i < this->children->size(); i ++) {
#ifdef DEBUG_PIPELINE
            PDB_COUT << id << ": run " << i << "-th child" << std :: endl;
            logger->debug(std :: to_string(id)+std :: string(":run ")+std :: to_string(i)+std :: string("-th child"));
#endif
            children->at(i)->run(context, outputBlock, batchSize, logger);
#ifdef DEBUG_PIPELINE
            PDB_COUT << id <<": done" << i << "-th child" << std :: endl;
            logger->debug(std :: to_string(id)+std :: string(":done ")+std :: to_string(i)+std :: string("-th child"));
#endif
    }

    if (children->size() == 0) {
            //I am a sink node, run unbundling
#ifdef DEBUG_PIPELINE
            PDB_COUT << id << ": I'm a sink node" << std :: endl;
            logger->debug(std :: to_string(id)+std :: string(": I'm a sink node"));
#endif
            unbundle(context, outputBlock, logger);
#ifdef DEBUG_PIPELINE
            PDB_COUT << id << ": done a sink node" << std :: endl;
            logger->debug(std :: to_string(id)+std :: string(": done a sink node"));
#endif
    }


    processor->clearOutputBlock();
    outputBlock = nullptr;
    processor->setContext(nullptr); 
    return true;

}
}

#endif





