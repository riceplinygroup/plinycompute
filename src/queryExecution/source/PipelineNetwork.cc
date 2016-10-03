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
#ifndef PIPELINE_NETWORK_CC
#define PIPELINE_NETWORK_CC

//by Jia, Sept 2016

#include "PipelineNetwork.h"
#include "PageCircularBufferIterator.h"
#include "DataProxy.h"
#include "HermesExecutionServer.h"
#include "PageScanner.h"
#include "PageCircularBufferIterator.h"

namespace pdb {

PipelineNetwork :: ~PipelineNetwork () {
    delete sourceNodes;
    delete allNodes;
}

PipelineNetwork :: PipelineNetwork (SharedMemPtr shm, PDBLoggerPtr logger, ConfigurationPtr conf, NodeID nodeId, size_t batchSize, int numThreads) {
    sourceNodes = new std :: vector<PipelineNodePtr> ();
    allNodes = new std :: unordered_map<OperatorID, PipelineNodePtr>();
    this->batchSize = batchSize;
    this->numThreads = numThreads;
    this->nodeId = nodeId;
    this->logger = logger;
    this->conf = conf;
    this->shm = shm;
    this->id = 0;
}

void PipelineNetwork :: initialize (PipelineNodePtr parentNode, Handle<JobStage> stage) {
    if(parentNode == nullptr) {
        this->stageId = stage->getStageId(); 
    }
    Handle<JobStage> curStage = stage;
    if (curStage != nullptr) {   
        PipelineNodePtr nextParentNode = nullptr;
        Vector<Handle<ExecutionOperator>> operators = stage->getOperators();
        Handle<ExecutionOperator> sourceOperator = operators[0];
        SimpleSingleTableQueryProcessorPtr processor = sourceOperator->getProcessor();
        bool isSink;
        Handle<SetIdentifier> outputSet = nullptr;
        if (operators->size() == 1) {
            isSink = true;
            outputSet = stage->getOutput();
        } else {
            isSink = false;
        }
        PipelineNodePtr node = nullptr;
        if (parentNode == nullptr) {
            Handle<SetIdentifier> inputSet = stage->getInput();
            node = make_shared<PipelineNode>(processor, true, isSink, inputSet, outputSet, id);
            appendSourceNode (node);
        } else {
            node = make_shared<PipelineNode>(processor, false, isSink, nullptr, outputSet, id);
            appendNode(parentNode->getOperatorId(), node);
        }
        if (isSink == true) {
            nextParentNode = node;
        }
        id ++;
        for (int i = 1; i < operators.size(); i++) {
            Handle<ExecutionOperator> curOperator = operators[i];
            SimpleSingleTableQueryProcessorPtr curProcessor = curOperator->getProcessor();
            bool isSource = false;
            Handle<SetIdentifier> inputSet = nullptr;
            bool isSink = false;
            Handle<SetIdentifier> outputSet = nullptr;
            if(i == operators.size()-1) {
                isSink = true;
                outputSet = stage->getOutput();
            }
            PipelineNodePtr node = make_shared<PipelineNode>(curProcessor, isSource, isSink, inputSet, outputSet, id);
            appendNode(id - 1, node);
            id ++;
            if (isSink == true) {
                nextParentNode = node;
            }
        }
        Vector<Handle<JobStage>> childrenStage = stage->getChildrenStages();
        for (int i = 0; i < childrenStage.size(); i ++) {
            initialize(nextParentNode, childrenStage[i]);
        }
    }
}


JobStageID PipelineNetwork :: getStageId () {
    return stageId;
}

std :: vector<PipelineNodePtr> PipelineNetwork :: getSourceNodes() {
    return this->sourceNodes;
}

bool PipelineNetwork :: appendNode (OperatorID parentId, PipelineNodePtr node) {
    auto iter = allNodes->find(parentId);
    if ( iter == allNodes->end() ) {
        std :: cout << "Can't find parent node" << std :: endl;
        return false;
    } else {
        (*allNodes)[parentId]->addChild(node);
        (*allNodes)[node->getOperatorId()] = node;
        return true;
    }
}

void PipelineNetwork :: appendSourceNode (PipelineNodePtr node) {
    this->sourceNodes->push_back(node);
    (*allNodes)[node->getOperatorId()] = node;
    return;
}

int PipelineNetwork :: getNumThreads () {
    return this->numThreads;

}

int PipelineNetwork :: getNumsources () {
     return this->sourceNodes->size();
} 

void PipelineNetwork :: runAllSources() {
     //TODO
}

void PipelineNetwork :: runSource (int sourceNode) {
    bool success;
    std :: string errMsg;
    PipelineNodePtr source = this->sourceNodes->at(sourceNode);
    //initialize the data proxy, scanner and set iterators
    PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>(); 
    communicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);

    //getScanner
    int backendCircularBufferSize = 3;
    PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, logger, numThreads, backendCircularBufferSize, nodeId); 
    if (getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
        success = false;
        errMsg = "Error: A job is already running!";
        std :: cout << errMsg << std :: endl;
        return;
    }

    //get set information
    SetIdentifier set = source->getSet();

    //get iterators
    std :: vector<PageCircularBufferIteratorPtr> iterators = scanner->getSetIterators(nodeId, set->getDatabaseId(), set->getTypeId(), set->getSetId());

    int numIteratorsReturned = iterators.size();
    if (numIteratorsReturned != numThreads) {
         success = false;
         errMsg = "Error: number of iterators doesn't match number of threads!";
         std :: cout << errMsg << std :: endl;
         return;
    }   

    //create a buzzer and counter
    PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
         [&] (PDBAlarm myAlarm, int & counter) {
             counter ++;
             std :: cout << "counter = " << counter << std :: endl;
         });
    
    int counter = 0;
    int batchSize = 4096;
    for (int i = 0; i < numThreads; i++) {
         PDBWorkerPtr worker = getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
         //std :: cout << "to run the " << i << "-th work..." << std :: endl;
         //TODO: start threads
         PDBWorkPtr myWork = make_shared<GenericWork> (
             [&, i] (PDBBuzzerPtr callerBuzzer) {
                  //create a data proxy
                  PDBCommunicatorPtr anotherCommunicatorToFrontend = make_shared<PDBCommunicator>();
                  anotherCommunicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
                  DataProxyPtr proxy = make_shared<DataProxy>(nodeId, anotherCommunicatorToFrontend, shm, logger);
                  PageCircularBufferIteratorPtr iter = iterators.at(i);
                  while (iter->hasNext()) {
                      PDBPagePtr page = iter->next();
                      if (page != nullptr) {
                          source->run(proxy, page->getBytes(), batchSize);
                          proxy->unpinUserPage(nodeId, page->getDbID(), page->getTypeID(), page->getSetID(), page);  
                      }
                  }

             }

         );
         worker->execute(myWork, tempBuzzer);
    }

    while (counter < numThreads) {
         tempBuzzer->wait();
    }

    return;
    

}

}

#endif
