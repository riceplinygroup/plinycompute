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

PipelineNode :: PipelineNode (SimpleSingleTableQueryProcessorPtr processor, bool amISource, bool amISink, 
        Handle<SetIdentifier> inputSet, Handle<SetIdentifier> outputSet, OperatorID operatorId) {

    this->children = new std :: vector<PipelineNodePtr>();
    this->processor = processor;
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

void PipelineNode :: setContext(PipelineContextPtr context) {
    this->context = context;
}

PipelineContextPtr getContext() {
    return this->context;
}



bool PipelineNode :: run(DataProxyPtr proxy, void * inputBatch, int batchSize) {
    // TODO

    if (this->amISink == false) {
         RefCountedOnHeapSmallPagePtr smallPage = make_shared<RefCountedOnHeapSmallPage>(batchSize);
         this->processor->loadOutputPage(smallPage->getData(), smallPage->getSize());
    } else {
         //get the page from the set specified by the set identifier
         
    }
    this->processor->loadInputPage(smallPage->getData());
    
    while (this->processor->fillNextOutputPage()) {
        for (int i = 0; i < this->children->size(); i ++ ) {
            this->children->at(i)->run(smallPage->getData(), batchSize);
        }
        if (this->amISink == false) {
            smallPage = make_shared<RefCountedOnHeapSmallPage>(batchSize);
            this->processor->loadOutputPage(smallPage->getData(), smallPage->getSize());
        } else {
            //get the page from the set specified by the set identifier
        }
    }

}


#endif
