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
#ifndef SINGLE_TABLE_UNBUNDLE_PROCESSOR_CC
#define SINGLE_TABLE_UNBUNDLE_PROCESSOR_CC

#include "SingleTableUnbundleProcessor.h"

namespace pdb {

SingleTableUnbundleProcessor :: ~SingleTableUnbundleProcessor() {

    this->clearInputBlock();
    this->clearOutputVec();

}

SingleTableUnbundleProcessor :: SingleTableUnbundleProcessor () {

    this->batchSize = 100;
    this->context = nullptr;
} 

void SingleTableUnbundleProcessor :: initialize () {

    finalized = false;   

}

void SingleTableUnbundleProcessor :: loadInputBlock (Handle<GenericBlock> inputBlock) {

    this->inputBlock = inputBlock;
    posInInput = 0;

}

void SingleTableUnbundleProcessor :: loadOutputVector () {

    this->clearOutputVec();
    this->outputVec = this->context->getOutputVec();
}

bool SingleTableUnbundleProcessor :: fillNextOutputVector () {

    Vector<Handle<Object>> &myInputVec = this->inputBlock->getBlock();
    Vector<Handle<Object>> &myOutputVec = *outputVec;

    // we are finalized in processing the input page
    if (finalized) {
        return false;
    }

    // we are not finalized, so process the page
    try {
        int vecSize = myInputVec.size();
        for (; posInInput < vecSize; posInInput++) {
            myOutputVec.push_back(myInputVec[posInInput]);
        }
        //an output block is finished.
        return false;
    } catch (NotEnoughSpace &n) {
        if (this->context != nullptr) {
            //because final output and intermediate data are allocated on the same page, due to object model limitation
            getRecord(this->context->getOutputVec());
            context->setOutputFull(true);
        }
        return true; 
    }

}

void SingleTableUnbundleProcessor :: finalize () {

   finalized = true;

}

void SingleTableUnbundleProcessor :: clearOutputVec () {

    this->outputVec = nullptr;

}

void SingleTableUnbundleProcessor :: clearInputBlock () {

    this->inputBlock = nullptr;

}

void SingleTableUnbundleProcessor :: setContext (PipelineContextPtr context) {

    this->context = context;

}

PipelineContextPtr SingleTableUnbundleProcessor :: getContext() {

    return this->context;

}

}



#endif
