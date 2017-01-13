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
#ifndef SINGLE_TABLE_BUNDLE_PROCESSOR_CC
#define SINGLE_TABLE_BUNDLE_PROCESSOR_CC
#include "PDBDebug.h"
#include "SingleTableBundleProcessor.h"

namespace pdb {

SingleTableBundleProcessor :: ~SingleTableBundleProcessor() {
    PDB_COUT << "Total input objects number =" << this->totalSize << std :: endl;
    this->clearInputPage();
    this->clearOutputBlock();
    this->context = nullptr;
}

SingleTableBundleProcessor :: SingleTableBundleProcessor () {

    this->batchSize = 100;
    this->context = nullptr;
    this->totalSize = 0;
} 

void SingleTableBundleProcessor :: initialize () {

    finalized = false;   

}

void SingleTableBundleProcessor :: loadInputPage (void * pageToProcess) {

    Record <Vector <Handle<Object>>> * myRec = (Record <Vector<Handle<Object>>> *) pageToProcess;
    inputVec = myRec->getRootObject ();
    PDB_COUT << "#######################################" << std :: endl;
    PDB_COUT << "Loaded a page with " << inputVec->size() << " objects"<< std :: endl;
    PDB_COUT << "#######################################" << std :: endl;
    posInInput = 0;

}

Handle<GenericBlock> SingleTableBundleProcessor :: loadOutputBlock ( size_t batchSize) {

    this->clearOutputBlock();
    this->outputBlock = makeObject<GenericBlock> ();
    this->batchSize = batchSize;
    return this->outputBlock;
}

bool SingleTableBundleProcessor :: fillNextOutputBlock () {

    Vector<Handle<Object>> &myInputVec = *(inputVec);
    Vector<Handle<Object>> &myOutputVec = this->outputBlock->getBlock();

    // we are finalized in processing the input page
    if (finalized) {
        return false;
    }
    // we are not finalized, so process the page
    try {
        int vecSize = myInputVec.size();
        int posToFinish = posInInput + batchSize;
//        std :: cout << "posInInput=" << posInInput << std :: endl;
//        std :: cout << "posToFinish=" << posToFinish << std :: endl;
        for (; ((posInInput < vecSize) && (posInInput < posToFinish)); posInInput++) {
//            std :: cout << "posInInput=" << posInInput << std :: endl;
              myOutputVec.push_back(myInputVec[posInInput]);
              totalSize ++;
        }
        //std :: cout << "posInInput=" << posInInput << std :: endl;
        //std :: cout << "posToFinish=" << posToFinish << std :: endl;
        if (posInInput==vecSize) {
            return false;
        } else {
            return true;
        }
    } catch (NotEnoughSpace &n) {
        //std :: cout << "got exception, the page is full" << std :: endl;
        //std :: cout << "loaded " << totalSize << " objects in total" << std :: endl;
        if (this->context != nullptr) {
            //because final output and intermediate data are allocated on the same page, due to object model limitation
            //getRecord(this->context->outputVec);
            context->setOutputFull(true);
        }
        return true; 
    }

}

void SingleTableBundleProcessor :: finalize () {

   finalized = true;

}

void SingleTableBundleProcessor :: clearOutputBlock () {

    this->outputBlock = nullptr;

}

void SingleTableBundleProcessor :: clearInputPage () {

    this->inputVec = nullptr;

}

void SingleTableBundleProcessor :: setContext (PipelineContextPtr context) {

    this->context = context;

}

PipelineContextPtr SingleTableBundleProcessor :: getContext() {

    return this->context;

}

}



#endif
