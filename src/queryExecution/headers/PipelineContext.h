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
#ifndef PIPELINE_CONTEXT_H
#define PIPELINE_CONTEXT_H

//by Jia, Oct 2016

#include "Handle.h"
#include "PDBVector.h"
#include "Object.h"
#include <memory>

typedef shared_ptr<PipelineContext> PipelineContextPtr;

using namespace pdb {


//this class encapsulates the global state that is shared by pipeline nodes in the same pipeline network
class PipelineContext {

    private:

    //the final output vector that needs to invoke getRecord() on
    Handle<Vector<Handle<Object>> outputVec;

    //the number of GenericBlocks allocated in the output page that hasn't been read
    int numUnreadGenericBlocks;

    public:

    PipelineContext (Handle<Vector<Handle<Object>>> outputVec) {
        this->outputVec = outputVec;
        int numUnreadGenericBlocks = 0;
    }

    Handle<Vector<Handle<Object>>> getOutputVec() {
        return this->outputVec;
    }

    void incNumUnreadGenericBlocks() {
        numUnreadGenericBlocks ++;
    }

    void decNumUnreadGenericBlocks() {
        numUnreadGenericBlocks --;
    }

    void clearOutputPage() {
        outputVec = nullptr;
    }

}



}




#endif
