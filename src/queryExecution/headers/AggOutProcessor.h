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
#ifndef AGGOUT_PROCESSOR_H
#define AGGOUT_PROCESSOR_H

//by Jia, Mar 13 2017

#include "UseTemporaryAllocationBlock.h"
#include "InterfaceFunctions.h"
#include "PDBMap.h"
#include "PDBVector.h"
#include "Handle.h"

namespace pdb {

template <class OutputClass, class KeyType, class ValueType>
class AggOutProcessor {

public:

    ~AggOutProcessor () {};
    AggOutProcessor ();
    void initialize ();
    void loadInputPage (void * pageToProcess);
    void loadOutputPage (void * pageToWriteTo, size_t numBytesInPage) override;
    bool fillNextOutputPage ();
    void finalize ();
    void clearOutputPage ();
    void clearInputPage ();

private:

    UseTemporaryAllocationBlockPtr blockPtr;
    Handle <Map <KeyType, ValueType>> inputData;
    Handle <Vector<OutputClass> outputData;
    bool finalized;
    
    //the iterators for current map partition
    PDBMapIterator <KeyType, ValueType> begin;
    PDBMapIterator <KeyType, ValueType> end;

};

}


#include "AggOutProcessor.cc"


#endif
