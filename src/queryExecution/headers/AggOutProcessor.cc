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
#ifndef AGGOUT_PROCESSOR_CC
#define AGGOUT_PROCESSOR_CC

#include "AggOutProcessor.h"

namespace pdb {

//OutputClass must have getKey() and getValue() methods implemented
template <class OutputClass, class KeyType, class ValueType>
AggOutProcessor <OutputClass, KeyType, ValueType> :: AggOutProcessor () {

    finalized = false;
}

//initialize
template <class OutputClass, class KeyType, class ValueType>
void AggOutProcessor <OutputClass, KeyType, ValueType> :: initialize () {
    finalized = false;
}

//loads up another input page to process
template <class OutputClass, class KeyType, class ValueType>
void AggOutProcessor <OutputClass, KeyType, ValueType> :: loadInputPage (void * pageToProcess) {

    Record <Map<KeyType, ValueType>> * myRec = (Record <Map<KeyType, ValueType>> *) pageToProcess;
    inputData = myRec->getRootObject();
    begin = new PDBMapIterator <KeyType, ValueType>(inputData->getArray(), true);
    end = new PDBMapIterator <KeyType, ValueType>(inputData->getArray());
}

//loads up another output page to write results to
template <class OutputClass, class KeyType, class ValueType>
void AggOutProcessor <OutputClass, KeyType, ValueType> :: loadOutputPage (void * pageToWriteTo, size_t numBytesInPage) {

    blockPtr = nullptr;
    blockPtr = std :: make_shared <UseTemporaryAllocationBlock>(pageToWriteTo, numBytesInPage);
    outputData = makeObject<Vector<Handle<OutputClass>>> ();
    pos = 0;

}

template <class OutputClass, class KeyType, class ValueType>
bool AggOutProcessor <OutputClass, KeyType, ValueType> :: fillNextOutputPage () {

    // if we are finalized, see if there are some left over records
    if (finalized) {
        getRecord (outputData);
        return false;
    }

    // we are not finalized, so process the page
    try {
        //see if there are any more items in current map to iterate over
        while (true) {

            if (!((*begin) != (*end))) {
                    PDB_COUT << "AggOutProcessor: processed " << pos << " elements in the input page" << std :: endl;
                    return false;
            }
          
            Handle<OutputClass> temp = makeObject<OutputClass> ();
            outputData->push_back(temp);
            (*outputData)[pos]->getKey() = (*(*begin)).key;
            (*outputData)[pos]->getValue() = (*(*begin)).value;
            pos++;
            ++(*begin);
        }

    } catch (NotEnoughSpace &n) {
        getRecord (outputData);
        return true;
    }
            

}

template <class OutputClass, class KeyType, class ValueType>
void AggOutProcessor <OutputClass, KeyType, ValueType> :: finalize () {
    finalized = true;
}

template <class OutputClass, class KeyType, class ValueType>
void AggOutProcessor <OutputClass, KeyType, ValueType> :: clearOutputPage () {
    blockPtr = nullptr;
    outputData = nullptr;
}

template <class OutputClass, class KeyType, class ValueType>
void AggOutProcessor <OutputClass, KeyType, ValueType> :: clearInputPage () {
    inputData = nullptr;
}




}


#endif
