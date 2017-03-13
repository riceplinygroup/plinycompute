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
#ifndef AGGREGATION_PROCESSOR_CC
#define AGGREGATION_PROCESSOR_CC

#include "AggregationProcessor.h"

namespace pdb {


template <class KeyType, class ValueType>
AggregationProcessor <KeyType, ValueType> :: AggregationProcessor () {

    finalized = false;

}

//initialize
template <class KeyType, class ValueType>
void AggregationProcessor <KeyType, ValueType> :: initialize () {

}

//loads up another input page to process
template <class KeyType, class ValueType>
void AggregationProcessor <KeyType, ValueType> :: loadInputPage (void * pageToProcess) {

    Record <Vector<Handle<Map<KeyType, ValueType>>>> * myRec = (Record <Vector<Handle<Map<KeyType, ValueType>>>> *) pageToProcess;
    inputData = myRec->getRootObject();
    curVecPos = 0;
    curMap = (*inputData)[curVecPos];
    begin = curMap->begin();
    end = curMap->end();
}

//loads up another output page to write results to
template <class KeyType, class ValueType>
void AggregationProcessor <KeyType, ValueType> :: loadOutputPage (void * pageToWriteTo, size_t numBytesInPage) {

    blockPtr = nullptr;
    blockPtr = std :: make_shared <UseTemporaryAllocationBlock>(pageToWriteTo, numBytesInPage);
    outputData = makeObject<Map<KeyType, ValueType>> ();

}

template <class KeyType, class ValueType>
bool AggregationProcessor <KeyType, ValueType> :: fillNextOutputPage () {

    // if we are finalized, see if there are some left over records
    if (finalized) {
        getRecord (outputData);
        return false;
    }

    // we are not finalized, so process the page
    try {

        //see if there are any more items in current map to iterate over
        while (true) {

            if (begin == end) {
                if (curVecPos < inputData->size()-1) {
                    curVecPos ++;
                    curMap = (*inputData)[curVecPos];
                    begin = curMap->begin();
                    end = curMap->end();
                } else {
                    return nullptr;
                }
            }
            KeyType curKey = (*begin).key;
            ValueType curValue = (*begin).value;
            // if the key is not there
            if (outputData->count (curKey) == 0) {
                
                ValueType * temp = nullptr;
                temp = &((*outputData)[curKey]);
                try {

                    *temp = curValue;
                    ++begin;

                // if we couldn't fit the value
                } catch (NotEnoughSpace &n) {
                    outputData->setUnused (curKey);
                    throw n;
                }
            // the key is there
            } else {

                //get the value and copy of it
                ValueType &temp = outputData[curKey];
                ValueType copy = temp;

                //and add to old value, producing a new one
                try {

                    temp = copy + curValue;
                    ++begin;

                //if we got here, it means we run out of RAM and we need to restore the old value in the destination hash map
                } catch (NotEnoughSpace &n) {
                    temp = copy;
                    throw n;
                }
            }
        
        }

    } catch (NotEnoughSpace &n) {
        getRecord (outputData);
        return true;
    }
            

}

template <class KeyType, class ValueType>
void AggregationProcessor <KeyType, ValueType> :: finalize () {
    finalized = true;
}

template <class KeyType, class ValueType>
void AggregationProcessor <KeyType, ValueType> :: clearOutputPage () {
    blockPtr = nullptr;
    outputData = nullptr;
}

template <class KeyType, class ValueType>
void AggregationProcessor <KeyType, ValueType> :: clearInputPage () {
    inputData = nullptr;
}




}


#endif
