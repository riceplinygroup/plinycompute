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
#ifndef COMBINER_PROCESSOR_CC
#define COMBINER_PROCESSOR_CC

#include "CombinerProcessor.h"

namespace pdb {


template <class KeyType, class ValueType>
CombinerProcessor <KeyType, ValueType> :: CombinerProcessor (int numClusterPartitions, int numNodePartitions, Vector<HashPartitionID> nodePartitionIds) {

    this->numClusterPartitions = numClusterPartitions;
    this->numNodePartitions = numNodePartitions;
    finalized = false;

    int i;
    for (i = 0; i < nodePartitionIds.size(); i ++) {
        nodePartitionIds.push_back(nodePartitionIds[i]);
    }
}

//initialize
template <class KeyType, class ValueType>
void CombinerProcessor <KeyType, ValueType> :: initialize () {
    finalized = false;
}

//loads up another input page to process
template <class KeyType, class ValueType>
void CombinerProcessor <KeyType, ValueType> :: loadInputPage (void * pageToProcess) {

    Record <Vector<Handle<Map<KeyType, ValueType>>>> * myRec = (Record <Vector<Handle<Map<KeyType, ValueType>>>> *) pageToProcess;
    inputData = myRec->getRootObject();
    curPartPos = 0;
    curPartId = nodePartitionIds[curPartPos];
    curMap = (*inputData)[curPartId];
    begin = new PDBMapIterator <KeyType, ValueType>(curMap->getArray(), true);
    end = new PDBMapIterator <KeyType, ValueType>(curMap->getArray());
}

//loads up another output page to write results to
template <class KeyType, class ValueType>
void CombinerProcessor <KeyType, ValueType> :: loadOutputPage (void * pageToWriteTo, size_t numBytesInPage) {

    blockPtr = nullptr;
    blockPtr = std :: make_shared <UseTemporaryAllocationBlock>(pageToWriteTo, numBytesInPage);
    outputData = makeObject<Vector<Handle<Map<KeyType, ValueType>>>> (this->numNodePartitions);
    int i;
    for ( i = 0; i < numNodePartitions; i ++ ) {
        Handle<Map<KeyType, ValueType>> currentMap =  makeObject<Map<KeyType, ValueType>>();
        currentMap->setHashPartitionId (i);
        outputData->push_back(currentMap);
    }
    curOutputMap = (*outputData)[curPartPos];

}

template <class KeyType, class ValueType>
bool CombinerProcessor <KeyType, ValueType> :: fillNextOutputPage () {

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
                if (curPartPos < nodePartitionIds.size()-1) {
                    curPartPos ++;
                    curPartId = nodePartitionIds[curPartPos];
                    curMap = (*inputData)[curPartId];
                    begin = new PDBMapIterator <KeyType, ValueType>(curMap->getArray(), true);
                    end = new PDBMapIterator <KeyType, ValueType> (curMap->getArray());
                    curOutputMap = (*outputData)[curPartPos];
                } else {
                    return false;
                }
            }
            KeyType curKey = (*(*begin)).key;
            ValueType curValue = (*(*begin)).value;
            // if the key is not there
            if (curOutputMap->count (curKey) == 0) {
                
                ValueType * temp = nullptr;
                temp = &((*curOutputMap)[curKey]);
                try {

                    *temp = curValue;
                    ++(*begin);

                // if we couldn't fit the value
                } catch (NotEnoughSpace &n) {
                    curOutputMap->setUnused (curKey);
                    throw n;
                }
            // the key is there
            } else {

                //get the value and copy of it
                ValueType &temp = (*curOutputMap)[curKey];
                ValueType copy = temp;

                //and add to old value, producing a new one
                try {

                    temp = copy + curValue;
                    ++(*begin);

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
void CombinerProcessor <KeyType, ValueType> :: finalize () {
    finalized = true;
}

template <class KeyType, class ValueType>
void CombinerProcessor <KeyType, ValueType> :: clearOutputPage () {
    blockPtr = nullptr;
    outputData = nullptr;
}

template <class KeyType, class ValueType>
void CombinerProcessor <KeyType, ValueType> :: clearInputPage () {
    inputData = nullptr;
}




}


#endif
