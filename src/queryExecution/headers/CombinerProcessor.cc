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
#include "Employee.h"

namespace pdb {


template <class KeyType, class ValueType>
CombinerProcessor<KeyType, ValueType>::CombinerProcessor(std::vector<HashPartitionID>& partitions) {
    PDB_COUT << "running CombinerProcessor constructor" << std::endl;
    this->numNodePartitions = partitions.size();
    finalized = false;

    int i;
    for (i = 0; i < partitions.size(); i++) {
        PDB_COUT << i << ":" << partitions[i] << std::endl;
        nodePartitionIds.push_back(partitions[i]);
    }
    count = 0;
    curPartPos = 0;
    begin = nullptr;
    end = nullptr;
}

// initialize
template <class KeyType, class ValueType>
void CombinerProcessor<KeyType, ValueType>::initialize() {
    finalized = false;
}

// loads up another input page to process
template <class KeyType, class ValueType>
void CombinerProcessor<KeyType, ValueType>::loadInputPage(void* pageToProcess) {
    PDB_COUT << "CombinerProcessor: to load a new input page" << std::endl;
    Record<Vector<Handle<Map<KeyType, ValueType>>>>* myRec =
        (Record<Vector<Handle<Map<KeyType, ValueType>>>>*)pageToProcess;
    inputData = myRec->getRootObject();
    curPartPos = 0;
    count = 0;
    curPartId = nodePartitionIds[curPartPos];
    curMap = (*inputData)[curPartId];
    if (begin != nullptr) {
        delete begin;
    }
    if (end != nullptr) {
        delete end;
    }
    PDB_COUT << "CombinerProcessor: loaded a page with first partition id=" << curPartId
             << " and size=" << curMap->size() << std::endl;
    begin = new PDBMapIterator<KeyType, ValueType>(curMap->getArray(), true);
    end = new PDBMapIterator<KeyType, ValueType>(curMap->getArray());
}

// loads up another output page to write results to
template <class KeyType, class ValueType>
void CombinerProcessor<KeyType, ValueType>::loadOutputPage(void* pageToWriteTo,
                                                           size_t numBytesInPage) {

    blockPtr = nullptr;
    blockPtr = std::make_shared<UseTemporaryAllocationBlock>(pageToWriteTo, numBytesInPage);
    outputData =
        makeObject<Vector<Handle<AggregationMap<KeyType, ValueType>>>>(this->numNodePartitions);
    int i;
    for (i = 0; i < numNodePartitions; i++) {
        PDB_COUT << "to create the " << i << "-th partition on this node" << std::endl;
        Handle<AggregationMap<KeyType, ValueType>> currentMap =
            makeObject<AggregationMap<KeyType, ValueType>>();
        HashPartitionID currentPartitionId = nodePartitionIds[i];
        PDB_COUT << "currentPartitionId=" << currentPartitionId << std::endl;
        // however we only use the relative/local hash partition id
        currentMap->setHashPartitionId(i);
        outputData->push_back(currentMap);
    }
    // std :: cout << "curPartPos=" << curPartPos << std :: endl;
    curOutputMap = (*outputData)[curPartPos];
}

template <class KeyType, class ValueType>
bool CombinerProcessor<KeyType, ValueType>::fillNextOutputPage() {

    // if we are finalized, see if there are some left over records
    if (finalized) {
        for (int i = 0; i < numNodePartitions; i++) {
            PDB_COUT << "outputData[" << i << "].size()=" << (*outputData)[i]->size() << std::endl;
            PDB_COUT << "count=" << count << std::endl;
        }

        getRecord(outputData);
        return false;
    }

    // we are not finalized, so process the page
    try {

        // see if there are any more items in current map to iterate over
        while (true) {

            if (!((*begin) != (*end))) {
                // std :: cout << "CombinerProcess: processed a map partition in current input page
                // with curPartId=" << curPartId << ", and curPartPos=" << curPartPos << std ::
                // endl;
                if (curPartPos < numNodePartitions - 1) {
                    curPartPos++;
                    PDB_COUT << "curPartPos=" << curPartPos << std::endl;
                    curPartId = nodePartitionIds[curPartPos];
                    PDB_COUT << "curPartId=" << curPartId << std::endl;
                    curMap = (*inputData)[curPartId];
                    PDB_COUT << "(*inputData)[" << curPartId << "].size()=" << curMap->size()
                             << std::endl;
                    if (curMap->size() > 0) {
                        begin = new PDBMapIterator<KeyType, ValueType>(curMap->getArray(), true);
                        end = new PDBMapIterator<KeyType, ValueType>(curMap->getArray());

                        if ((*begin) != (*end)) {
                            // std :: cout << "Combiner processor: now we have a new output map with
                            // index in outputData being " << curPartPos << std :: endl;
                            curOutputMap = (*outputData)[curPartPos];
                        } else {
                            PDB_COUT << "this is strage: map size > 0 but begin == end"
                                     << std::endl;
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    // JiaNote: this is important, we need make sure when we load next page, we
                    // start output from the 0-th partition
                    curPartPos = 0;
                    curOutputMap = (*outputData)[0];
                    return false;
                }
            }
            KeyType curKey = (*(*begin)).key;
            ValueType curValue = (*(*begin)).value;
            // std :: cout << "combine the " << count << "-th element" << std :: endl;
            // if the key is not there
            if (curOutputMap->count(curKey) == 0) {

                ValueType* temp = nullptr;
                try {
                    temp = &((*curOutputMap)[curKey]);

                } catch (NotEnoughSpace& n) {

                    // std :: cout << "Info: Combiner page is too small, exception 1 thrown" << std
                    // :: endl;
                    throw n;
                }
                try {

                    *temp = curValue;
                    ++(*begin);
                    count++;
                    // if we couldn't fit the value
                } catch (NotEnoughSpace& n) {
                    // std :: cout << "Info: Combiner page is too small, exception 2 thrown" << std
                    // :: endl;
                    curOutputMap->setUnused(curKey);

                    throw n;
                }
                // the key is there
            } else {

                // get the value and copy of it
                ValueType& temp = (*curOutputMap)[curKey];
                ValueType copy = temp;

                // and add to old value, producing a new one
                try {

                    temp = copy + curValue;
                    ++(*begin);
                    count++;

                    // if we got here, it means we run out of RAM and we need to restore the old
                    // value in the destination hash map
                } catch (NotEnoughSpace& n) {
                    // std :: cout << "Info: Combiner page is too small, exception 3 thrown" << std
                    // :: endl;
                    temp = copy;
                    throw n;
                }
            }
        }

    } catch (NotEnoughSpace& n) {
        for (int i = 0; i < numNodePartitions; i++) {
            PDB_COUT << "outputData[" << i << "].size()=" << (*outputData)[i]->size() << std::endl;
        }
        getRecord(outputData);
        return true;
    }
}

template <class KeyType, class ValueType>
void CombinerProcessor<KeyType, ValueType>::finalize() {
    finalized = true;
}

template <class KeyType, class ValueType>
void CombinerProcessor<KeyType, ValueType>::clearOutputPage() {
    blockPtr = nullptr;
    outputData = nullptr;
    curOutputMap = nullptr;
}

template <class KeyType, class ValueType>
void CombinerProcessor<KeyType, ValueType>::clearInputPage() {
    inputData = nullptr;
}
}


#endif
