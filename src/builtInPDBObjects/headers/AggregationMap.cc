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
#ifndef AGGREGATION_MAP_CC
#define AGGREGATION_MAP_CC

#include "AggregationMap.h"

namespace pdb {

//wraps the container for shuffle data for Aggregation

template <class KeyType, class ValueType>
AggregationMap<KeyType, ValueType>::AggregationMap(uint32_t initSize) {

    if (initSize < 2) {
        std::cout << "Fatal Error: Map initialization:" << initSize
                  << " too small; must be at least one.\n";

        initSize = 2;
    }

    // this way, we'll allocate extra bytes on the end of the array
    MapRecordClass<KeyType, ValueType> temp;
    size_t size = temp.getObjSize();
    this->myArray =
        makeObjectWithExtraStorage<PairArray<KeyType, ValueType>>(size * initSize, initSize);
}

template <class KeyType, class ValueType>
AggregationMap<KeyType, ValueType>::AggregationMap() {

    MapRecordClass<KeyType, ValueType> temp;
    size_t size = temp.getObjSize();
    this->myArray = makeObjectWithExtraStorage<PairArray<KeyType, ValueType>>(size * 2, 2);
}

template <class KeyType, class ValueType>
AggregationMap<KeyType, ValueType>::~AggregationMap() {}


template <class KeyType, class ValueType>
unsigned int AggregationMap<KeyType, ValueType>::getHashPartitionId() {
    return this->hashPartitionId;
}

template <class KeyType, class ValueType>
void AggregationMap<KeyType, ValueType>::setHashPartitionId(unsigned int partitionId) {
    this->hashPartitionId = partitionId;
}
}

#endif
