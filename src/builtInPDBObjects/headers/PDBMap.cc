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

#ifndef MAP_CC
#define MAP_CC

#include "InterfaceFunctions.h"
#include "PDBMap.h"

namespace pdb {

// does a bunch of bit twiddling to compute a hash of an int
inline unsigned int newHash(unsigned int x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

// computes a hash over an input character array
inline size_t hashMe(char* me, size_t len) {
    size_t code = 0;
#ifndef HASH_FOR_TPCH
    for (int i = 0; i < len; i += 8) {

        int low = 0;
        int high = 0;

        for (int j = 0; j < 4; j++) {
            if (i + j < len) {
                low += (me[i + j] & 0xFF) << (8 * j);
            } else {
                low += 123 << (8 * j);
            }
        }

        if (len <= 4)
            high = low;
        else {
            for (int j = 4; j < 8; j++) {
                if (i + j < len) {
                    high += (me[i + j] & 0xFF) << (8 * (j - 3));
                } else {
                    high += 97 << (8 * (j - 3));
                }
            }
        }

        size_t returnVal = ((size_t)newHash(high)) << 32;
        returnVal += newHash(low) & 0xFFFFFFFF;
        code = code ^ returnVal;
    }
#else
    for (int i = 0; i < len; i++) {
        code = 31 * code + me[i];
    }

#endif
    return code;
}

template <class KeyType, class ValueType>
Map<KeyType, ValueType>::Map(uint32_t initSize) {

    if (initSize < 2) {
        std::cout << "Fatal Error: Map initialization:" << initSize
                  << " too small; must be at least one.\n";

        initSize = 2;
    }

    // this way, we'll allocate extra bytes on the end of the array
    MapRecordClass<KeyType, ValueType> temp;
    size_t size = temp.getObjSize();
    myArray = makeObjectWithExtraStorage<PairArray<KeyType, ValueType>>(size * initSize, initSize);
}

template <class KeyType, class ValueType>
Map<KeyType, ValueType>::Map() {

    MapRecordClass<KeyType, ValueType> temp;
    size_t size = temp.getObjSize();
    myArray = makeObjectWithExtraStorage<PairArray<KeyType, ValueType>>(size * 2, 2);
}

template <class KeyType, class ValueType>
Map<KeyType, ValueType>::~Map() {}


template <class KeyType, class ValueType>
void Map<KeyType, ValueType>::setUnused(const KeyType& clearMe) {
    myArray->setUnused(clearMe);
}


template <class KeyType, class ValueType>
ValueType& Map<KeyType, ValueType>::operator[](const KeyType& which) {

    // JiaNote: each time we increase size only when key doesn't exist.
    // so that we can make sure usedSlot < maxSlots each time before we invoke[] for insertion
    // and for read-only data, we will not invoke doubleArray(), if we always invoke count() before
    // invoke []
    if (myArray->count(which) == 0) {
        if (myArray->isOverFull()) {
            Handle<PairArray<KeyType, ValueType>> temp = myArray->doubleArray();
            myArray = temp;
        }
    }
    ValueType& res = (*myArray)[which];
    return res;
}

template <class KeyType, class ValueType>
int Map<KeyType, ValueType>::count(const KeyType& which) {
    return myArray->count(which);
}

template <class KeyType, class ValueType>
size_t Map<KeyType, ValueType>::size() const {
    return myArray->numUsedSlots();
}

template <class KeyType, class ValueType>
PDBMapIterator<KeyType, ValueType> Map<KeyType, ValueType>::begin() {
    PDBMapIterator<KeyType, ValueType> returnVal(myArray, true);
    return returnVal;
}

template <class KeyType, class ValueType>
PDBMapIterator<KeyType, ValueType> Map<KeyType, ValueType>::end() {
    PDBMapIterator<KeyType, ValueType> returnVal(myArray);
    return returnVal;
}


template <class KeyType, class ValueType>
Handle<PairArray<KeyType, ValueType>>& Map<KeyType, ValueType>::getArray() {
    return myArray;
}
}

#endif
