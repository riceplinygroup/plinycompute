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

#ifndef MAP_H
#define MAP_H

// PRELOAD %Map <Nothing>%

#include "Object.h"
#include "Handle.h"
#include "PairArray.h"

namespace pdb {

// This is the basic Map type that works correcrly with Objects and Handles.

template <class KeyType, class ValueType = Nothing>
class Map : public Object {

protected:
    // this is where the data are actually stored
    Handle<PairArray<KeyType, ValueType>> myArray;

public:
    ENABLE_DEEP_COPY

    // this constructor pre-allocates initSize slots... initSize must be a power of two
    Map(uint32_t initSize);

    // this constructor creates a map with a single slot
    Map();

    // destructor
    ~Map();

    // access the value at "which"; if this is undefined, define it and return a reference
    ValueType& operator[](const KeyType& which);

    // clears the particular key from the map, destructing both the key and the value.  NOTE THAT
    // THIS IS ONLY SAFE TO USE IF CLEARME WAS THE VERY LAST ITEM ADDED TO THE MAP.  If it is not,
    // the hash table may be in an inconsistent state.  This is typically used when an out-of-memory
    // exception is thrown when we try to add to the hash table, and we want to immediately clear
    // the last item added.
    void setUnused(const KeyType& clearMe);

    // returns the number of elements in the map
    size_t size() const;

    // returns 0 if this entry is undefined; 1 if it is defined
    int count(const KeyType& which);

    // these are used for iteration
    PDBMapIterator<KeyType, ValueType> begin();
    PDBMapIterator<KeyType, ValueType> end();

    Handle<PairArray<KeyType, ValueType>>& getArray();
};
}

#include "PDBMap.cc"

#endif
