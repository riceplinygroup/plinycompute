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

#ifndef JOIN_MAP_H
#define JOIN_MAP_H

// PRELOAD %JoinMap <Nothing>%

#include "Object.h"
#include "Handle.h"
#include "JoinPairArray.h"

template <typename StoredType>
class JoinRecordLst;

namespace pdb {

// This is the Map type used to power joins 

template <class ValueType>
class JoinMap : public Object {

private:

	// this is where the data are actually stored
	Handle <JoinPairArray <ValueType>> myArray;

public:

	ENABLE_DEEP_COPY

	// this constructor pre-allocates initSize slots... initSize must be a power of two
	JoinMap (uint32_t initSize);

	// this constructor creates a map with a single slot
	JoinMap ();

	// destructor
	~JoinMap ();

	// allows us to access all of the records with a particular hash value
	JoinRecordList <ValueType> lookup (const size_t &which);

        // adds a new value at position which
        ValueType &push (const size_t &which);

        // clears the particular key from the map, destructing both the key and the value.  
        // This is typically used when an out-of-memory
        // exception is thrown when we try to add to the hash table, and we want to immediately clear
        // the last item added.
        void setUnused (const size_t &clearMe);

	// returns the number of elements in the map
	size_t size() const;
	
	// returns 0 if this entry is undefined; 1 if it is defined
	int count (const size_t &which);

        // these are used for iteration
        JoinMapIterator <ValueType> begin ();
        JoinMapIterator <ValueType> end ();

};

}

#include "JoinMap.cc"

#endif

