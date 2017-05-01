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

#include "Object.h"
#include "PDBTemplateBase.h"
#include "Handle.h"

#ifndef JOIN_PAIR_ARRAY_H
#define JOIN_PAIR_ARRAY_H

#include <cstddef>
#include <iostream>
#include <iterator>
#include <cstring>

// PRELOAD %JoinPairArray <Nothing>%

namespace pdb {

template <class ValueType> class JoinPairArray;

// this little class is used to ask the compiler to build the layout of the records used to
// store (hash, next, ValueType) triples
template <class ValueType>
struct JoinMapRecordClass {
        size_t hash;
        uint32_t next;
        ValueType value;

        size_t getObjSize () {
                return sizeof (JoinMapRecordClass <ValueType>);
        }
};

// this allows us to access all of the records with the same key as
// we iterate through the map
template <class ValueType>
class JoinRecordList {

private:

	// the slot in the pair array
	uint32_t whichOne;

	// the pair array
	JoinPairArray <ValueType> &parent;

public:

	// constructor
	JoinRecordList (uint32_t whichOne, JoinPairArray <ValueType> &parent);

	// returns the hash value that all of these guys share
	size_t getHash ();

	// returns the number of items that have this particular key
	size_t size ();
	
	// accesses the i^th item in the list
	ValueType &operator [] (const size_t i);
};

// this little class is used to support iteration over pdb :: Maps
template <class ValueType>
class JoinMapIterator {

public:

	bool operator != (const JoinMapIterator &me) const;
	JoinRecordList <ValueType> &operator * ();
	void operator ++ ();
	JoinMapIterator (Handle <JoinPairArray <ValueType>> iterateMeIn, bool);
	JoinMapIterator (Handle <JoinPairArray <ValueType>> iterateMeIn);

private:

	uint32_t slot;
	JoinPairArray <ValueType> &iterateMe;
	bool done;
	
};

template <class ValueType> 
class JoinPairArray : public Object {

public:

	// constructor/sdestructor
	JoinPairArray ();
	JoinPairArray (uint32_t numSlots);
	JoinPairArray (const JoinPairArray &copyFromMe);
	~JoinPairArray ();

	// normally these would be defined by the ENABLE_DEEP_COPY macro, but because
	// JoinPairArray is quite special, we need to manually override these methods
	void setUpAndCopyFrom (void *target, void *source) const;
	void deleteObject (void *deleteMe);
	size_t getSize (void *forMe);

private:

	PDBTemplateBase valueTypeInfo;

	// the number of slots actually used
	uint32_t usedSlots;

	// the number of slots
	uint32_t numSlots;

	// the max number of slots before doubling
	uint32_t maxSlots;

	// the size of the data
	uint32_t objSize;

	// this stores all of the repeated values (those with the same key)
	Vector <Vector <ValueType>> overflows;

	// the array of data
	JoinMapRecordClass <Nothing> data[0];

public:

	// create a new PairArray via doubling
	Handle <JoinPairArray <ValueType>> doubleArray ();

	// adds a new value at position which
	ValueType &push (const size_t &which);

	// allows us to access all of the records with a particular hash value
	JoinRecordList <ValueType> lookup (const size_t &which);
	
	// returns true if this has hit its max fill factor
	bool isOverFull ();

	// returns the number of items in this PairArray
	uint32_t numUsedSlots ();

	// returns 0 if this entry is undefined; 1 if it is defined
	int count (const size_t &which);

	// so this guy can look inside
	template <class ValueTwo> friend class JoinMapIterator;
	template <class ValueTwo> friend class JoinRecordList;

        // clear an item
        void setUnused (const size_t &clearMe);
};

}

#include "JoinPairArray.cc"

#endif

