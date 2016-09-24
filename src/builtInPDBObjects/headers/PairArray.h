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

#ifndef PAIR_ARRAY_H
#define PAIR_ARRAY_H

#include <cstddef>
#include <iostream>
#include <iterator>
#include <cstring>

// PRELOAD %PairArray <Nothing>%

namespace pdb {

// The Array type is the one type that we allow to be variable length.  This is accomplished
// the the data[] array at the end of the class.  When an Array object is alllocated, it is
// always allocated with some extra space at the end of the class to allow this array to
// be used.
//
// Since the array class can be variable length, it is used as the key building block for
// both the Vector and String classes.

template <class KeyType, class ValueType = Nothing> 
class PairArray : public Object {

public:

	// constructor/sdestructor
	PairArray ();
	PairArray (uint32_t numSlots);
	PairArray (const PairArray &copyFromMe);
	~PairArray ();

	// normally these would be defined by the ENABLE_DEEP_COPY macro, but because
	// PairArray is quite special, we need to manually override these methods
	void setUpAndCopyFrom (void *target, void *source) const;
	void deleteObject (void *deleteMe);
	size_t getSize (void *forMe);

private:

	// and this gives us our info about TypeContained
	PDBTemplateBase keyTypeInfo;
	PDBTemplateBase valueTypeInfo;

	// the size of the records we need to store
	uint32_t objSize;

	// the offset to where the value is in the records that we store
	uint32_t valueOffset;

	// the number of slots actually used
	uint32_t usedSlots;

	// the number of slots
	uint32_t numSlots;

	// the max number of slots before doubling
	uint32_t maxSlots;

	// the array of data
	Nothing data[0];

public:

	// create a new PairArray via doubling
	Handle <PairArray <KeyType, ValueType>> doubleArray ();

	// access the value at which; if this is undefined, define it and return a reference
	// to a newly-creaated value
	ValueType &operator [] (KeyType &which);

	// returns true if this has hit its max fill factor
	bool isOverFull ();

	// returns the number of items in this PairArray
	uint32_t numUsedSlots ();

	// returns 0 if this entry is undefined; 1 if it is defined
	int count (KeyType &which);
};

}

#include "PairArray.cc"

#endif

