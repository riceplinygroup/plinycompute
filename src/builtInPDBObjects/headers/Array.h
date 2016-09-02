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

#ifndef ARRAY_H
#define ARRAY_H

#include <cstddef>
#include <iostream>
#include <iterator>
#include <cstring>

// PRELOAD %Array <Nothing>%

namespace pdb {


// The Array type is the one type that we allow to be variable length.  This is accomplished
// the the data[] array at the end of the class.  When an Array object is alllocated, it is
// always allocated with some extra space at the end of the class to allow this array to
// be used.
//
// Since the array class can be variable length, it is used as the key building block for
// both the Vector and String classes.

template <class TypeContained> 
class Array : public Object {

public:

	// constructor/sdestructor
	Array ();
	Array (uint32_t numSlots);
	Array (const Array &copyFromMe);
	~Array ();

	// this constructor pre-allocates the given number of slots, and initializes the specified number,
	// so that the number of used slots is equal to the secod parameter
	Array (uint32_t numSlots, uint32_t numUsedSlots);

	// normally these would be defined by the ENABLE_DEEP_COPY macro, but because
	// Array is the one variable-sized type that we allow, we need to manually override 
	// these methods
	void setUpAndCopyFrom (void *target, void *source) const;
	void deleteObject (void *deleteMe);
	size_t getSize (void *forMe);

private:

	// and this gives us our info about TypeContained
	PDBTemplateBase typeInfo;

	// the number of slots actually used
	uint32_t usedSlots;

	// the number of slots
	uint32_t numSlots;

	// the array of data
	Nothing data[0];

public:

	// create a new Array object of size howMany, and copy our contents into it
	Handle <Array <TypeContained>> resize (uint32_t howMany);

	// get a pointer to the data
	TypeContained *c_ptr ();

	// access a particular object in the array, by index
	TypeContained &getObj (uint32_t which);

	// assign a particular item in the array
	void assign (uint32_t which, const TypeContained &val);

	// add to the end
	void push_back (const TypeContained &val);

	// remove from the end and shrink the number of used slots
	void pop_back ();

	// return a new array that is twice the size, containing the same contents 
	Handle <Array <TypeContained>> doubleSize ();

	// check if the array is full
	bool isFull ();

	// return/set the number of used slots
	uint32_t numUsedSlots ();
	void setUsed (uint32_t toMe);

	// beause the communicator needs to see inside to do efficient sends
	friend class PDBCommunicator;
};

}

#include "Array.cc"

#endif

