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

#ifndef ARRAY_CC
#define ARRAY_CC

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <type_traits>
#include <cstring>

#include "Handle.h"
#include "Object.h"
#include "InterfaceFunctions.h"

// Note: we need to write all operations in constructors, destructors, and assignment operators WITHOUT using
// the underlying type in any way (including assignment, initialization, destruction, size).  
//
namespace pdb {

template <class TypeContained>
void Array <TypeContained> :: setUpAndCopyFrom (void *target, void *source) const {

	new (target) Array <TypeContained> ();
	Array <TypeContained> &fromMe = *((Array <TypeContained> *) source);
	Array <TypeContained> &toMe = *((Array <TypeContained> *) target);

	// copy the number of slots
	toMe.numSlots = fromMe.numSlots;
	toMe.usedSlots = fromMe.usedSlots;

	// copy the type info
	toMe.typeInfo = fromMe.typeInfo;
        if (toMe.typeInfo.getTypeCode() == 0) {
            std :: cout << "Array::setUpAndCopyFrom: typeInfo=0 before getSizeOfConsitituentObject" << std :: endl;
        }
        // copy everything over... use memmove on primitive type, or properly do the assignment on everything else
        char *newLoc = (char *) toMe.data;
	char *data = (char *) fromMe.data;

	// get the type code for our child from the RHS
	// get the size of the constituent object
	uint32_t dataSize = 0;
	if (fromMe.usedSlots > 0)
#ifdef DEBUG_DEEP_COPY
                PDB_COUT << "fromMe.usedSlots=" << fromMe.usedSlots << std::endl;
#endif                
		dataSize = toMe.typeInfo.getSizeOfConstituentObject (data);

#ifdef DEBUG_DEEP_COPY
                PDB_COUT << "dataSize=" << dataSize << std::endl;
#endif

	if (!toMe.typeInfo.descendsFromObject ()) {
		// in this case, we might have a fundmanetal type... regardless, just do a simple bitwise copy
		memmove (newLoc, data, dataSize * fromMe.usedSlots);
	} else {
		for (uint32_t i = 0; i < fromMe.usedSlots; i++) {
			toMe.typeInfo.setUpAndCopyFromConstituentObject (newLoc + dataSize * i,  data + dataSize * i);
		}
	}
}

// just use setUpAndCopyFrom to do a deep copy... this assumes that we have enough space!!
template <class TypeContained>
Array <TypeContained> :: Array (const Array &toMe) {
	setUpAndCopyFrom (this, &toMe);
}

template <class TypeContained>
Array <TypeContained> :: Array (uint32_t numSlotsIn, uint32_t numUsedSlots) {
	typeInfo.setup <TypeContained> ();
	usedSlots = numUsedSlots;
	numSlots = numSlotsIn;
	if (typeInfo.descendsFromObject ()) {
		// run the constructor on each object
		for (uint32_t i = 0; i < numUsedSlots; i++) {
			new ((void *) &(data[i])) TypeContained ();
		}
	} else {
		// zero out everything
		bzero (data, numUsedSlots * sizeof (TypeContained));
	}
}

template <class TypeContained>
Array <TypeContained> :: Array (uint32_t numSlotsIn) {
	typeInfo.setup <TypeContained> ();
	usedSlots = 0;
	numSlots = numSlotsIn;
}

template <class TypeContained>
Handle <Array <TypeContained>> Array <TypeContained> :: doubleSize () {
	return resize (numSlots * 2);
}

template <class TypeContained>
bool Array <TypeContained> :: isFull () {
	typeInfo.setup <TypeContained> ();
	return usedSlots == numSlots;
}

template <class TypeContained>
Array <TypeContained> :: Array () {
	typeInfo.setup <TypeContained> ();
	usedSlots = 0;
	numSlots = 0;
}

// Note: because this can be called by Object.deleteObject (), it must be written so as to not use TypeContained
template <class TypeContained>
Array <TypeContained> :: ~Array () {
        if(typeInfo.getTypeCode() == 0) {
            std :: cout << "Array::~Array: typeInfo = 0 before getSizeOfConstituentObject" << std :: endl;
        }
	// do no work if the guys we store do not come from pdb :: Object
	if (!typeInfo.descendsFromObject ()) 
		return;

	// loop through and explicitly call the destructor on everything
	char *location = (char *) data;

	// get the size of the constituent object
	uint32_t objSize = 0;
	if (usedSlots > 0)
		objSize = typeInfo.getSizeOfConstituentObject (location);

	// now, delete each of the objects in there, if we have got an object type
	for (uint32_t i = 0; i < usedSlots; i++) {
		typeInfo.deleteConstituentObject (location + i * objSize);
	}
}

template <class TypeContained>
Handle <Array <TypeContained>> Array <TypeContained> :: resize (uint32_t howMany) {

	// allocate the new Array
	Handle <Array <TypeContained>> tempArray = 
		makeObjectWithExtraStorage <Array <TypeContained>> (sizeof (TypeContained) * howMany, howMany);

	// copy everything over
	TypeContained *newLoc = (TypeContained *) (tempArray->data);

	uint32_t max = usedSlots;
	if (max < howMany)
		max = howMany;

	uint32_t min = usedSlots;
	if (min > howMany)
		min = howMany;

	tempArray->usedSlots = min;

	for (uint32_t i = 0; i < min || i < usedSlots; i++) {

		if (i < min) {
			new ((void *) &(newLoc[i])) TypeContained ();
			newLoc[i] = ((TypeContained *) (data))[i];
		} else if (i < usedSlots) {
			((TypeContained *) (data))[i].~TypeContained ();
		}
	}

	// empty out this guy
	return tempArray;
}

template <class TypeContained>
TypeContained &Array <TypeContained> :: getObj (uint32_t which) {
	return ((TypeContained *) (data))[which];
}

template <class TypeContained>
void Array <TypeContained> :: assign (uint32_t which, const TypeContained &val) {
	if (which > usedSlots) {
		std :: cerr << "Bad: you are writing past the end of the vector!\n";
		return;
	}
	((TypeContained *) (data))[which] = val;
}	

template <class TypeContained>
void Array <TypeContained> :: push_back (const TypeContained &val) {

	// need a placement new to correctly initialize before the copy
	new ((void *) &(((TypeContained *) (data))[usedSlots])) TypeContained ();
	((TypeContained *) (data))[usedSlots] = val;
	usedSlots++;
}

template <class TypeContained>
TypeContained *Array <TypeContained> :: c_ptr () {
	return ((TypeContained *) (data));
}

template <class TypeContained>
void Array <TypeContained> :: pop_back () {
	if (usedSlots != 0) {
		((TypeContained *) (data))[usedSlots - 1].~TypeContained ();
		usedSlots--;
	}
}

template <class TypeContained>
uint32_t Array <TypeContained> :: numUsedSlots () {   
        return usedSlots;                                
}                                                                

template <class TypeContained>
void Array <TypeContained> :: setUsed (uint32_t toMe) {   
        usedSlots = toMe;
}                                                                

template <class TypeContained>
void Array <TypeContained> :: deleteObject (void *deleteMe) {   
        deleter (deleteMe, this);                                
}                                                                
                                                                 
template <class TypeContained>
size_t Array <TypeContained> :: getSize (void *forMe) {                                              
        if(((Array<TypeContained> *)forMe)->typeInfo.getTypeCode() == 0){
            std :: cout << "Array::getSize: typeInfo = 0 before getSizeOfConstituent" << std :: endl;
        }
	return sizeof (Array <TypeContained>) + 
		((Array <TypeContained> *) forMe)->typeInfo.getSizeOfConstituentObject (forMe) * 
		((Array <TypeContained> *) forMe)->numSlots;
}

}

#endif
