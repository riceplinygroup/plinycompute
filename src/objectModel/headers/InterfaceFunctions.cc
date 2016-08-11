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

#ifndef INTERFACE_FUNCTIONS_CC
#define INTERFACE_FUNCTIONS_CC

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>

#include "InterfaceFunctions.h"
#include "Handle.h"
#include "RefCountedObject.h"
#include "Record.h"
#include "DeepCopy.h"
#include "RefCountMacros.h"
#include "TypeName.cc"
#include "VTableMap.h"

namespace pdb {

template <class ObjType>
int16_t getTypeID () {
        static int16_t typeID = -1;

	// if we've never been here before, return the value we got last time
        if (typeID != -1)
                return typeID;

	// if we have a type not derived from Object, return -1	
        else if (!std::is_base_of <Object, ObjType> :: value) {
		return -1;
	
	// we have some other type name
	} else {
                typeID = VTableMap :: getIDByName (getTypeName <ObjType> ());
                return typeID;
        }
}

#define CHAR_PTR(c) ((char *) c)

inline void makeObjectAllocatorBlock (size_t numBytesIn, bool throwExceptionOnFail) {
	allocator.setupBlock (malloc (numBytesIn), numBytesIn, throwExceptionOnFail);
}

inline void makeObjectAllocatorBlock (void *spaceToUse, size_t numBytesIn, bool throwExceptionOnFail) {
	allocator.setupUserSuppliedBlock (spaceToUse, numBytesIn, throwExceptionOnFail);
}

inline size_t getBytesAvailableInCurrentAllocatorBlock () {
	return allocator.getBytesAvailableInCurrentAllocatorBlock ();
}

inline unsigned getNumObjectsInCurrentAllocatorBlock () {
	return allocator.getNumObjectsInCurrentAllocatorBlock ();
}

template <class ObjType>
unsigned getNumObjectsInHomeAllocatorBlock (Handle <ObjType> &forMe) {
	return allocator.getNumObjectsInHomeAllocatorBlock (forMe);
}

template <class ObjType> 
RefCountedObject <ObjType> *getHandle (ObjType &forMe) {

	// first we see if the object is located in the current allocation block
	if (allocator.contains (&forMe)) {

		// it is, so we can just return the RefCountedObject directly
		return (RefCountedObject <ObjType> *) (
			CHAR_PTR (&forMe) - REF_COUNT_PREAMBLE_SIZE);

	// otherwise, we need to do a deep copy to the current allocation block
	} else {

		// get the space... allocate and set up the reference count before it
		PDBTemplateBase temp;
		temp.template setup <ObjType> ();
		void *space = allocator.getRAM (temp.getSizeOfConstituentObject (&forMe) + REF_COUNT_PREAMBLE_SIZE);

		// see if there was not enough RAM
		if (space == nullptr) {
			return nullptr;
		}

		// allocate the new space
		RefCountedObject <ObjType> *returnVal = (RefCountedObject <ObjType> *) space;

		// set the ref count to zero
		returnVal->setRefCount (0);
		setUpAndCopyFromTemplate <ObjType> (returnVal->getObject (), &forMe, nullptr);

		// and get outta here
		return returnVal;
	}
}

template <class ObjType, class... Args>
RefCountedObject <ObjType> *makeObject (Args&&... args) {

	// create a new object
	RefCountedObject <ObjType> *returnVal = (RefCountedObject <ObjType> *) 
		(allocator.getRAM (sizeof (ObjType) + REF_COUNT_PREAMBLE_SIZE));

	// if we got a nullptr, get outta there
	if (returnVal == nullptr)
		return nullptr;

	// call the placement new
	new ((void *) returnVal->getObject ()) ObjType (args... );

	// set the reference count
	returnVal->setRefCount (0);

	// and return it
	return returnVal;
}

template <class ObjType, class... Args>
RefCountedObject <ObjType> *makeObjectWithExtraStorage (size_t extra, Args&&... args) {

	// create a new object
	RefCountedObject <ObjType> *returnVal = (RefCountedObject <ObjType> *) 
		(allocator.getRAM (extra + sizeof (ObjType) + REF_COUNT_PREAMBLE_SIZE));

	// if we got a nullptr, get outta there
	if (returnVal == nullptr)
		return nullptr;

	// call the placement new
	new ((void *) returnVal->getObject ()) ObjType (args... );

	// set the reference count
	returnVal->setRefCount (0);

	// and return it
	return returnVal;
}

template <class ObjType> 
Record <ObjType> *getRecord (Handle <ObjType> &forMe) {

	// get a pointer to the allocation block for this guy
	void *res = allocator.getAllocationBlock (forMe);
			
	// and return that
	return (Record <ObjType> *) res;
}

template <class ObjType>
Record <ObjType> * getRecord (Handle <ObjType> &forMe, void *putMeHere, size_t numBytesAvailable) {

	// temporarily use the memory given by the caller
	allocator.temporarilyUseBlockForAllocations (putMeHere, numBytesAvailable);

	// copy this guy over
	Handle <ObjType> temp = forMe.copyTargetToCurrentAllocationBlock ();

	// get a pointer to the allocation block for this guy
	void *res = allocator.getAllocationBlock (temp);
			
	// put the old allocation block back
	allocator.restoreAllocationBlock ();	

	// and return that pointer
	return (Record <ObjType> *) res;
}

}

#endif
