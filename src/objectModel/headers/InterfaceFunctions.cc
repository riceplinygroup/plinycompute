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
#include "TypeName.h"
#include "VTableMap.h"
#include "Holder.h"

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
	//getAllocator ().setupBlock (malloc (numBytesIn), numBytesIn, throwExceptionOnFail);
        //add malloc results check by Jia
        void * space = malloc(numBytesIn);
        if (space == nullptr) {
             std :: cout << "Fatal Error in makeObjectAllocatorBlock(): out of memory" << std :: endl;
             exit (-1);
        }
        getAllocator ().setupBlock (space, numBytesIn, throwExceptionOnFail);
}

inline void makeObjectAllocatorBlock (void *spaceToUse, size_t numBytesIn, bool throwExceptionOnFail) {
	getAllocator ().setupUserSuppliedBlock (spaceToUse, numBytesIn, throwExceptionOnFail);
}

inline size_t getBytesAvailableInCurrentAllocatorBlock () {
	return getAllocator ().getBytesAvailableInCurrentAllocatorBlock ();
}

inline unsigned getNumObjectsInAllocatorBlock (void *forMe) {
	return getAllocator ().getNumObjectsInAllocatorBlock (forMe);
}

inline unsigned getNumObjectsInCurrentAllocatorBlock () {
	return getAllocator ().getNumObjectsInCurrentAllocatorBlock ();
}

template <class ObjType>
unsigned getNumObjectsInHomeAllocatorBlock (Handle <ObjType> &forMe) {
	return getAllocator ().getNumObjectsInHomeAllocatorBlock (forMe);
}

template <class ObjType> 
RefCountedObject <ObjType> *getHandle (ObjType &forMe) {

	// first we see if the object is located in the current allocation block
	if (getAllocator ().contains (&forMe)) {

		// it is, so we can just return the RefCountedObject directly
		return (RefCountedObject <ObjType> *) (
			CHAR_PTR (&forMe) - REF_COUNT_PREAMBLE_SIZE);

	// otherwise, we need to do a deep copy to the current allocation block
	} else {

		// get the space... allocate and set up the reference count before it
		PDBTemplateBase temp;
		temp.template setup <ObjType> ();
                #ifdef DEBUG_OBJECT_MODEL
		void *space = getAllocator ().getRAM (temp.getSizeOfConstituentObject (&forMe) + REF_COUNT_PREAMBLE_SIZE, temp.getTypeCode());
                #else
                void *space = getAllocator ().getRAM (temp.getSizeOfConstituentObject (&forMe) + REF_COUNT_PREAMBLE_SIZE);
                #endif
		// see if there was not enough RAM
		if (space == nullptr) {
			throw myException;
		}

		// allocate the new space
		RefCountedObject <ObjType> *returnVal = (RefCountedObject <ObjType> *) space;

		// set the ref count to zero
		returnVal->setRefCount (0);
                try {
		   setUpAndCopyFromTemplate <ObjType> (returnVal->getObject (), &forMe, nullptr);
                } catch (NotEnoughSpace &n) {
                #ifdef DEBUG_OBJECT_MODEL
                   getAllocator().freeRAM(returnVal, temp.getTypeCode());
                #else
                   getAllocator().freeRAM(returnVal);
                #endif
                   throw n;
                }
		// and get outta here
		return returnVal;
	}
}

template <class ObjType, class... Args>
RefCountedObject <ObjType> *makeObject (Args&&... args) {
#ifdef DEBUG_OBJECT_MODEL
        PDBTemplateBase temp;
        temp.template setup <ObjType> ();
#endif
	// create a new object
	RefCountedObject <ObjType> *returnVal = (RefCountedObject <ObjType> *)
#ifdef DEBUG_OBJECT_MODEL 
		(getAllocator ().getRAM (sizeof (ObjType) + REF_COUNT_PREAMBLE_SIZE, temp.getTypeCode()));
#else
                (getAllocator ().getRAM (sizeof (ObjType) + REF_COUNT_PREAMBLE_SIZE));
#endif

	// if we got a nullptr, get outta there
	if (returnVal == nullptr)
		return nullptr;

	// call the placement new
        try {
	        new ((void *) returnVal->getObject ()) ObjType (args... );
        } catch (NotEnoughSpace &n) {
                //for reference counting correctness
                //returnVal->setRefCount(0);
                //Handle<ObjType> temp = returnVal;
#ifdef DEBUG_OBJECT_MODEL
                getAllocator().freeRAM(returnVal, temp.getTypeCode());
#else
                getAllocator().freeRAM(returnVal);
#endif
                throw n;
        }

	// set the reference count
	returnVal->setRefCount (0);

	// and return it
	return returnVal;
}

template <class ObjType, class... Args>
RefCountedObject <ObjType> *makeObjectWithExtraStorage (size_t extra, Args&&... args) {
#ifdef DEBUG_OBJECT_MODEL
        PDBTemplateBase temp;
        temp.template setup <ObjType> ();
#endif
	// create a new object
	RefCountedObject <ObjType> *returnVal = (RefCountedObject <ObjType> *) 
#ifdef DEBUG_OBJECT_MODEL
		(getAllocator ().getRAM (extra + sizeof (ObjType) + REF_COUNT_PREAMBLE_SIZE, temp.getTypeCode()));
#else
                (getAllocator ().getRAM (extra + sizeof (ObjType) + REF_COUNT_PREAMBLE_SIZE));
#endif
	// if we got a nullptr, get outta there
	if (returnVal == nullptr)
		return nullptr;

	// call the placement new
        try {
	    new ((void *) returnVal->getObject ()) ObjType (args... );
        } catch (NotEnoughSpace &n) {
             //added by Jia based on Chris' proposal
             //for reference counting correctness
             //returnVal->setRefCount(0);
             //Handle<ObjType> temp = returnVal;
#ifdef DEBUG_OBJECT_MODEL
             getAllocator().freeRAM(returnVal, temp.getTypeCode());
#else
             getAllocator().freeRAM(returnVal);
#endif
             throw n;
        }

	// set the reference count
	returnVal->setRefCount (0);

	// and return it
	return returnVal;
}

template <class ObjType> 
Record <ObjType> *getRecord (Handle <ObjType> &forMe) {

	// get a pointer to the allocation block for this guy
        //std :: cout << "to get record..." << std :: endl;
	void *res = getAllocator ().getAllocationBlock (forMe);
        //std :: cout << "record size=" << (size_t)(*(size_t *)res) << std :: endl;			
	// and return that
	return (Record <ObjType> *) res;
}

template <class OutObjType, class InObjType>
Handle <OutObjType> unsafeCast (Handle <InObjType> &castMe) {
	
	Handle <OutObjType> result;
	result.offset = castMe.offset + (CHAR_PTR (&castMe) - CHAR_PTR (&result));
	result.typeInfo = castMe.typeInfo;
	result.getTarget ()->incRefCount ();
	return result;
}

template <class ObjType>
Record <ObjType> * getRecord (Handle <ObjType> &forMe, void *putMeHere, size_t numBytesAvailable) {

	// temporarily use the memory given by the caller
	AllocatorState tempAllocator = getAllocator ().temporarilyUseBlockForAllocations (putMeHere, numBytesAvailable);

	Handle <ObjType> temp;
	try {
		// copy this guy over
		temp = forMe.copyTargetToCurrentAllocationBlock ();

	// if we get an exception, then restore the allocation block and throw the exception ourselves
	} catch (NotEnoughSpace &n) {
		
		getAllocator ().restoreAllocationBlock (tempAllocator);
		throw n;
	}

	// get a pointer to the allocation block for this guy
	void *res = getAllocator ().getAllocationBlock (temp);
			
	// put the old allocation block back
	getAllocator ().restoreAllocationBlock (tempAllocator);	

	// and return that pointer
	return (Record <ObjType> *) res;
}

//added by Jia based on Chris' proposal
template <class TargetType> 
Handle <TargetType> deepCopyToCurrentAllocationBlock (Handle <TargetType> &copyMe) {
  //std :: cout << "create a handle for the Holder object" << std :: endl;
  Handle <Holder <TargetType>> temp = makeObject <Holder <TargetType>> ();
  //std :: cout << "to do a deep copy" << std :: endl;
  temp->child = copyMe; // since temp->child is physically located in the current allocation block, forces a deep copy
  //std :: cout << "deep copy done" << std :: endl;
  return temp->child;
};


}

#endif
