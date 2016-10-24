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

#ifndef HANDLE_H
#define HANDLE_H

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>

namespace pdb {

template <class ObjType> 
class RefCountedObject;

#define CHAR_PTR(c) ((char *) c)

// A Handle is a pointer-like object that helps with several different tasks:
//
// (1) It ensures that everything pointed to by the Handle lives in the 
//     same allocation block as the Handle (an allocation block is a 
//     contiguous range of memory)---if the Handle itself lives in an 
//     allocation block.  If an assignment could cause this requirement
//     to be allocated, then a deep copy is automatically performed.
//
//     For example, consider the following:
//
//     class Supervisor : public Object {
//
//     private:
//
//         int salary;
//         double weight;
//         Handle <Employee> myEmployee;
//
//     public:
//
//         Supervisor (int salIn, double wgtIn, Handle <Employee> &myEmp) {
//             salary = salIn;
//             weight = wgtIn;
//             myEmployee = myEmp;
//         }
//     }
//
//     Then we have:
//
//     makeObjectAllocatorBlock (8192, true);
//     myEmp = makeObject <Employee> (100, 120);
//
//     makeObjectAllocatorBlock (8192, true);
//     mySup = makeObject <Supervisor> (134, 124.5, myEmp);
//
//     The assignment "myEmployee = myEmp;" will cause a deep copy to happen,
//     so that everything inside of the object pointed to by mySup (including
//     the data pointed to by the member myEmployee) resided in the same allocation
//     block.
//
//  (2) Handles also act as smart pointers.  Whenever there is no longer a way
//      to reach an Object that is in an allocation block, the Object is deleted.
//      Whenever an allocation block is no longer active and it has no more reachable
//      Objects, it is automatically freed.
//
//  (3) Finally, Handles act as offset pointers that can move from one address space
//      to another without causing any problems.  This means that objects that use
//      Handles can be written to disk and then read back again by another process,
//      and there won't be any problem.

// Here is the Handle class....
template <class ObjType> 
class Handle : public Object {

private:

	// where this guy is located
	int64_t offset;

	// this is the class that all PDB templates include, so that they can 
	// implement type erasure...
	PDBTemplateBase typeInfo;

public:

	// makes a null handle
	Handle ();

	// free memory, as needed
	~Handle ();

	// makes a null handle
	Handle (const std :: nullptr_t rhs);
	Handle <ObjType> &operator = (const std :: nullptr_t rhs);

	// see if we are null
	friend bool operator == (const Handle <ObjType> &lhs, std :: nullptr_t rhs) {return lhs.isNullPtr ();}
	friend bool operator == (std :: nullptr_t rhs, const Handle <ObjType> &lhs) {return lhs.isNullPtr ();}
	friend bool operator != (std :: nullptr_t rhs, const Handle <ObjType> &lhs) {return !lhs.isNullPtr ();}
	friend bool operator != (const Handle <ObjType> &lhs, std :: nullptr_t rhs) {return !lhs.isNullPtr ();}
	bool isNullPtr () const;

	// a hash on a handle just hashes the underlying object
	size_t hash () const {return (*this)->hash ();}

	// get the reference count for this Handle, if it exists (returns 99999999 if it does not)
	unsigned getRefCount ();

	// makes sure that the data pointed to by *this is located in the current 
	// allocation block.  If it is, simply return a copy of *this.  If it is not,
	// copy the item referenced by it to the current allocation block and return
	// a handle to that item.
	Handle <ObjType> copyTargetToCurrentAllocationBlock ();

	/***************************************************************************/
	/* There are eight different cases for assign/copy construction on Handles */
	/*                                                                         */
	/* 1. Copy construct from RefCountedObject of same Object type             */ 
	/* 2. Copy construct from RefCountedObject of diff Object type             */ 
	/* 3. Copy construct from Handle of same Object type                       */ 
	/* 4. Copy construct from Handle of diff Object type                       */ 
	/* 5. Assignment from RefCountedObject of same Object type                 */ 
	/* 6. Assignment from RefCountedObject of diff Object type                 */ 
	/* 7. Assignment from Handle of same Object type                           */ 
	/* 8. Assignment from Handle of diff Object type                           */ 
	/***************************************************************************/

	/***************************************/
	/* Here are the four copy constructors */
	/***************************************/
	
	/*************************************************************/
	/* Here are the two copy constructors from RefCountedObjects */
	/*************************************************************/
	
	Handle (const RefCountedObject <ObjType> *fromMe);
	template <class ObjTypeTwo>
	Handle (const RefCountedObject <ObjTypeTwo> *fromMe);

	/***************************************************/
	/* Here are the two copy constructors from Handles */
	/***************************************************/
	
	Handle (const Handle <ObjType> &fromMe);
	template <class ObjTypeTwo>
	Handle (const Handle <ObjTypeTwo> &fromMe);

	/******************************************/
	/* Here are the four assignment operators */
	/******************************************/
	
	/****************************************************************/
	/* Here are the two assignment operators from RefCountedObjects */
	/****************************************************************/

	Handle &operator = (const RefCountedObject <ObjType> *fromMe);
	template <class ObjTypeTwo>
	Handle &operator = (const RefCountedObject <ObjTypeTwo> *fromMe);

	/******************************************************/
	/* Here are the two assignment operators from Handles */
	/******************************************************/

	Handle <ObjType> &operator = (const Handle <ObjType> &fromMe);
	template <class ObjTypeTwo>
	Handle <ObjType> &operator = (const Handle <ObjTypeTwo> &fromMe);

	// de-reference operators
	ObjType *operator -> () const;
	ObjType &operator * () const;

	// get/set the offset
	void setOffset (int64_t toMe);
	int64_t getOffset ();

	// get the type code
	int16_t getTypeCode ();

	// so we can perform a deep copy over handles
	ENABLE_DEEP_COPY

	// gets a pointer to the target object
	RefCountedObject <ObjType> *getTarget () const;

private:

	template <class ObjTypeTwo> friend class Handle;
	friend class Allocator;
	template <class Obj, class... Args> friend RefCountedObject <Obj> * makeObject (Args&&... args);
	template <class OutObjType, class InObjType> friend Handle <OutObjType> unsafeCast (Handle <InObjType> &castMe);
	template <class Obj> friend class Record;
};

// equality on handles checks for equality of the underlying objects... 
template <class ObjTypeOne, class ObjTypeTwo>
bool operator == (const Handle <ObjTypeOne> &lhs, const Handle <ObjTypeTwo> &rhs) {
	if (lhs.isNullPtr () || rhs.isNullPtr ())
		return false;
	return *lhs == *rhs;
}

template <class ObjTypeOne, class ObjTypeTwo>
bool operator == (const ObjTypeOne &lhs, const Handle <ObjTypeTwo> &rhs) {
	if (rhs.isNullPtr ())
		return false;
	return lhs == *rhs;
}

template <class ObjTypeOne, class ObjTypeTwo>
bool operator == (const Handle<ObjTypeOne> &lhs, const ObjTypeTwo &rhs) {
	if (lhs.isNullPtr ())
		return false;
	return *lhs == rhs;
}

}

#include "Handle.cc"

#endif
