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

#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>

namespace pdb {

// This file contains the Allocator class, as well as its helper classes.  While
// these classes are used extenviely during Object management, most programmers
// (even PDB engineers) will not directly touch these classes.  Instead, the
// Allocator is used indirectly, via the functions in InterfactFunctions.h.

#define CHAR_PTR(c) ((char *) c)

// this class is an exception that is (optionally) thrown 
// when we try to do an allocation to create an Object
// but there is not enough RAM
class NotEnoughSpace : public std :: bad_alloc {
	virtual const char* what () const throw () {
		return "Not enough free memory in current allocation block.\n";
	}
};

extern NotEnoughSpace myException;

// this is a little container class for all of the Allocator-managed blocks of
// memory that still contain active objects.  
class InactiveAllocationBlock {

private:

	// where the block begins
	void *start;

	// where the block ends
	void *end;

	friend class Allocator;
public:

	// create a block
	InactiveAllocationBlock ();

	// free the memory associated with the block
	void freeBlock ();

	// create a block... the two params are the location, and the size in bytes
	InactiveAllocationBlock (void *startIn, size_t numBytes);

	// returns true if the block is *before* compToMe, and does not contain it
	friend bool operator < (const InactiveAllocationBlock &in, const void *compToMe);

	// returns true if the ends of the lhs is before the end of the rhs
	friend bool operator < (const InactiveAllocationBlock &lhs, const InactiveAllocationBlock &rhs);
	
	// returns true if the block is *after* compToMe, and does not contain it
	friend bool operator > (const InactiveAllocationBlock &in, const void *compToMe);
	
	// returns true if the block contains compToMe
	friend bool operator == (const InactiveAllocationBlock &in, const void *compToMe);

	// decrement the reference count of this block
	void decReferenceCount ();

	// return the reference count
	unsigned getReferenceCount ();

	// free it if there are no more references
	bool areNoReferences ();
};

// this class holds the current state of an alloctor
struct AllocatorState {

	// the location of the block of RAM that this guy is allocating from
	void *activeRAM;

	// the number of bytes available in all
	size_t numBytes;

	// true if we are supposed to throw an exception on allocation failure
	bool throwException;

	// true if the current allocation block is user-supplied
	bool curBlockUserSupplied;

	// Th Allocator implements a really simple memory manager.  Every time there is a
	// request, the allocator class services it by pre-pending an usigned int to the returned
	// bytes that mark the length of the set of bytes.
	//
	// When the bytes are freed, a pointer to the region is added to the "chunks" 
	// vector, declared below.
	//
	// All of the free regions are organized into 32 lists.  The first list has all
	// chunks 2^0 bytes in size.  The second all chunks 2^1 bytes in size.  The third
	// all 2^2 and up to 2^3 - 1.  The fourth 2^3 and up to 2^4 - 1.  The fifth all
	// 2^4 and up to 2^5 - 1, and so on.
	// 
	// When a request for RAM is serviced, the correct list is located (that is, the 
	// first list that could possibly service the request) depending upon the size of
	// the request.  It is searched linearly, and the first free region that can 
	// service the request is returned.  If no region in the list can service it, the
	// next list is checked.  Then the next, and so on.
	std :: vector <std :: vector <void *>> chunks;
};

template <class ObjType>
class Handle;

// this is the Allocator class.  There is one allocator per thread.  The allocator
// is responsible for managing the RAM that is used to allocate everything that
// is descended from object.  Most programmers (even PDB engineers) will never touch
// the allocator class directly.  
class Allocator {

private:

	AllocatorState myState;

	// this is the list of all self-managed allocation blocks that are not active, but
	// still contain some object...
	std :: vector <InactiveAllocationBlock> allInactives;

public:

	// return true if allocations should not fail due to not enough RAM...
	// in this case, a null pointer is returned on a bad allocate, and NOT
	// an exception
	bool doNotFail ();

	// destructor; if there is a self-allocated current allocation block, free it
	~Allocator ();

	// we have no active RAM
	Allocator ();

	// give this guy the specified active RAM
	Allocator (size_t numBytes);

	// get the number of currently-reachable objects in this guy's block
	template <class ObjType>
	unsigned getNumObjectsInHomeAllocatorBlock (Handle <ObjType> &forMe);

	// get the number of currently-reachable objects in the current allocation block
	unsigned getNumObjectsInCurrentAllocatorBlock ();

	// get the number of bytes available in the current allocation block
	size_t getBytesAvailableInCurrentAllocatorBlock ();

	// returns true if and only if the RAM is in the current allocation block
	bool contains (void *whereIn);

	// returns true if and only if the RAM is in the current allocation block
	// or it is in some inactive allocation block that has not been deleted
	// as of yet
	bool isManaged (void *here); 

	// returns some RAM... this can throw an exception if the request is too large
	// to be handled because there is not enough RAM in the current allocation block
	void *getRAM (size_t howMuch);

	// free some RAM that was previous allocated via a call to getRAM
	void freeRAM (void *here);

	// make this RAM the current allocation block
	void setupBlock (void *where, size_t numBytesIn, bool throwExceptionOnFail);

	// make this user-supplied RAM the current allocation block
	void setupUserSuppliedBlock (void *where, size_t numBytesIn, bool throwExceptionOnFail);

	// returns a pointer to the allocation block housing the
	// object pointed to by this handle.  A nullptr is returned
	// if it is point possible to find the allocation block
	// pointed to by this handle.  This happens if the object
	// pointed to by forMe was not created in an allocation 
	// block managed by this thread's allocator.
	//
	// In the block of memory returned, the first few
	// byes returned store the number of bytes used by the
	// allocator, and the next few bytes store an offset to
	// the location of forMe.
	template <class ObjType> 
	void * getAllocationBlock (Handle <ObjType> &forMe);

	// uses a specified block of memory for all allocations, until 
	// restoreAllocationBlock () is called... SHOUD NOT BE USED DIRECTLY
	AllocatorState temporarilyUseBlockForAllocations (void *putMeHere, size_t numBytesAvailable);
	AllocatorState temporarilyUseBlockForAllocations (size_t numBytesAvailable);

	// goes back to the old allocation block.. this should only
	// by alled after a call to temporarilyUseBlockForAllocations ()
	void restoreAllocationBlock (AllocatorState &restoreMe);
	void restoreAllocationBlockAndManageOldOne (AllocatorState &restoreMe);

	// like the above call, except that it does not erase the current
	// allocation block; it is managed (reference counted) just like all
	// of the others, and deleted as needed later on
	void restoreAllocationBlockAndManageOldOne ();

private:

	friend void makeObjectAllocatorBlock (size_t numBytesIn);
};

// returns a reference to the allocator that should be used.  Each process has one default allocator.
// Each of the workers in a WorkerQueue are given their own allocator, which resides at the beginning
// of the worker's call stack.  If this is called from within a worker, the worker's allocator is
// returned; otherwise, the process' default allocator is returned.
Allocator &getAllocator ();

}

// because all of the methods are inline, include the .cc file
#include "Allocator.cc"

#endif
