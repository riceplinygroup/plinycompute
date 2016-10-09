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

#ifndef ALLOCATOR_CC
#define ALLOCATOR_CC

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>

namespace pdb {

#define CHAR_PTR(c) ((char *) c)
#define ALLOCATOR_REF_COUNT (*((unsigned *) (CHAR_PTR (start) + 2 * sizeof (size_t))))

inline InactiveAllocationBlock :: InactiveAllocationBlock () {
	start = nullptr;
	end = 0;
}

// free the memory associated with the block
inline void InactiveAllocationBlock :: freeBlock () {
	if (start != nullptr)
		free (start);
}

// create a block
inline InactiveAllocationBlock :: InactiveAllocationBlock (void *startIn, size_t numBytes) {
	start = startIn;
	end = CHAR_PTR (startIn) + numBytes;	
}

// returns true if the block is *before* compToMe, and does not contain it
inline bool operator < (const InactiveAllocationBlock &in, const void *compToMe) {
	return (CHAR_PTR (in.end) < CHAR_PTR (compToMe));
}

// returns true if the ends of the lhs is before the end of the rhs
inline bool operator < (const InactiveAllocationBlock &lhs, const InactiveAllocationBlock &rhs) {
	return (CHAR_PTR (lhs.end) < CHAR_PTR (rhs.end));
}
	
// returns true if the block is *after* compToMe, and does not contain it
inline bool operator > (const InactiveAllocationBlock &in, const void *compToMe) {
	return (CHAR_PTR (in.start) > CHAR_PTR (compToMe));
}
	
// returns true if the block contains compToMe
inline bool operator == (const InactiveAllocationBlock &in, const void *compToMe) {
	return (CHAR_PTR (in.start) <= CHAR_PTR (compToMe) && CHAR_PTR (in.end) >= CHAR_PTR (compToMe));
}

// decrement the reference count of this block
inline void InactiveAllocationBlock :: decReferenceCount () {
	ALLOCATOR_REF_COUNT--;
}

// free it if there are no more references
inline bool InactiveAllocationBlock :: areNoReferences () {
	return ALLOCATOR_REF_COUNT == 0;
}

inline unsigned InactiveAllocationBlock :: getReferenceCount () {
	return ALLOCATOR_REF_COUNT;
}

// These macros are used to manipulate the block of RAM that makes up an allocation block
// The layout is | num bytes used | offset to root object | number active objects | data <--> |
#undef ALLOCATOR_REF_COUNT
#define ALLOCATOR_REF_COUNT (*((unsigned *) (CHAR_PTR (myState.activeRAM) + 2 * sizeof (size_t))))
#define LAST_USED (*((size_t *) myState.activeRAM))
#define OFFSET_TO_OBJECT (*((size_t *) (CHAR_PTR (myState.activeRAM) + sizeof (size_t))))
#define OFFSET_TO_OBJECT_RELATIVE(fromWhere) (*((size_t *) (CHAR_PTR (fromWhere) + sizeof (size_t))))
#define HEADER_SIZE (sizeof (unsigned) + 2 * sizeof (size_t))
#define GET_CHUNK_SIZE(ofMe) (*((unsigned *) ofMe))
#define CHUNK_HEADER_SIZE sizeof (unsigned)

// return true if allocations should not fail due to not enough RAM...
// in this case, a null pointer is returned on a bad allocate, and NOT
// an exception
inline bool Allocator :: doNotFail () {
	return (myState.throwException == false);	
}

// destructor; if there is a self-allocated current allocation block, free it
inline Allocator :: ~Allocator () {

	if (myState.activeRAM != nullptr && !myState.curBlockUserSupplied) {
		if (getNumObjectsInCurrentAllocatorBlock () != 0) {
			std :: cout << "This is bad.  Current allocation block has " << getNumObjectsInCurrentAllocatorBlock () << " references.\n";
		}
		free (myState.activeRAM);
	}

	for (auto &a : allInactives) {
		if (a.areNoReferences ()) {
			std :: cout << "This is bad.  There is an allocation block left with no references.\n";
			exit (1);
		} else {
			std :: cout << "This is bad.  There is an allocation block left with " << a.getReferenceCount () << " references.\n";
			exit (1);
		}
	}
}

// we have no active RAM
inline Allocator :: Allocator () {
	for (int i = 0; i < 32; i++) {
		std :: vector <void *> temp;
		myState.chunks.push_back (temp);
	}
	myState.activeRAM = nullptr;
	myState.numBytes = 0;

	// now, setup the active block
	setupBlock (malloc (1024), 1024, true);
}

inline Allocator :: Allocator (size_t numBytesIn) {
	for (int i = 0; i < 32; i++) {
		std :: vector <void *> temp;
		myState.chunks.push_back (temp);
	}
	myState.activeRAM = nullptr;
	myState.numBytes = 0;

	// now, setup the active block
	setupBlock (malloc (numBytesIn), numBytesIn, true);
}

// returns true if and only if the RAM is in the current allocation block
inline bool Allocator :: contains (void *whereIn) {
	char *where = CHAR_PTR (whereIn);
	char *target = CHAR_PTR (myState.activeRAM);
	return (where >= target && where < target + myState.numBytes);
}

// returns some RAM... this can throw an exception if the request is too large
// to be handled because there is not enough RAM in the current allocation block
inline void *Allocator :: getRAM (size_t howMuch) {

	unsigned bytesNeeded = (unsigned) (CHUNK_HEADER_SIZE + howMuch);
	if ((bytesNeeded % 4) != 0) {
		bytesNeeded += (4 - (bytesNeeded % 4));
	}
	
	// get the number of leading zero bits in bytesNeeded
	int numLeadingZeros = __builtin_clz (bytesNeeded);
	
	// loop through all of the free chunks
	// Lets say that someone asks for 54 bytes.  In binary, this is ...000110110 and so there are 26 leading zeros
	// in the binary representation.  So, we are going to loop through the sets of chunks at position 5 (2^5 and larger)
	// at position 6 (2^6 and larger) at position 7 (2^7 and larger), and so on, trying to find one that fits.
	for (int i = 31 - numLeadingZeros; i < 32; i++) {
		int len = myState.chunks[i].size ();
		for (int j = len - 1; j >= 0; j--) {
			if (GET_CHUNK_SIZE (myState.chunks[i][j]) >= bytesNeeded) {
				void *returnVal = myState.chunks[i][j];
				myState.chunks[i].erase (myState.chunks[i].begin () + j);
				ALLOCATOR_REF_COUNT++;
				return CHAR_PTR (returnVal) + CHUNK_HEADER_SIZE;
			}
		}
	}

	// if we got here, then we cannot fit, and we need to carve out a bit at the end
	// if there is not enough RAM
	if (LAST_USED + bytesNeeded > myState.numBytes) {

		// see how we are supposed to react...
		if (myState.throwException) {

			// either we throw an exception...
			throw myException;

		// or we return a nullptr
		} else {
			return nullptr;
		}
		
	}

	// now do the allocation
	void *res = LAST_USED + CHAR_PTR (myState.activeRAM);
	LAST_USED += bytesNeeded;
	GET_CHUNK_SIZE (res) = bytesNeeded;
	ALLOCATOR_REF_COUNT++;
	return CHAR_PTR (res) + CHUNK_HEADER_SIZE;
}

inline bool Allocator :: isManaged (void *here) {

	// see if this guy is from the active block
	if (contains (here)) {
		return true;
	}

	// otherwise, he is not in the active block, so look for him
	auto i = std :: lower_bound (allInactives.begin (), allInactives.end (), here);

	// see if we found him
	if (i != allInactives.end () && !(*i > here)) {
		return true;
	}

	return false;
}

// free some RAM
inline void Allocator :: freeRAM (void *here) {

	// see if this guy is from the active block
	if (contains (here)) {

		here = CHAR_PTR (here) - CHUNK_HEADER_SIZE;

		// get the chunk size
		unsigned chunkSize = GET_CHUNK_SIZE (here);
		
		// get the number of leading zeros
		int leadingZeros = __builtin_clz (chunkSize);

		// and remember this chunk
		myState.chunks[31 - leadingZeros].push_back (here);
		
		ALLOCATOR_REF_COUNT--;
		return;
	}

	// otherwise, he is not in the active block, so look for him
	auto i = std :: lower_bound (allInactives.begin (), allInactives.end (), here);

	// see if we found him
	if (i != allInactives.end () && !(*i > here)) {

		// we did, so dec reference count
		i->decReferenceCount ();

		// if he is done, delete him
		if (i->areNoReferences ()) {
			i->freeBlock ();
			allInactives.erase (i);	
		}	

		return;
	}

	// if we made it here, the object was allocated in some other way,
	// so we can go ahead and forget about him
}

inline void Allocator :: setupUserSuppliedBlock (void *where, size_t numBytesIn, bool throwExceptionOnFail) {
	setupBlock (where, numBytesIn, throwExceptionOnFail);
	myState.curBlockUserSupplied = true;
}

inline size_t Allocator :: getBytesAvailableInCurrentAllocatorBlock () {

	// count all of the chunks that are not currently in use
	unsigned amtUnused = 0;
	for (auto &a : myState.chunks) {
		for (auto &v  : a) {
			amtUnused += GET_CHUNK_SIZE (v);
		}
	}
	
	return myState.numBytes - LAST_USED + amtUnused;
}

inline unsigned Allocator :: getNumObjectsInCurrentAllocatorBlock () {
	return ALLOCATOR_REF_COUNT;
}

template <class ObjType>
unsigned Allocator :: getNumObjectsInHomeAllocatorBlock (Handle <ObjType> &forMe) {

	void *here = forMe.getTarget ();

	// see if this guy is from the active block
	if (contains (here)) {
		return ALLOCATOR_REF_COUNT;
	}

	// otherwise, he is not in the active block, so look for him
	auto i = std :: lower_bound (allInactives.begin (), allInactives.end (), here);

	// see if we found him
	if (i != allInactives.end () && !(*i > here)) {

		// we did, so dec reference count
		return i->getReferenceCount ();
	}

	return 0;
}

inline void Allocator :: setupBlock (void *where, size_t numBytesIn, bool throwExceptionOnFail) {

	// make sure that we are gonna be able to write the header
	if (numBytesIn < HEADER_SIZE) {
		std :: cerr << "You need to have an allocation block that is at least " << HEADER_SIZE << " bytes.\n";
		int *foo = 0;
		*foo = 24;
		exit (1);
	}

	// remember how to handle a failed allocate
	myState.throwException = throwExceptionOnFail;

	// if there is currently an active block, then it becomes inactive
	if (myState.activeRAM != nullptr && !myState.curBlockUserSupplied) {

		// don't remember a block with no objects
		if (ALLOCATOR_REF_COUNT != 0) {
			allInactives.emplace_back (myState.activeRAM, myState.numBytes);	
			std :: sort (allInactives.begin (), allInactives.end ());
		} else {
			free (myState.activeRAM);
		}
	}

	// empty out the list of unused chunks of RAM in this block
	for (auto &c : myState.chunks) {
		c.clear ();
	}

	myState.activeRAM = where;

	myState.numBytes = numBytesIn;	
	myState.curBlockUserSupplied = false;
	LAST_USED = HEADER_SIZE;	
	ALLOCATOR_REF_COUNT = 0;
}

template <class ObjType> 
void *Allocator :: getAllocationBlock (Handle <ObjType> &forMe) {

	// try to find the allocation block
	void *here = forMe.getTarget ();

	// see if this guy is from the active block
	if (contains (here)) {
		//std :: cout << "getAllocationBlock: object offset =" << CHAR_PTR (here) - CHAR_PTR (myState.activeRAM) << std :: endl;
		// set up the pointer to the object
		OFFSET_TO_OBJECT = CHAR_PTR (here) - CHAR_PTR (myState.activeRAM);
		return myState.activeRAM;
	}

	// he's not, so see if he is from another block
	auto i = std :: lower_bound (allInactives.begin (), allInactives.end (), here);

	// see if we found him
	if (i != allInactives.end () && !(*i > here)) {

		// set up the pointer to the object
		OFFSET_TO_OBJECT_RELATIVE (i->start) = CHAR_PTR (here) - CHAR_PTR (i->start);
		return i->start;
	}

	// if we got here, we could not find this dude
	return nullptr;
}

// uses a specified block of memory for all allocations, until 
// restoreAllocationBlock () is called.
inline AllocatorState Allocator :: temporarilyUseBlockForAllocations (void *putMeHere, size_t numBytesAvailable) {

	// remember the old stuff
	AllocatorState returnVal = myState;

	// mark this one as user-supplied so that it is remembered
	myState.curBlockUserSupplied = false;

	// give the current alloction block a phantom reference count so it is not freed
	ALLOCATOR_REF_COUNT++;
	
	// and set up the new block
	setupUserSuppliedBlock (putMeHere, numBytesAvailable, true);
	return returnVal;
}

// uses a specified block of memory for all allocations, until 
// restoreAllocationBlock () is called.
inline AllocatorState Allocator :: temporarilyUseBlockForAllocations (size_t numBytesAvailable) {

	// remember the old stuff
	AllocatorState returnVal = myState;

	// mark this one as user-supplied so that it is remembered
	myState.curBlockUserSupplied = false;

	// give the current alloction block a phantom reference count so it is not freed
	ALLOCATOR_REF_COUNT++;
	
	// and set up the new block
	void *putMeHere = malloc (numBytesAvailable);
	setupBlock (putMeHere, numBytesAvailable, true);
	return returnVal;
}

inline void Allocator :: restoreAllocationBlockAndManageOldOne (AllocatorState &useMe) {

	// first, remember the current block
	// don't remember a block with no objects
	if (ALLOCATOR_REF_COUNT != 0) {
		allInactives.emplace_back (myState.activeRAM, myState.numBytes);	
		std :: sort (allInactives.begin (), allInactives.end ());
	} else {
		free (myState.activeRAM);
	}

	// now restore the old block
	restoreAllocationBlock (useMe);
}

// goes back to the old allocation block.. this should only
// by called after a call to temporarilyUseBlockForAllocations ()
inline void Allocator :: restoreAllocationBlock (AllocatorState &useMe) {

	myState = useMe;
	
	// remove the old allocation block from the list of inactive ones
	for (int i = 0; i < allInactives.size (); i++) {
		if (allInactives[i].start == myState.activeRAM) {
			allInactives.erase (allInactives.begin () + i);
			break;
		}
	}

	// remove the phantom reference count for the old block
	ALLOCATOR_REF_COUNT--;
}

extern void *stackBase;
extern void *stackEnd;
extern Allocator *mainAllocatorPtr;

inline Allocator &getAllocator () {

        // this serves to gives us the location of our stack, which we use to map to an allocator
        int i;

        // if we are not in one of the created worker threads, then we return the main allocator
        // we know we are not in one of the worker threads if (a) there are no worker threads (stackBase is null)
        // or the address of i is not within the valid range of the stack
        if (stackBase == nullptr || !(((char *) &i) >= (char *) stackBase && ((char *) &i) < (char *) stackEnd))
                return *mainAllocatorPtr;

        // chop off the last 22 bits of i to get the address of this thread's allocator... we chop off 22 bits
        // because the size of the allocated region is 2^22 bytes
        size_t temp = (size_t) &i;
        return *((Allocator *) ((temp >> 22) << 22));
}


}

#endif
