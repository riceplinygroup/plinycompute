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
#ifndef NO_RECLAIM_POLICY_H
#define NO_RECLAIM_POLICY_H

//added by Jia, May 2017
#include "Allocator.h"

namespace pdb {

class NoReclaimPolicy {

public:

// free some RAM
#ifdef DEBUG_OBJECT_MODEL
inline freeRAM (void *here, AllocatorState myState, int16_t typeId) {
#else
inline void freeRAM (void *here, AllocatorState myState) {
#endif

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
                
                #ifdef DEBUG_OBJECT_MODEL      
                std :: cout << "###################################"<< std :: endl;
                std :: cout << "allocator block reference count--=" << ALLOCATOR_REF_COUNT << " with typeId=" << typeId << std :: endl;
                std :: cout << "allocator block start =" << myState.activeRAM << std :: endl;
                std :: cout << "allocator numBytes=" << myState.numBytes << std :: endl;
                std :: cout << "freed numBytes=" << chunkSize << std :: endl;
                std :: cout << "chunk index=" << 31-leadingZeros << std :: endl;
                std :: cout << "###################################"<< std :: endl;
                #endif     
		return;
	}

	// otherwise, he is not in the active block, so look for him
	auto i = std :: lower_bound (allInactives.begin (), allInactives.end (), here);

	// see if we found him
	if (i != allInactives.end () && !(*i > here)) {

		// we did, so dec reference count
		i->decReferenceCount ();
                #ifdef DEBUG_OBJECT_MODEL     
                std :: cout << "###################################"<< std :: endl;
                std :: cout << "allocator block reference count--=" << i->getReferenceCount() << " with typeId=" << typeId << std :: endl;
                std :: cout << "allocator block starting address=" << i->getStart() << std :: endl;
                std :: cout << "allocator block size=" << i->numBytes() << std :: endl;
                std :: cout << "###################################"<< std :: endl;
                #endif     

		// if he is done, delete him
		if (i->areNoReferences ()) {
			i->freeBlock ();
                        PDB_COUT << "Killed an old block naturally." << std :: endl;
			allInactives.erase (i);	
		}	

		return;
	}

	// if we made it here, the object was allocated in some other way,
	// so we can go ahead and forget about him
        #ifdef DEBUG_OBJECT_MODEL
        std :: cout << "###################################################################" << std :: endl;
        std :: cout << "We can't free the object, it may be allocated by some other thread!" << std :: endl;
        std :: cout << "typeId=" << typeId << std :: endl;
        std :: cout << "###################################################################" << std :: endl;
        #endif
}

// returns some RAM... this can throw an exception if the request is too large
// to be handled because there is not enough RAM in the current allocation block
#ifdef DEBUG_OBJECT_MODEL
inline void * getRAM (size_t howMuch, AllocatorState myState, int16_t typeId) {
        std :: cout << "to get RAM with size = " << howMuch << " and typeId = " << typeId << std :: endl;
#else
inline void * getRAM (size_t howMuch, AllocatorState myState) {
#endif
	unsigned bytesNeeded = (unsigned) (CHUNK_HEADER_SIZE + howMuch);
	if ((bytesNeeded % 4) != 0) {
		bytesNeeded += (4 - (bytesNeeded % 4));
	}
	// get the number of leading zero bits in bytesNeeded
	unsigned int numLeadingZeros = __builtin_clz (bytesNeeded);

#ifdef DEBUG_OBJECT_MODEL
       std :: cout << "howMuch=" << howMuch << ", bytesNeeded=" << bytesNeeded << ", numLeadingZeros=" << numLeadingZeros << std :: endl;
#endif


	// if we got here, then we cannot fit, and we need to carve out a bit at the end
	// if there is not enough RAM
	if (LAST_USED + bytesNeeded > myState.numBytes) {

		// see how we are supposed to react...
		if (myState.throwException) {
                        PDB_COUT << "Allocator: LAST_USED=" << LAST_USED << std :: endl;
                        PDB_COUT << "Allocator: bytesNeeded=" << bytesNeeded << std :: endl;
                        PDB_COUT << "Allocator: numBytes=" << myState.numBytes << std :: endl;
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
	void *retAddress= CHAR_PTR (res) + CHUNK_HEADER_SIZE;
        
        #ifdef DEBUG_OBJECT_MODEL
        std :: cout << "###################################"<< std :: endl;
        std :: cout << "allocator block reference count++=" << ALLOCATOR_REF_COUNT << " with typeId=" << typeId << std :: endl;
        std :: cout << "allocator block start =" << myState.activeRAM << std :: endl;                     
        std :: cout << "allocator numBytes=" << myState.numBytes << std :: endl;
        std :: cout << "created a new chunk with size =" << bytesNeeded << std :: endl;
        std :: cout << "###################################"<< std :: endl;
        #endif
        return retAddress;
}


};



}
#endif
