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
#ifndef NO_REFERENCE_COUNT_POLICY_H
#define NO_REFERENCE_COUNT_POLICY_H

//added by Jia, May 2017
#include "Allocator.h"

namespace pdb {

class NoReferenceCountPolicy {

public:

// free some RAM
#ifdef DEBUG_OBJECT_MODEL
inline freeRAM (void *here, AllocatorState myState, int16_t typeId) {
#else
inline void freeRAM (void *here, AllocatorState myState) {
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

	// loop through all of the free chunks
	// Lets say that someone asks for 54 bytes.  In binary, this is ...000110110 and so there are 26 leading zeros
	// in the binary representation.  So, we are going to loop through the sets of chunks at position 5 (2^5 and larger)
	// at position 6 (2^6 and larger) at position 7 (2^7 and larger), and so on, trying to find one that fits.
	for (unsigned int i = 31 - numLeadingZeros; i < 32; i++) {
		int len = myState.chunks[i].size ();
		for ( int j = len - 1;  j >= 0;  j-- ) {
			if (GET_CHUNK_SIZE (myState.chunks[i][j]) >= bytesNeeded) {
				void *returnVal = myState.chunks[i][j];
				myState.chunks[i].erase (myState.chunks[i].begin () + j);
				ALLOCATOR_REF_COUNT++;
                                void *retAddress= CHAR_PTR (returnVal) + CHUNK_HEADER_SIZE;
      #ifdef DEBUG_OBJECT_MODEL                          
                                std :: cout << "###################################"<< std :: endl;
                                std :: cout << "allocator block reference count++=" << ALLOCATOR_REF_COUNT << " with typeId=" << typeId << std :: endl;
                                std :: cout << "allocator block start =" << myState.activeRAM << std :: endl;
                                std :: cout << "allocator numBytes=" << myState.numBytes << std :: endl;
                                std :: cout << "starting chunk index=" << 31-numLeadingZeros << std :: endl;
                                std :: cout << "ending chunk index=" << i << std :: endl;
                                std :: cout << "bytes needed=" << bytesNeeded << std :: endl;
                                std :: cout << "###################################"<< std :: endl;
      #endif  
                                return retAddress;
			}
                 }
        }

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
