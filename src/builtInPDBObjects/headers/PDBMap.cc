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

#ifndef MAP_CC
#define MAP_CC

#include "InterfaceFunctions.h"
#include "PDBMap.h"

namespace pdb {

// does a bunch of bit twiddling to compute a hash of an int
inline unsigned int newHash (unsigned int x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

// computes a hash over an input character array
inline size_t hashMe (char *me, size_t len) {
	size_t code = 0;
	for (int i = 0; i < len; i += 8) {

		int low = 0;
		int high = 0;

		for (int j = 0; j < 4; j++) {
			if (i + j < len) {
				low += (me[i + j] & 0xFF) << (8 * j);
			} else {
				low += 123 << (8 * j);
			}
		}

		if (len <= 4)
			high = low;
		else {
			for (int j = 4; j < 8; j++) {
				if (i + j < len) {
					high += (me[i + j] & 0xFF) << (8 * (j - 3));
				} else {
					high += 97 << (8 * (j - 3));
				}
			}
		}

		size_t returnVal = ((size_t) newHash (high)) << 32;
		returnVal += newHash (low) & 0xFFFFFFFF;
		code = code ^ returnVal;
	}
	return code;
}

template <class KeyType, class ValueType>
Map <KeyType, ValueType> :: Map (uint32_t initSize) {

	if (initSize == 0) {
		std :: cout << initSize << " too small; must be at least one.\n";
		exit (0);
	}

	// this way, we'll allocate extra bytes on the end of the array
	MapRecordClass <KeyType, ValueType> temp;
	size_t size = temp.getObjSize ();	
	myArray = makeObjectWithExtraStorage <PairArray <KeyType, ValueType>> (size * initSize, initSize);
}

template <class KeyType, class ValueType>
Map <KeyType, ValueType> :: Map () {

	MapRecordClass <KeyType, ValueType> temp;
	size_t size = temp.getObjSize ();	
	myArray = makeObjectWithExtraStorage <PairArray <KeyType, ValueType>> (size, 1);
}

template <class KeyType, class ValueType>
Map <KeyType, ValueType> :: ~Map () {}


template <class KeyType, class ValueType>
ValueType &Map <KeyType, ValueType> :: operator [] (KeyType &which) {
		
	if (myArray->isOverFull ()) {
		Handle <PairArray <KeyType, ValueType>> temp = myArray->doubleArray ();
		myArray = temp;
	}
	ValueType &res = (*myArray)[which];
	return res;
}

template <class KeyType, class ValueType>
int Map <KeyType, ValueType> :: count (KeyType &which) {
	return myArray->count ();
}

template <class KeyType, class ValueType>
PDBMapIterator <KeyType, ValueType> Map <KeyType, ValueType> :: begin () {
	PDBMapIterator <KeyType, ValueType> returnVal (myArray, true);
	return returnVal;	
}

template <class KeyType, class ValueType>
PDBMapIterator <KeyType, ValueType> Map <KeyType, ValueType> :: end () {
	PDBMapIterator <KeyType, ValueType> returnVal (myArray);
	return returnVal;	
}


}

#endif
