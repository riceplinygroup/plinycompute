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

#ifndef JOIN_MAP_CC
#define JOIN_MAP_CC

#include "InterfaceFunctions.h"
#include "JoinMap.h"

namespace pdb {

template <class ValueType>
JoinMap <ValueType> :: JoinMap (uint32_t initSize) {

	if (initSize < 2) {
		std :: cout << "Fatal Error: JoinMap initialization:" << initSize << " too small; must be at least one.\n";
		initSize = 2;
	}
        
	// this way, we'll allocate extra bytes on the end of the array
	JoinMapRecordClass <ValueType> temp;
	size_t size = temp.getObjSize ();	
	myArray = makeObjectWithExtraStorage <JoinPairArray <ValueType>> (size * initSize, initSize);
}

template <class ValueType>
JoinMap <ValueType> :: JoinMap () {

	JoinMapRecordClass <ValueType> temp;
	size_t size = temp.getObjSize ();	
	myArray = makeObjectWithExtraStorage <JoinPairArray <ValueType>> (size*2, 2);
}

template <class ValueType>
JoinMap <ValueType> :: ~JoinMap () {}

template <class ValueType>
void JoinMap <ValueType> :: setUnused (const size_t &clearMe) {
        myArray->setUnused (clearMe);
}

template <class ValueType>
ValueType &JoinMap <ValueType> :: push (const size_t &me) {
        if (myArray->isOverFull ()) {
            Handle <JoinPairArray <ValueType>> temp = myArray->doubleArray ();
            myArray = temp;
        }
	return myArray->push (me);
}

template <class ValueType>
JoinRecordList <ValueType> JoinMap <ValueType> :: lookup (const size_t &me) {
	return myArray->lookup (me);
}

template <class ValueType>
int JoinMap <ValueType> :: count (const size_t &which) {
	return myArray->count (which);
}

template <class ValueType>
size_t JoinMap <ValueType> :: size () const {
    return myArray->numUsedSlots();
}

template <class ValueType>
JoinMapIterator <ValueType> JoinMap <ValueType> :: begin () {
	JoinMapIterator <ValueType> returnVal (myArray, true);
	return returnVal;	
}

template <class ValueType>
JoinMapIterator <ValueType> JoinMap <ValueType> :: end () {
	JoinMapIterator <ValueType> returnVal (myArray);
	return returnVal;	
}

}

#endif
