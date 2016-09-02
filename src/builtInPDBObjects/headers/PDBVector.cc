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

#ifndef VECTOR_CC
#define VECTOR_CC

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
Vector <TypeContained> :: Vector (uint32_t initSize) {

	// this way, we'll allocate extra bytes on the end of the array
	myArray = makeObjectWithExtraStorage <Array <TypeContained>> (sizeof (TypeContained) * initSize, initSize);
}

template <class TypeContained>
Vector <TypeContained> :: Vector (uint32_t initSize, uint32_t usedSize) {

	// this way, we'll allocate extra bytes on the end of the array
	myArray = makeObjectWithExtraStorage <Array <TypeContained>> (sizeof (TypeContained) * initSize, initSize, usedSize);
}

template <class TypeContained>
Vector <TypeContained> :: Vector () {

	myArray = makeObjectWithExtraStorage <Array <TypeContained>> (sizeof (TypeContained), 1);
}

template <class TypeContained>
uint32_t Vector <TypeContained> :: size () {
	return myArray->numUsedSlots ();
}	

template <class TypeContained>
TypeContained &Vector <TypeContained> :: operator [] (uint32_t which) {
	return myArray->getObj (which);
}

template <class TypeContained>
void Vector <TypeContained> :: assign (uint32_t which, const TypeContained& val) {
	myArray->assign (which, val);
}

template <class TypeContained>
void Vector <TypeContained> :: push_back (const TypeContained& val) {
	if (myArray->isFull ()) {
		myArray = myArray->doubleSize ();
	}
	
	myArray->push_back (val);
}

template <class TypeContained>
void Vector <TypeContained> :: pop_back () {
	myArray->pop_back ();
}

template <class TypeContained>
void Vector <TypeContained> :: clear () {
	myArray = makeObjectWithExtraStorage <Array <TypeContained>> (sizeof (TypeContained), 1);
}

template <class TypeContained>
void Vector <TypeContained> :: resize (uint32_t toMe) {
	myArray = myArray->resize (toMe);
}

template <class TypeContained>
TypeContained *Vector <TypeContained> :: c_ptr () const {
	return myArray->c_ptr ();
}

}

#endif
