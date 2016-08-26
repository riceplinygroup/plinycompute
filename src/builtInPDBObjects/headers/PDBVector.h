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
#include "Handle.h"
#include "Array.h"

#ifndef VECTOR_H
#define VECTOR_H

#include <cstddef>
#include <iostream>
#include <iterator>
#include <cstring>

// PRELOAD %Vector <Nothing>%

namespace pdb {

// This is the basic Vector type that works correcrly with Objects and Handles.
// The operations have exactly the same interface as std :: vector, except that
// not all operations are implemented.

template <class TypeContained>           
class Vector : public Object {

private:

	// this is where the data are actually stored
	Handle <Array <TypeContained>> myArray;

public:

	ENABLE_DEEP_COPY

	// these operations all have the same semantics as in std :: vector
	Vector (uint32_t initSize);
	Vector (Vector <TypeContained> &assignToMe);
	Vector ();
	uint32_t size ();
	TypeContained &operator [] (uint32_t which);
	void assign (uint32_t which, const TypeContained& val);
	void push_back (const TypeContained& val);
	void pop_back ();
	void clear ();
	TypeContained *c_ptr () const;
	void resize (uint32_t toMe);

	// beause the communicator needs to see inside to do efficient sends
        friend class PDBCommunicator;
};

}

#include "PDBVector.cc"

#endif

