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

#ifndef REF_COUNTED_OBJECT_H
#define REF_COUNTED_OBJECT_H

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>

namespace pdb {

// When an Object is stored in an allocator, we have to have a bit of meta-data
// associated with it.  It is the job of the RefCountedObject class to manage
// this meta-data, along with the Object.  Specifically, we need:
//
// (1) A count of the number of Handles that are currently pointing to the Object.
//     When this count goes down to zero, then the object is unreachable and can
//     be freed.
//
class Object;
class PDBTemplateBase;

template <class ObjType>
class RefCountedObject {
	
public:

	// set the number of references to this object to a specific value
	void setRefCount (unsigned toMe);

	// increment the reference count for the object by one
	void incRefCount ();

	// return the ref count
	unsigned getRefCount ();

	// decrement the ref count for the object by one.  If the value of the
	// reference count goes down to zero, the object is deleted using the
	// PDBTemplateBased object
	void decRefCount (PDBTemplateBase &typeInfo);

	// get a pointer to the object itself
	ObjType *getObject () const;

private:

	// so that no one can create one
	RefCountedObject () {}
};

}

#include "RefCountedObject.cc"

#endif
