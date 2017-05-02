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

#ifndef PTR_H
#define PTR_H

namespace pdb {

class PtrBase {};

// this class wraps up a pointer, and provides implicity dereferencing...
// it is used during query processing, whenever we want to store a pointer
// to a data member.  This will also be used when we have a C++ lambda or
// a method call that returns a reference; in this case, we store a pointer
// to the value so that we can avoid a copy
template <class BaseType> 
class Ptr : public PtrBase {

private:

	BaseType *data;

public:

	Ptr () {
		data = nullptr;
	}

	Ptr (BaseType *fromMe) {
		data = fromMe;
	} 

	Ptr (BaseType &fromMe) {
		data = &fromMe;
	}

	Ptr &operator = (BaseType *fromMe) {
		data = fromMe;
		return *this;
	}

	Ptr &operator = (BaseType &fromMe) {
		data = &fromMe;
		return *this;
	}

	~Ptr () {}
	
	operator BaseType& () const {
		return *data;
	}

	operator BaseType * () const {
		return data;
	}

	static size_t getObjectSize () {
		return sizeof (BaseType);	
	}
};

}

#endif
