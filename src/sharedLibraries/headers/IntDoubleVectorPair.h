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

#ifndef INT_DOUBLE_VECTOR_PAIR_H
#define INT_DOUBLE_VECTOR_PAIR_H

#include "Object.h"
#include "PDBVector.h"
#include "Handle.h"

// By Shangyu

namespace pdb {

class IntDoubleVectorPair : public Object {

private:

        int myInt;
	Handle<Vector<double>> myVector;

public:

	ENABLE_DEEP_COPY

        ~IntDoubleVectorPair () {}
        IntDoubleVectorPair () {}

	IntDoubleVectorPair (int fromInt, Handle<Vector<double>>& fromVector) {
		this->myVector = fromVector;
		this->myInt = fromInt;
	}
	
	void setInt(int fromInt) {
		this->myInt = fromInt;
	}

	void setVector(Handle<Vector<double>>& fromVector) {
		this->myVector = fromVector;
	}

	int getInt() {
		return this->myInt;
	}

	Handle<Vector<double>>& getVector() {
		return this->myVector;
	}



};

}

#endif
