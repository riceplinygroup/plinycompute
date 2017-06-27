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
#include <cstddef>

// By Shangyu

using namespace pdb;
class IntDoubleVectorPair : public Object {

private:


public:
        int myInt;
	Vector<double> myVector;

	ENABLE_DEEP_COPY

        ~IntDoubleVectorPair () {}
        IntDoubleVectorPair () {}

	IntDoubleVectorPair (int fromInt, Handle<Vector<double>>& fromVector) {
		this->myInt = fromInt;
		this->myVector = *fromVector;
		/*
		int size = fromVector->size();
		this->myVector = makeObject<Vector<double>>(size, size);
		for (int i = 0; i < size; i++) {
			(*(this->myVector))[i] = (*fromVector)[i];
		}
		*/
	}
	
	void setInt(int fromInt) {
		this->myInt = fromInt;
	}

	void setVector(Handle<Vector<double>>& fromVector) {
		int size = fromVector->size();
                for (int i = 0; i < size; i++) {
                    myVector.push_back((*fromVector)[i]);
                }
		//this->myVector = *fromVector;
		/*
		if (this->myVector->c_ptr() == nullptr || this->myVector->size() < size)
                	this->myVector = makeObject<Vector<double>>(size, size);
                for (int i = 0; i < size; i++) {
                        (*(this->myVector))[i] = (*fromVector)[i];
                }
		*/
	}

	int getInt() {
		return this->myInt;
	}

	Vector<double>& getVector() {
		return this->myVector;
	}
	
	int &getKey() {
                return this->myInt;
        }

        Vector<double> &getValue() {
                 return this->myVector;
        }

};
namespace pdb{
inline Vector<double> &operator+ (Vector<double> &lhs, Vector<double> &rhs) {
        int size = lhs.size();
        if (size != rhs.size()) {
                std :: cout << "You cannot add two vectors in different sizes!" << std :: endl;
                return lhs;
        }
        for (int i = 0; i < size; ++i) {
		if (rhs[i] != 0)
                	lhs[i] = lhs[i] + rhs[i];
        }
        return lhs;

}
}

#endif
