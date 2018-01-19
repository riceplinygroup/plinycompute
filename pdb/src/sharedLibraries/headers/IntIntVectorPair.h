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
#ifndef INT_INT_VECTOR_PAIR_H
#define INT_INT_VECTOR_PAIR_H

#include "Object.h"
#include "PDBVector.h"
#include "Handle.h"

/* This class is for storing a (int, int vector) pair */
using namespace pdb;
class IntIntVectorPair : public Object {

private:
    int myInt;
    Vector<int> myVector;

public:
    ENABLE_DEEP_COPY

    ~IntIntVectorPair() {}
    IntIntVectorPair() {}

    IntIntVectorPair(int fromInt, Handle<Vector<int>>& fromVector) {
        this->myVector = *fromVector;
        this->myInt = fromInt;
    }

    int getInt() {
        return this->myInt;
    }

    Vector<int>& getVector() {
        return this->myVector;
    }

    int& getKey() {
        return this->myInt;
    }

    Vector<int>& getValue() {
        return this->myVector;
    }
};

/* Overload the + operator */
namespace pdb {
inline Vector<int>& operator+(Vector<int>& lhs, Vector<int>& rhs) {
    int size = lhs.size();
    if (size != rhs.size()) {
        std::cout << "You cannot add two vectors in different sizes!" << std::endl;
        return lhs;
    }
    int* lhsArray = lhs.c_ptr();
    int* rhsArray = rhs.c_ptr();
    for (int i = 0; i < size; ++i) {
        lhsArray[i] = lhsArray[i] + rhsArray[i];
    }
    return lhs;
}
}
#endif
