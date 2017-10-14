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
#ifndef DOUBLE_VECTOR_RESULT_H
#define DOUBLE_VECTOR_RESULT_H

// by Jia, May 2017

#include "Object.h"
#include "DoubleVector.h"
// PRELOAD %DoubleVectorResult%

namespace pdb {

class DoubleVectorResult : public Object {

public:
    DoubleVectorResult() {}


    DoubleVector doubleVector;
    int identifier;

    ENABLE_DEEP_COPY

    void print() {
        this->doubleVector.print();
    }

    int& getKey() {
        return identifier;
    }

    DoubleVector& getValue() {
        return doubleVector;
    }
};
}


#endif
