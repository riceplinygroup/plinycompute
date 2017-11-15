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

#ifndef DOC_ASSIGNMENT_H
#define DOC_ASSIGNMENT_H

#include "Object.h"
#include "Handle.h"
#include "NumericalVector.h"

/* This class stores the topic assignment for a document */
using namespace pdb;

class DocAssignment : public Object {

    unsigned whichDoc = 0;
    NumericalVector<unsigned> whichTopics;

public:
    ENABLE_DEEP_COPY

    DocAssignment(unsigned numDims, unsigned whichDoc, unsigned whichTopic, unsigned cnt)
        : whichDoc(whichDoc), whichTopics(numDims, whichTopic, cnt) {}

    DocAssignment() {}

    DocAssignment& operator+(DocAssignment& addMe) {
        whichTopics += addMe.whichTopics;
        return *this;
    }

    unsigned& getKey() {
        return whichDoc;
    }

    DocAssignment& getValue() {
        return *this;
    }

    Vector<unsigned>& getVector() {
        return whichTopics.getVector();
    }
};

#endif
