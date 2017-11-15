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

#ifndef TOPIC_ASSIGNMENT_H
#define TOPIC_ASSIGNMENT_H

#include "Object.h"
#include "Handle.h"
#include "NumericalVector.h"

/* This class stores the word assignment for a topic */
using namespace pdb;

class TopicAssignment : public Object {

    unsigned whichTopic = 0;
    NumericalVector<unsigned> whichWords;

public:
    ENABLE_DEEP_COPY

    TopicAssignment(unsigned numDims, unsigned whichTopic, unsigned whichWord, unsigned cnt)
        : whichTopic(whichTopic), whichWords(numDims, whichWord, cnt) {}

    TopicAssignment() {}

    TopicAssignment& operator+(TopicAssignment& addMe) {
        whichWords += addMe.whichWords;
        return *this;
    }

    unsigned& getKey() {
        return whichTopic;
    }

    TopicAssignment& getValue() {
        return *this;
    }

    Vector<unsigned>& getVector() {
        return whichWords.getVector();
    }
};

#endif
