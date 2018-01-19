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
#ifndef LDA_DOC_WORD_TOPIC_ASSIGNMENT_H
#define LDA_DOC_WORD_TOPIC_ASSIGNMENT_H

#include "Object.h"
#include "PDBVector.h"
#include "Handle.h"
#include "TopicAssignment.h"
#include "DocAssignment.h"

/* This class stores the pair (DocAssighment, TopicAssignment) */
using namespace pdb;

class LDADocWordTopicAssignment : public Object {

private:
    Handle<Vector<Handle<DocAssignment>>> myDocsAssigns;
    Handle<Vector<Handle<TopicAssignment>>> myTopicAssigns;

public:
    ENABLE_DEEP_COPY

    LDADocWordTopicAssignment() {}

    void setup() {
        myDocsAssigns = makeObject<Vector<Handle<DocAssignment>>>();
        myTopicAssigns = makeObject<Vector<Handle<TopicAssignment>>>();
    }

    Vector<Handle<DocAssignment>>& getDocAssigns() {
        return *myDocsAssigns;
    }

    Vector<Handle<TopicAssignment>>& getTopicAssigns() {
        return *myTopicAssigns;
    }

    void push_back(Handle<DocAssignment>& me) {
        myDocsAssigns->push_back(me);
    }

    void push_back(Handle<TopicAssignment>& me) {
        myTopicAssigns->push_back(me);
    }

    ~LDADocWordTopicAssignment() {}
};


#endif
