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

#ifndef LDA_DOC_WORD_TOPIC_MULTI_SELECT_H
#define LDA_DOC_WORD_TOPIC_MULTI_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"
#include "PDBVector.h"
#include "LDA/LDADocWordTopicCount.h"
#include "LDADocWordTopicAssignment.h"

using namespace pdb;
class LDADocWordTopicMultiSelection
    : public MultiSelectionComp<LDADocWordTopicCount, LDADocWordTopicAssignment> {

public:
    ENABLE_DEEP_COPY

    LDADocWordTopicMultiSelection() {}

    Lambda<bool> getSelection(Handle<LDADocWordTopicAssignment> checkMe) override {
        return makeLambda(checkMe, [](Handle<LDADocWordTopicAssignment>& checkMe) { return true; });
    }

    Lambda<Vector<Handle<LDADocWordTopicCount>>> getProjection(
        Handle<LDADocWordTopicAssignment> checkMe) override {
        return makeLambda(checkMe, [&](Handle<LDADocWordTopicAssignment>& checkMe) {

            Vector<Handle<LDADocWordTopicCount>> result;
            Vector<int>& topicAssign = checkMe->getTopicAssignment();
            int docID = checkMe->getDoc();
            int wordID = checkMe->getWord();

            for (int i = 0; i < topicAssign.size(); i += 2) {
                Handle<LDADocWordTopicCount> myDWTC = makeObject<LDADocWordTopicCount>(
                    docID, wordID, topicAssign[i], topicAssign[i + 1]);
                result.push_back(myDWTC);
            }

            return result;
        });
    }
};


#endif
