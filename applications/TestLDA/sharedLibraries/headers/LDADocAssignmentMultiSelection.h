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

#ifndef LDA_DOC_ASSIGN_MULTI_SELECT_H
#define LDA_DOC_ASSIGN_MULTI_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"
#include "PDBVector.h"
#include "LDADocWordTopicCount.h"
#include "LDADocWordTopicAssignment.h"

/* This class implements a multi-selection that extract  doc assignment from LDADocWordTopicAssignment */
using namespace pdb;

class LDADocAssignmentMultiSelection
    : public MultiSelectionComp<DocAssignment, LDADocWordTopicAssignment> {

public:
    ENABLE_DEEP_COPY

    Lambda<bool> getSelection(Handle<LDADocWordTopicAssignment> checkMe) override {
        return makeLambda(checkMe, [](Handle<LDADocWordTopicAssignment>& checkMe) { return true; });
    }

    Lambda<Vector<Handle<DocAssignment>>> getProjection(
        Handle<LDADocWordTopicAssignment> checkMe) override {
        return makeLambdaFromMethod(checkMe, getDocAssigns);
    }
};


#endif
