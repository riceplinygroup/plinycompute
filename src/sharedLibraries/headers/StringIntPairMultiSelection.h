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

#ifndef STRINGINTPAIR_MULTI_SELECT_H
#define STRINGINTPAIR_MULTI_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"
#include "Employee.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "StringIntPair.h"

using namespace pdb;
class StringIntPairMultiSelection : public MultiSelectionComp<StringIntPair, StringIntPair> {

public:
    ENABLE_DEEP_COPY

    StringIntPairMultiSelection() {}

    Lambda<bool> getSelection(Handle<StringIntPair> checkMe) override {
        return makeLambda(checkMe,
                          [](Handle<StringIntPair>& checkMe) { return checkMe->myInt % 9 == 0; });
    }

    Lambda<Vector<Handle<StringIntPair>>> getProjection(Handle<StringIntPair> checkMe) override {
        return makeLambda(checkMe, [](Handle<StringIntPair>& checkMe) {
            Vector<Handle<StringIntPair>> myVec;
            for (int i = 0; i < 10; i++) {
                Handle<StringIntPair> myPair =
                    makeObject<StringIntPair>("Hi", (checkMe->myInt) * i);
                myVec.push_back(myPair);
            }
            return myVec;
        });
    }
};


#endif
