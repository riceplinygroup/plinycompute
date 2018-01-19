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
#ifndef INT_SELECTION_OF_STRING_INT_PAIR_H
#define INT_SELECTION_OF_STRING_INT_PAIR_H

// by Jia, May 2017

#include "SelectionComp.h"
#include "StringIntPair.h"
#include "LambdaCreationFunctions.h"

using namespace pdb;

class IntSelectionOfStringIntPair : public SelectionComp<int, StringIntPair> {

public:
    ENABLE_DEEP_COPY

    IntSelectionOfStringIntPair() {}

    Lambda<bool> getSelection(Handle<StringIntPair> checkMe) override {
        return makeLambda(checkMe, [](Handle<StringIntPair>& checkMe) {
            if (((*checkMe).myInt % 3 == 0) && ((*checkMe).myInt < 1000)) {
                return true;
            } else {
                return false;
            }
        });
    }

    Lambda<Handle<int>> getProjection(Handle<StringIntPair> checkMe) override {
        return makeLambda(checkMe, [](Handle<StringIntPair>& checkMe) {
            Handle<int> ret = makeObject<int>((*checkMe).myInt);
            return ret;
        });
    }
};

#endif
