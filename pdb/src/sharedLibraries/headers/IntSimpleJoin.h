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
#ifndef INT_SIMPLE_JOIN_H
#define INT_SIMPLE_JOIN_H

// by Jia, Mar 2017

#include "JoinComp.h"
#include "PDBString.h"
#include "StringIntPair.h"
#include "LambdaCreationFunctions.h"


using namespace pdb;

class IntSimpleJoin : public JoinComp<int, int, StringIntPair, String> {

public:
    ENABLE_DEEP_COPY

    IntSimpleJoin() {}

    Lambda<bool> getSelection(Handle<int> in1,
                              Handle<StringIntPair> in2,
                              Handle<String> in3) override {
        return (makeLambdaFromSelf(in1) == makeLambdaFromMember(in2, myInt)) &&
            (makeLambdaFromMember(in2, myString) == makeLambdaFromSelf(in3));
    }

    Lambda<Handle<int>> getProjection(Handle<int> in1,
                                      Handle<StringIntPair> in2,
                                      Handle<String> in3) override {
        return makeLambda(in1, [](Handle<int>& in1) { return in1; });
    }
};


#endif
