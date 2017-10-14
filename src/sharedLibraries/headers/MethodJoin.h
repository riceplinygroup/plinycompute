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
#ifndef METHOD_JOIN_H
#define METHOD_JOIN_H

// by Jia, Mar 2017

#include "JoinComp.h"
#include "PDBString.h"
#include "StringIntPair.h"
#include "LambdaCreationFunctions.h"


using namespace pdb;

class MethodJoin : public JoinComp<StringIntPair, StringIntPair, StringIntPair> {

public:
    ENABLE_DEEP_COPY

    MethodJoin() {}

    Lambda<bool> getSelection(Handle<StringIntPair> in1, Handle<StringIntPair> in2) override {
        return (makeLambdaFromMethod(in1, getSillyInt) == makeLambdaFromMethod(in2, getSillyInt)) &&
            (makeLambdaFromMethod(in1, getSillyString) ==
             makeLambdaFromMethod(in2, getSillyString));
    }

    Lambda<Handle<StringIntPair>> getProjection(Handle<StringIntPair> in1,
                                                Handle<StringIntPair> in2) override {
        return makeLambda(
            in1, in2, [](Handle<StringIntPair>& in1, Handle<StringIntPair>& in2) { return in2; });
    }
};


#endif
