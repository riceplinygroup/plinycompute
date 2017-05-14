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
#ifndef SILLY_JOIN_H
#define SILLY_JOIN_H

//by Jia, Mar 2017

#include "JoinComp.h"
#include "PDBString.h"
#include "StringIntPair.h"
#include "LambdaCreationFunctions.h"


using namespace pdb;

class SillyJoin : public JoinComp <String, int, StringIntPair, String> {

public:

        ENABLE_DEEP_COPY

        SillyJoin () {}

        Lambda <bool> getSelection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                std :: cout << "SillyJoin selection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
                return (makeLambdaFromSelf (in1) == makeLambdaFromMember (in2, myInt)) && (makeLambdaFromMember (in2, myString) == makeLambdaFromSelf (in3));
        }

        Lambda <Handle <String>> getProjection (Handle <int> in1, Handle <StringIntPair> in2, Handle <String> in3) override {
                 std :: cout << "SillyJoin projection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << ", " << in3.getExactTypeInfoValue() << std :: endl;
                return makeLambdaFromMember (in2, myString);
        }

};


#endif
