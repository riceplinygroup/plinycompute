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
#ifndef CARTESIAN_JOIN_H
#define CARTESIAN_JOIN_H

//by Jia, Mar 2017

#include "JoinComp.h"
#include "PDBString.h"
#include "StringIntPair.h"
#include "LambdaCreationFunctions.h"


using namespace pdb;

class CartesianJoin : public JoinComp <StringIntPair, int, String> {

public:

        ENABLE_DEEP_COPY

        CartesianJoin () {}

        Lambda <bool> getSelection (Handle <int> in1, Handle <String> in2) override {
                std :: cout << "CartesianJoin selection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue()  << std :: endl;
                return makeLambda (in1, in2, [] (Handle<int> & in1, Handle<String> & in2) {
                    return true;
                });
        }

        Lambda <Handle <StringIntPair>> getProjection (Handle <int> in1, Handle <String> in2) override {
                 std :: cout << "CartesianJoin projection: type code is " << in1.getExactTypeInfoValue() << ", " << in2.getExactTypeInfoValue() << std :: endl;
                return makeLambda (in1, in2, [] (Handle<int> & in1, Handle<String> & in2) {
                    Handle<StringIntPair> pair = makeObject<StringIntPair> ((*in2), (*in1));
                    return pair;
                });
        }

};


#endif
