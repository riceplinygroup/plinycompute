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
#ifndef SILLY_LA_INVERSE2_SELECT_H
#define SILLY_LA_INVERSE2_SELECT_H

// by Binhang, June 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SelectionComp.h"
#include "LASingleMatrix.h"

// LA libraries:
#include <eigen3/Eigen/Dense>

using namespace pdb;

class LAInverse2Selection : public SelectionComp<SingleMatrix, SingleMatrix> {

public:
    ENABLE_DEEP_COPY

    LAInverse2Selection() {}

    Lambda<bool> getSelection(Handle<SingleMatrix> checkMe) override {
        return makeLambda(checkMe, [](Handle<SingleMatrix>& checkMe) { return true; });
    }


    Lambda<Handle<SingleMatrix>> getProjection(Handle<SingleMatrix> checkMe) override {
        return makeLambdaFromMethod(checkMe, getInverse);
    }
};


#endif
