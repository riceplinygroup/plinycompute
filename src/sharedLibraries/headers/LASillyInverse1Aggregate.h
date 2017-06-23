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
#ifndef SILLY_LA_INVERSE1_AGGREGATE_H
#define SILLY_LA_INVERSE1_AGGREGATE_H

//by Binhang, May 2017

#include "ClusterAggregateComp.h"
#include "MatrixBlock.h"
#include "LASingleMatrix.h"
#include "LambdaCreationFunctions.h"



using namespace pdb;


//This aggregation will 
class LASillyInverse1Aggregate : public ClusterAggregateComp <SingleMatrix, MatrixBlock, int, MatrixBlock> {

public:

        ENABLE_DEEP_COPY

        LASillyInverse1Aggregate () {}

        // the key type must have == and size_t hash () defined
        Lambda <int> getKeyProjection (Handle <MatrixBlock> aggMe) override {
                return makeLambda (aggMe, [] (Handle<MatrixBlock> & aggMe) {return 1;});
        }

        // the value type must have + defined
        Lambda <MatrixBlock> getValueProjection (Handle <MatrixBlock> aggMe) override {
                return makeLambda (aggMe, [] (Handle<MatrixBlock> & aggMe) {return *aggMe;});
        }
};


#endif