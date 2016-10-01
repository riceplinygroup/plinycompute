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
#ifndef JOBSTAGE_H
#define JOBSTAGE_H

//by Jia, Oct 1st

#include "Object.h"
#include "DataTypes.h"
#include "Handle.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "SetIdentifier.h"
#include "SetExpressionIr.h"

// PRELOAD %JobStage%

namespace pdb {

    class JobStage : public Object {
        
    public:

            JobStage () {}
            ~JobStage () {}
 
            JobStage (Handle<Vector<Handle<SetIdentifier>>> inputs, Handle<Vector<Handle<SetIdentifier>>> outputs, Handle<Vector<Handle<pdb_detail::SetExpressionIr>>> operators, bool aggregationOrNot, bool finalOrNot) : inputs(inputs), outputs(outputs), operators(operators), aggregationOrNot(aggregationOrNot), finalOrNot(finalOrNot) {}


            //to return a vector of input set identifiers
            Handle<Vector<Handle<SetIdentifier>>> getInputs() {
                return this->inputs;
            }

            //to return a vector of output set identifiers
            Handle<Vector<Handle<SetIdentifier>>> getOutputs() {
                return this->outputs;
            }

            //to return a vector of operators for this JobStage
            Handle<Vector<Handle<pdb_detail::SetExpressionIr>>> getOperators() {
                return this->operators;
            }

            //to return whether this stage is an aggregation stage
            bool isAggregation() {
                return this->aggregationOrNot;
            }

            //to return whether this stage produces final outputs
            bool isFinal() {
                return this->finalOrNot;
            }

            ENABLE_DEEP_COPY


    private:

            //Input set information
            Handle<Vector<Handle<SetIdentifier>>> inputs;

            //Output set information
            Handle<Vector<Handle<SetIdentifier>>> outputs;

            //operator information
            Handle<Vector<Handle<pdb_detail::SetExpressionIr>>> operators;

            //Is this stage an aggregation stage?
            bool aggregationOrNot;

            //Does this stage produces final output?
            bool finalOrNot;

   };

}

#endif
