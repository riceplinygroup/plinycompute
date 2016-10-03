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
#include "ExecutionOperator.h"

// PRELOAD %JobStage%

namespace pdb {

    class JobStage : public Object {
        
    public:

            JobStage () {}

            JobStage (JobStageID stageId) {
                this->id = stageId;
            }

            ~JobStage () {}
 

            void setInput( Handle<SetIdentifier> input ) {
                this->input = input;
            }

            //to return input set identifier
            Handle<SetIdentifier> getInput() {
                return this->input;
            }

            void setOutput( Handle<SetIdentifier> output ) {
                this->output = output;
            }


            //to return a vector of output set identifiers
            Handle<SetIdentifier> getOutput() {
                return this->output;
            }

            void addOperator( Handle<ExecutionOperator> executionOperator ) {
                operators.push_back(executionOperator);
            }

            //to return a vector of operators for this JobStage
            Vector<Handle<ExecutionOperator>> getOperators() {
                return this->operators;
            }

            void setAggregation (bool aggregationOrNot) {
                this->aggregationOrNot = aggregationOrNot;
            }            

            //to return whether this stage is an aggregation stage
            bool isAggregation() {
                return this->aggregationOrNot;
            }

            void setFinal( bool finalOrNot ) {
                this->finalOrNot = finalOrNot;
            }


            //to return whether this stage produces final outputs
            bool isFinal() {
                return this->finalOrNot;
            }

            Vector<Handle<JobStage>> getChildrenStages() {
                return this->childrenStages;
            }

            void appendStage(Handle<JobStage> childStage) {
                this->childrenStages.push_back(childStage);
            }	

            JobStageID getStageId() {
                return this->id;
            }

            ENABLE_DEEP_COPY


    private:

            //Input set information
            Handle<SetIdentifier> input;

            //Output set information
            Handle<SetIdentifier> output;

            //operator information
            Vector<Handle<ExecutionOperator>> operators;

            //Is this stage an aggregation stage?
            bool aggregationOrNot;

            //Does this stage produces final output?
            bool finalOrNot;

            //children nodes
            Vector<Handle<JobStage>> childrenStages;

            JobStageID id;
   };

}

#endif
