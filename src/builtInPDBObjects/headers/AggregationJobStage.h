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
#ifndef AGGREGATION_JOBSTAGE_H
#define AGGREGATION_JOBSTAGE_H

//by Jia, Mar 5th, 2017

#include "PDBDebug.h"
#include "Object.h"
#include "DataTypes.h"
#include "Handle.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "SetIdentifier.h"
#include "AbstractAggregateComp.h"
#include "AbstractJobStage.h"

// PRELOAD %AggregationJobStage%

namespace pdb {

    //encapsulates a synchronous aggregation consuming job stage
    class AggregationJobStage : public AbstractJobStage {
        
    public:

            AggregationJobStage () {}
             
            AggregationJobStage (JobStageID stageId, bool materializeOrNot, Handle<AbstractAggregateComp> aggComputation, int numNodePartitions) {
                this->id = stageId;
                this->materializeOrNot = materializeOrNot; 
                this->aggComputation = aggComputation;
                this->numNodePartitions = numNodePartitions;
            }

            ~AggregationJobStage () {}

            std :: string getJobStageType () override {
                return "AggregationJobStage";
            }

            //to set source set identifier
            void setSourceContext( Handle<SetIdentifier> sourceContext ) {
                this->sourceContext = sourceContext;
            }

            //to return source set identifier
            Handle<SetIdentifier> getSourceContext() {
                return this->sourceContext;
            }
 
            //to set sink set identifier
            void setSinkContext( Handle<SetIdentifier> sinkContext ) {
                this->sinkContext = sinkContext;
            }

            //to return sink set identifier
            Handle<SetIdentifier> getSinkContext() {
                return this->sinkContext;
            } 

            JobStageID getStageId() {
                return this->id;
            }

            bool needsToMaterializeAggOut () {
                return materializeOrNot;
            } 

            void print() override {
                PDB_COUT << "[ID] id=" << id << std :: endl;
                PDB_COUT << "[INPUT] databaseName=" << sourceContext->getDatabase()<<", setName=" << sourceContext->getSetName()<< std :: endl;
                PDB_COUT << "[OUTPUT] databaseName=" << sinkContext->getDatabase()<<", setName=" << sinkContext->getSetName()<< std :: endl;
                PDB_COUT << "[OUTTYPE] typeName=" << getOutputTypeName() << std :: endl;
            }

            std :: string getOutputTypeName () {
                return this->outputTypeName;
            }

            void setOutputTypeName (std :: string outputTypeName) {
                this->outputTypeName = outputTypeName;
            }

            Handle<AbstractAggregateComp> getAggComputation() {
                return aggComputation;
            }

            void setAggComputation (Handle<AbstractAggregateComp> aggComputation) {
                this->aggComputation = aggComputation;
            }

            void setNeedsRemoveInputSet (bool needsRemoveInputSet) {
                this->needsRemoveInputSet = needsRemoveInputSet;
            }

            bool getNeedsRemoveInputSet () {
                return this->needsRemoveInputSet;
            }


            int getNumNodePartitions () {
                return this->numNodePartitions;
            }

            ENABLE_DEEP_COPY


    private:

            Handle<SetIdentifier> sourceContext;

            Handle<SetIdentifier> sinkContext;


            JobStageID id;

            String outputTypeName; 

            bool materializeOrNot;

            Handle<AbstractAggregateComp> aggComputation;

            bool needsRemoveInputSet;

            int numNodePartitions;

   };

}

#endif
