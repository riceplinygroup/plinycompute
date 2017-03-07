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
#ifndef TUPLESET_JOBSTAGE_H
#define TUPLESET_JOBSTAGE_H

//by Jia, Mar 5th, 2017

#include "PDBDebug.h"
#include "Object.h"
#include "DataTypes.h"
#include "Handle.h"
#include "PDBString.h"
#include "SetIdentifier.h"
#include "ComputePlan.h"

// PRELOAD %TupleSetJobStage%

namespace pdb {

    class TupleSetJobStage : public Object {
        
    public:

            TupleSetJobStage () {}
             
            TupleSetJobStage (JobStageID stageId) {
                this->id = stageId;
                this->sharedPlan = nullptr;
                this->sourceContext = nullptr;
                this->sinkContext = nullptr;
                this->probeOrNot = false;
                this->repartitionOrNot = false;
                this->combineOrNot = false;
            }

            ~TupleSetJobStage () {}
 
            //to set compute plan
            void setComputePlan( Handle<ComputePlan> plan, std::string sourceTupleSetSpecifier, std::string targetTupleSetSpecifier, std::string targetComputationSpecifier ) {
                this->sharedPlan = plan;
                this->sourceTupleSetSpecifier = sourceTupleSetSpecifier;
                this->targetTupleSetSpecifier = targetTupleSetSpecifier;
                this->targetComputationSpecifier = targetComputationSpecifier;
            } 

            std::string getSourceTupleSetSpecifier() {
                return this->sourceTupleSetSpecifier;
            }

            std::string getTargetTupleSetSpecifier() {
                return this->targetTupleSetSpecifier;
            }

            std::string getTargetComputationSpecifier() {
                return this->targetComputationSpecifier;
            }

            //to get compute plan
            Handle<ComputePlan> getComputePlan() {
                return this->sharedPlan;
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

            //to set hash set identifier
            void setHashContext( Handle<SetIdentifier> hashContext ) {
                this->hashContext = hashContext;
            }

            //to return sink set identifier
            Handle<SetIdentifier> getHashContext() {
                return this->hashContext;
            }

            //to set whether to probe hash table
            void setProbing (bool probeOrNot) {
                this->probeOrNot = probeOrNot;
            }            

            //to return whether this stage requires to probe hash table
            bool isProbing() {
                return this->probeOrNot;
            }

            //to set whether to repartition the output
            void setRepartition (bool repartitionOrNot) {
                this->repartitionOrNot = repartitionOrNot;
            }

            //to return whether this stage requires to repartition output
            bool isRepartition() {
                return this->repartitionOrNot;
            }

            //to set whether to combine the repartitioned output
            void setCombining (bool combineOrNot) {
                this->combineOrNot = combineOrNot;
            }

            //to return whether this stage requires to combine repartitioned output
            bool isCombining() {
                return this->combineOrNot;
            }

            JobStageID getStageId() {
                return this->id;
            }

            void print() {
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

            ENABLE_DEEP_COPY


    private:

            //Input set information
            Handle<SetIdentifier> sourceContext;

            //Hash set information
            Handle<SetIdentifier> hashContext;

            //Output set information
            Handle<SetIdentifier> sinkContext;

            //Output type name
            String outputTypeName;

            //logical plan information
            Handle<ComputePlan> sharedPlan;

            //source tuple set
            String sourceTupleSetSpecifier;

            //target tuple set
            String targetTupleSetSpecifier;

            //target computation
            String targetComputationSpecifier;

            //Does this stage require probing a hash table ?
            bool probeOrNot;

            //Does this stage require repartitioning output?
            bool repartitionOrNot;

            //Does this stage require combining repartitioned results?
            bool combineOrNot;

            //the id to identify this job stage
            JobStageID id;

   };

}

#endif
