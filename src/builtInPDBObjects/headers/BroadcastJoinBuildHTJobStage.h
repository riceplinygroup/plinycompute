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
#ifndef BROADCAST_JOIN_BUILDHT_STAGE_H 
#define BROADCAST_JOIN_BUILDHT_STAGE_H

//by Jia, May 2017

#include "AbstractJobStage.h"
#include "SetIdentifier.h"
#include "ComputePlan.h"

// PRELOAD %BroadcastJoinBuildHTJobStage%

namespace pdb {

//this class encapsulates the job stage for building hash table for broadcasting join

class BroadcastJoinBuildHTJobStage : public AbstractJobStage {

private:

    //input set
    Handle<SetIdentifier> sourceContext;

    //name for output hash table
    String sharedHashSetName;

    //job stage id
    JobStageID id;

    //needs to remove input set 
    bool needsRemoveInputSet;

    //number of pages
    int numPages;

    //logical plan information
    Handle<ComputePlan> sharedPlan;

    //source tuple set
    String sourceTupleSetSpecifier;

    //target tuple set
    String targetTupleSetSpecifier;

    //target computation
    String targetComputationSpecifier;



public:

    ENABLE_DEEP_COPY

//constructor
BroadcastJoinBuildHTJobStage () {}

//constructor
BroadcastJoinBuildHTJobStage (std :: string jobId, JobStageID stageId, std :: string hashSetName) {

    this->jobId = jobId;
    this->id = stageId;
    this->sharedHashSetName = hashSetName;

}

//to set source set identifier
void setSourceContext( Handle<SetIdentifier> sourceContext ) {
    this->sourceContext = sourceContext;
}

//to return source set identifier
Handle<SetIdentifier> getSourceContext() {
    return this->sourceContext;
}

//return job stage type
std :: string getJobStageType() override {
    return "BroadcastJoinBuildHTJobStage";
}

//return job stage id
JobStageID getStageId() override {
    return this->id;
}

//set whether to remove input set after processing this job stage
void setNeedsRemoveInputSet (bool needsRemoveInputSet) {
    this->needsRemoveInputSet = needsRemoveInputSet;
}


//return whether to remove input set after processing this job stage
bool getNeedsRemoveInputSet () {
    return this->needsRemoveInputSet;
}



//set name of the hash set that stores the broadcasted table
void setHashSetName (std :: string hashSetName) {
    this->sharedHashSetName = hashSetName;
}

//return name of the hash set that stores the broadcasted table
std :: string getHashSetName () {
    return this->sharedHashSetName;
}

void print() override {
    std :: cout << "[JOB ID] jobId = " << jobId << std :: endl;
    std :: cout << "[STAGE ID] id = " << id << std :: endl;
    std :: cout << "[HASH SET] name = " << sharedHashSetName << std :: endl;
}

void setNumPages (int numPages) {
    this->numPages = numPages;
}

int getNumPages () {
    return this->numPages;
}

//to set compute plan
void setComputePlan( Handle<ComputePlan> plan, std::string sourceTupleSetSpecifier, std::string targetTupleSetSpecifier, std::string targetComputationSpecifier ) {
    this->sharedPlan = plan;
    this->sourceTupleSetSpecifier = sourceTupleSetSpecifier;
    this->targetTupleSetSpecifier = targetTupleSetSpecifier;
    this->targetComputationSpecifier = targetComputationSpecifier;
}

//nullify compute plan shared pointer
void nullifyComputePlanPointer () {
    this->sharedPlan->nullifyPlanPointer();
}

//to get source tupleset name for this pipeline stage
std::string getSourceTupleSetSpecifier() {
    return this->sourceTupleSetSpecifier;
}

//to get target tupleset name for this pipeline stage
std::string getTargetTupleSetSpecifier() {
    return this->targetTupleSetSpecifier;
}

//to get target computation name for this pipeline stage
std::string getTargetComputationSpecifier() {
    return this->targetComputationSpecifier;
}

//to get compute plan
Handle<ComputePlan> getComputePlan() {
    return this->sharedPlan;
}


};




}

#endif
