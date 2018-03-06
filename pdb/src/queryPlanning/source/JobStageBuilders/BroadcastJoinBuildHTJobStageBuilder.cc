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
#include "JobStageBuilders/BroadcastJoinBuildHTJobStageBuilder.h"


namespace pdb {

void BroadcastJoinBuildHTJobStageBuilder::setJobId(const std::string &jobId) {
    this->jobId = jobId;
}

void BroadcastJoinBuildHTJobStageBuilder::setJobStageId(int jobStageId) {
    this->jobStageId = jobStageId;
}

void BroadcastJoinBuildHTJobStageBuilder::setSourceTupleSetName(const std::string &sourceTupleSetName) {
    BroadcastJoinBuildHTJobStageBuilder::sourceTupleSetName = sourceTupleSetName;
}

void BroadcastJoinBuildHTJobStageBuilder::setTargetTupleSetName(const std::string &targetTupleSetName) {
    BroadcastJoinBuildHTJobStageBuilder::targetTupleSetName = targetTupleSetName;
}

void BroadcastJoinBuildHTJobStageBuilder::setComputePlan(const Handle<ComputePlan> &computePlan) {
    BroadcastJoinBuildHTJobStageBuilder::computePlan = computePlan;
}

void BroadcastJoinBuildHTJobStageBuilder::setTargetComputationName(const std::string &targetComputationName) {
    BroadcastJoinBuildHTJobStageBuilder::targetComputationName = targetComputationName;
}

void BroadcastJoinBuildHTJobStageBuilder::setSourceContext(const Handle<SetIdentifier> &sourceContext) {
    BroadcastJoinBuildHTJobStageBuilder::sourceContext = sourceContext;
}

void BroadcastJoinBuildHTJobStageBuilder::setHashSetName(const std::string &hashSetName) {
    BroadcastJoinBuildHTJobStageBuilder::hashSetName = hashSetName;
}

Handle<BroadcastJoinBuildHTJobStage> BroadcastJoinBuildHTJobStageBuilder::build() {
    // create an instance of the BroadcastJoinBuildHTJobStage
    Handle<BroadcastJoinBuildHTJobStage> broadcastJoinStage = makeObject<BroadcastJoinBuildHTJobStage>(this->jobId,
                                                                                                       jobStageId,
                                                                                                       hashSetName);
    // set the parameters
    broadcastJoinStage->setComputePlan(this->computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
    broadcastJoinStage->setSourceContext(sourceContext);

    return broadcastJoinStage;
}
}

