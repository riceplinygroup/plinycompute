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
#include <Handle.h>
#include "HashPartitionedJoinBuildHTJobStage.h"
#include "JobStageBuilders/HashPartitionedJoinBuildHTJobStageBuilder.h"

namespace pdb {

void HashPartitionedJoinBuildHTJobStageBuilder::setJobId(const std::string &jobId) {
  HashPartitionedJoinBuildHTJobStageBuilder::jobId = jobId;
}

void HashPartitionedJoinBuildHTJobStageBuilder::setJobStageId(int jobStageId) {
  HashPartitionedJoinBuildHTJobStageBuilder::jobStageId = jobStageId;
}

void HashPartitionedJoinBuildHTJobStageBuilder::setSourceTupleSetName(const std::string &sourceTupleSetName) {
  HashPartitionedJoinBuildHTJobStageBuilder::sourceTupleSetName = sourceTupleSetName;
}

void HashPartitionedJoinBuildHTJobStageBuilder::setTargetTupleSetName(const std::string &targetTupleSetName) {
  HashPartitionedJoinBuildHTJobStageBuilder::targetTupleSetName = targetTupleSetName;
}

void HashPartitionedJoinBuildHTJobStageBuilder::setTargetComputationName(const std::string &targetComputationName) {
  HashPartitionedJoinBuildHTJobStageBuilder::targetComputationName = targetComputationName;
}

void HashPartitionedJoinBuildHTJobStageBuilder::setSourceContext(const Handle<SetIdentifier> &sourceContext) {
  HashPartitionedJoinBuildHTJobStageBuilder::sourceContext = sourceContext;
}

void HashPartitionedJoinBuildHTJobStageBuilder::setHashSetName(const std::string &hashSetName) {
  HashPartitionedJoinBuildHTJobStageBuilder::hashSetName = hashSetName;
}

void HashPartitionedJoinBuildHTJobStageBuilder::setComputePlan(const Handle<ComputePlan> &computePlan) {
  HashPartitionedJoinBuildHTJobStageBuilder::computePlan = computePlan;
}

Handle<HashPartitionedJoinBuildHTJobStage> HashPartitionedJoinBuildHTJobStageBuilder::build() {
  // create an instance of the HashPartitionedJoinBuildHTJobStage
  Handle<HashPartitionedJoinBuildHTJobStage> hashPartitionedJobStage = makeObject<HashPartitionedJoinBuildHTJobStage>(this->jobId,
                                                                                                                      jobStageId,
                                                                                                                      hashSetName);

  // set the parameters
  hashPartitionedJobStage->setComputePlan(this->computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
  hashPartitionedJobStage->setSourceContext(sourceContext);

  return hashPartitionedJobStage;
}


}


