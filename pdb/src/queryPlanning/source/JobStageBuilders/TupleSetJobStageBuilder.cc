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
#include "JobStageBuilders/TupleSetJobStageBuilder.h"


namespace pdb {

TupleSetJobStageBuilder::TupleSetJobStageBuilder() {

  hashSetsToProbe = makeObject<Map<String, String>>();
  policy = defaultAllocator;

  // the default for all types of sinks is false
  isProbing = false;
  isRepartitionJoin = false;
  isRepartitioning = false;
  isBroadcasting = false;
  isCollectAsMap = false;

  // set the set identifiers to null
  sourceContext = nullptr;
  sinkContext = nullptr;
  combinerContext = nullptr;

  numNodesToCollect = 0;
}

void TupleSetJobStageBuilder::setJobId(const std::string &jobId) {
  this->jobId = jobId;
}

void TupleSetJobStageBuilder::setJobStageId(int jobStageId) {
  this->jobStageId = jobStageId;
}

void TupleSetJobStageBuilder::setComputePlan(const Handle<ComputePlan> &plan) {
  this->computePlan = plan;
}

void TupleSetJobStageBuilder::setSourceTupleSetName(const std::string &sourceTupleSetName) {
  this->sourceTupleSetName = sourceTupleSetName;
}

void TupleSetJobStageBuilder::setTargetTupleSetName(const std::string &targetTupleSetName) {
  this->targetTupleSetName = targetTupleSetName;
}

void TupleSetJobStageBuilder::setTargetComputationName(const std::string &targetComputationSpecifier) {
  this->targetComputationName = targetComputationSpecifier;
}

void TupleSetJobStageBuilder::addTupleSetToBuildPipeline(const std::string &buildMe) {
  this->buildTheseTupleSets.push_back(buildMe);
}

void TupleSetJobStageBuilder::addHashSetToProbe(const std::string &outputName, const std::string &hashSetName) {
  (*hashSetsToProbe)[outputName] = hashSetName;
}

void TupleSetJobStageBuilder::setSourceContext(const Handle<SetIdentifier> &sourceContext) {
  this->sourceContext = sourceContext;
}

void TupleSetJobStageBuilder::setSinkContext(const Handle<SetIdentifier> &sinkContext) {
  this->sinkContext = sinkContext;
}

void TupleSetJobStageBuilder::setCombiner(Handle<SetIdentifier> combinerContext) {
  this->combinerContext = combinerContext;
}

void TupleSetJobStageBuilder::setOutputTypeName(const std::string &outputTypeName) {
  this->outputTypeName = outputTypeName;
}

void TupleSetJobStageBuilder::setProbing(bool isProbing) {
  this->isProbing = isProbing;
}

void TupleSetJobStageBuilder::setAllocatorPolicy(AllocatorPolicy policy) {
  this->policy = policy;
}

void TupleSetJobStageBuilder::setRepartitionJoin(bool repartitionJoinOrNot) {
  this->isRepartitionJoin = repartitionJoinOrNot;
}

void TupleSetJobStageBuilder::setBroadcasting(bool broadcastOrNot) {
  this->isBroadcasting = broadcastOrNot;
}

void TupleSetJobStageBuilder::setRepartition(bool repartitionOrNot) {
  this->isRepartitioning = repartitionOrNot;
}

void TupleSetJobStageBuilder::setInputAggHashOut(bool inputAggHashOut) {
  TupleSetJobStageBuilder::inputAggHashOut = inputAggHashOut;
}

void TupleSetJobStageBuilder::setCollectAsMap(bool collectAsMapOrNot) {
  this->isCollectAsMap = collectAsMapOrNot;
}

void TupleSetJobStageBuilder::setNumNodesToCollect(int numNodesToCollect) {
  this->numNodesToCollect = numNodesToCollect;
}

bool TupleSetJobStageBuilder::isPipelineProbing() {
  return isProbing;
}

Handle<SetIdentifier> TupleSetJobStageBuilder::getSourceSetIdentifier() {
  return sourceContext;
}

const std::string &TupleSetJobStageBuilder::getSourceTupleSetName() const {
  return sourceTupleSetName;
}

const std::string &TupleSetJobStageBuilder::getLastSetThatBuildsPipeline() const {
  return buildTheseTupleSets.back();
}

Handle<TupleSetJobStage> TupleSetJobStageBuilder::build() {

  // create an instance of the tuple set job stage
  Handle<TupleSetJobStage> jobStage = makeObject<TupleSetJobStage>(jobStageId);

  // set the parameters
  jobStage->setComputePlan(computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
  jobStage->setTupleSetsToBuildPipeline(buildTheseTupleSets);
  jobStage->setSourceContext(sourceContext);
  jobStage->setSinkContext(sinkContext);
  jobStage->setOutputTypeName(outputTypeName);
  jobStage->setAllocatorPolicy(policy);
  jobStage->setRepartitionJoin(isRepartitionJoin);
  jobStage->setBroadcasting(isBroadcasting);
  jobStage->setRepartition(isRepartitioning);
  jobStage->setJobId(this->jobId);
  jobStage->setCollectAsMap(isCollectAsMap);
  jobStage->setNumNodesToCollect(numNodesToCollect);

  // do we have all the parameters set to do probing
  if (hashSetsToProbe != nullptr && isProbing) {

    // set probing to true
    jobStage->setProbing(true);

    // set them
    jobStage->setHashSetsToProbe(hashSetsToProbe);
  }

  // are we using a combiner
  if (combinerContext != nullptr) {

    // set the parameters for the combiner
    jobStage->setCombinerContext(combinerContext);
    jobStage->setCombining(true);
  }

  // aggregation output should not be kept across
  // stages; if an aggregation has more than one
  // consumers, we need materialize aggregation
  // results.
  if (sourceContext->isAggregationResult()) {
    jobStage->setInputAggHashOut(true);
  }

  PDB_COUT << "PhysicalOptimizer generates tupleSetJobStage:" << "\n";
  return jobStage;
}

}

