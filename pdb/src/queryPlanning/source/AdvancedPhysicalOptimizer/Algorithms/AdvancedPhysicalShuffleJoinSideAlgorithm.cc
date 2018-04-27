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

#include <JobStageBuilders/TupleSetJobStageBuilder.h>
#include <JobStageBuilders/HashPartitionedJoinBuildHTJobStageBuilder.h>
#include <JoinComp.h>
#include "AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalShuffleJoinSideAlgorithm.h"

AdvancedPhysicalShuffleJoinSideAlgorithm::AdvancedPhysicalShuffleJoinSideAlgorithm(const AdvancedPhysicalPipelineNodePtr &handle,
                                                                                   const std::string &jobID,
                                                                                   const Handle<SetIdentifier> &source,
                                                                                   const vector<AtomicComputationPtr> &pipeComputations,
                                                                                   const Handle<ComputePlan> &computePlan,
                                                                                   const LogicalPlanPtr &logicalPlan,
                                                                                   const ConfigurationPtr &conf)
                                                                                   : AdvancedPhysicalAbstractAlgorithm(handle,
                                                                                                                       jobID,
                                                                                                                       source,
                                                                                                                       pipeComputations,
                                                                                                                       computePlan,
                                                                                                                       logicalPlan,
                                                                                                                       conf) {}

PhysicalOptimizerResultPtr AdvancedPhysicalShuffleJoinSideAlgorithm::generate(int nextStageID) {

  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // get the source atomic computation
  auto sourceAtomicComputation = this->pipeComputations.front();

  // we get the first atomic computation of the join pipeline this should be the apply join computation
  auto joinAtomicComputation = handle->getAdvancedPhysicalNodeHandle()->getPipelineComputationAt(0);

  // get the final atomic computation
  string finalAtomicComputationName = this->pipeComputations.back()->getOutputName();

  Handle<SetIdentifier> sink = nullptr;

  // the computation specifier of this join
  std::string computationSpecifier = joinAtomicComputation->getComputationName();

  // grab the output of the current node
  std::string outputName = joinAtomicComputation->getOutputName();

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // cast the computation to a JoinComp
  Handle<JoinComp<Object, Object, Object>> join = unsafeCast<JoinComp<Object, Object, Object>, Computation>(curComp);

  // mark it as a hash partition join
  join->setJoinType(HashPartitionedJoin);

  // I am a pipeline breaker.
  // We first need to create a TupleSetJobStage with a repartition sink
  sink = makeObject<SetIdentifier>(jobID, outputName + "_repartitionData");
  sink->setPageSize(conf->getBroadcastPageSize());

  // create a tuple set job stage builder
  TupleSetJobStageBuilderPtr tupleStageBuilder = make_shared<TupleSetJobStageBuilder>();

  // copy the computation names
  for(const auto &it : this->pipeComputations) {

    // we don't need the output set name... (that is jsut the way the pipeline building works)
    if(it->getAtomicComputationTypeID() == WriteSetTypeID) {
      continue;
    }

    // add the set name of the atomic computation to the pipeline
    tupleStageBuilder->addTupleSetToBuildPipeline(it->getOutputName());
  }

  // set the parameters
  tupleStageBuilder->setSourceTupleSetName(sourceAtomicComputation->getOutputName());
  tupleStageBuilder->setSourceContext(source);
  tupleStageBuilder->setInputAggHashOut(source->isAggregationResult());
  tupleStageBuilder->setJobId(jobID);
  tupleStageBuilder->setComputePlan(computePlan);
  tupleStageBuilder->setJobStageId(nextStageID++);
  tupleStageBuilder->setTargetTupleSetName(finalAtomicComputationName);
  tupleStageBuilder->setTargetComputationName(computationSpecifier);
  tupleStageBuilder->setOutputTypeName(joinAtomicComputation->getOutputName());
  tupleStageBuilder->setSinkContext(sink);
  tupleStageBuilder->setRepartition(true);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());
  tupleStageBuilder->setRepartitionJoin(true);

  // create the tuple stage to run a pipeline with a hash partition sink
  Handle<TupleSetJobStage> joinPrepStage = tupleStageBuilder->build();

  // add the stage to the list of stages to be executed
  result->physicalPlanToOutput.emplace_back(joinPrepStage);

  // add the sink to the intermediate sets
  result->interGlobalSets.push_back(sink);

  // the source set for the HashPartitionedJoinBuildHTJobStage is the sink set of the TupleSetJobStage
  std::string hashSetName = sink->toSourceSetName();

  // initialize the build hash partition set builder stage
  HashPartitionedJoinBuildHTJobStageBuilderPtr hashBuilder = make_shared<HashPartitionedJoinBuildHTJobStageBuilder>();

  // set the parameters
  hashBuilder->setJobId(jobID);
  hashBuilder->setJobStageId(nextStageID);
  hashBuilder->setSourceTupleSetName(joinPrepStage->getSourceTupleSetSpecifier());
  hashBuilder->setTargetTupleSetName(finalAtomicComputationName);
  hashBuilder->setTargetComputationName(computationSpecifier);
  hashBuilder->setSourceContext(sink);
  hashBuilder->setHashSetName(hashSetName);
  hashBuilder->setComputePlan(computePlan);

  // create the build hash partitioned join hash table job stage to partition and shuffle the source set
  Handle<HashPartitionedJoinBuildHTJobStage> joinPartitionStage = hashBuilder->build();

  // add the stage to the list of stages to be executed
  result->physicalPlanToOutput.emplace_back(joinPartitionStage);

  // set the remaining parameters of the result
  result->success = true;
  result->newSourceComputation = nullptr;

  return result;
}

AdvancedPhysicalAbstractAlgorithmTypeID AdvancedPhysicalShuffleJoinSideAlgorithm::getType() {
  return JOIN_HASH_ALGORITHM;
}
