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
#include <JobStageBuilders/BroadcastJoinBuildHTJobStageBuilder.h>
#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalJoinBroadcastedHashsetAlgorithm.h>
#include <AdvancedPhysicalOptimizer/Pipes/AdvancedPhysicalJoinSidePipe.h>

AdvancedPhysicalJoinBroadcastedHashsetAlgorithm::AdvancedPhysicalJoinBroadcastedHashsetAlgorithm(const AdvancedPhysicalPipelineNodePtr &handle,
                                                                                       const std::string &jobID,
                                                                                       bool isProbing,
                                                                                       bool isOutput,
                                                                                       const Handle<SetIdentifier> &source,
                                                                                       const Handle<ComputePlan> &computePlan,
                                                                                       const LogicalPlanPtr &logicalPlan,
                                                                                       const ConfigurationPtr &conf) :
                                                                                       AdvancedPhysicalAbstractAlgorithm(handle,
                                                                                                                         jobID,
                                                                                                                         isProbing,
                                                                                                                         isOutput,
                                                                                                                         source,
                                                                                                                         computePlan,
                                                                                                                         logicalPlan,
                                                                                                                         conf) {}

PhysicalOptimizerResultPtr AdvancedPhysicalJoinBroadcastedHashsetAlgorithm::generate(int nextStageID,
                                                                                     const StatisticsPtr &stats) {

  // extract the atomic computations from the pipes for this algorithm
  extractAtomicComputations();

  // extract the hash sets we might want to probe
  extractHashSetsToProbe();

  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // get the source atomic computation
  auto sourceAtomicComputation = this->pipelineComputations.front();

  // we get the first atomic computation of the join pipeline that comes after this one.
  // This computation should be the apply join computation
  auto joinAtomicComputation = pipeline.back()->getConsumer(0)->to<AdvancedPhysicalAbstractPipe>()->getPipelineComputationAt(0);

  // get the final atomic computation
  string finalAtomicComputationName = this->pipelineComputations.back()->getOutputName();

  // the computation specifier of this join
  std::string computationSpecifier = joinAtomicComputation->getComputationName();

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // grab the output of the current node
  std::string outputName = joinAtomicComputation->getOutputName();

  // the set identifier of the set where we store the output of the TupleSetJobStage
  sink = makeObject<SetIdentifier>(jobID, outputName + "_broadcastData");
  sink->setPageSize(conf->getBroadcastPageSize());

  // create a tuple set job stage builder
  TupleSetJobStageBuilderPtr tupleStageBuilder = make_shared<TupleSetJobStageBuilder>();

  // copy the computation names
  for (const auto &it : this->pipelineComputations) {

    // we don't need the output set name... (that is jsut the way the pipeline building works)
    if (it->getAtomicComputationTypeID() == WriteSetTypeID) {
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
  tupleStageBuilder->setProbing(isProbing);
  tupleStageBuilder->setComputePlan(computePlan);
  tupleStageBuilder->setJobStageId(nextStageID);
  tupleStageBuilder->setTargetTupleSetName(finalAtomicComputationName);
  tupleStageBuilder->setTargetComputationName(computationSpecifier);
  tupleStageBuilder->setOutputTypeName("IntermediateData");
  tupleStageBuilder->setSinkContext(sink);
  tupleStageBuilder->setBroadcasting(true);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

  // add all the probing hash sets
  for(auto it : probingHashSets) {
    tupleStageBuilder->addHashSetToProbe(it.first, it.second);
  }

  // We are setting isBroadcasting to true so that we run a pipeline with broadcast sink
  Handle<TupleSetJobStage> joinPrepStage = tupleStageBuilder->build();

  // add the stage to the list of stages to be executed
  result->physicalPlanToOutput.emplace_back(joinPrepStage);

  // add the sink to the intermediate sets
  result->interGlobalSets.push_back(sink);

  // grab the hash set name
  std::string hashSetName = sink->toSourceSetName();

  // initialize the build hash partition set builder stage
  BroadcastJoinBuildHTJobStageBuilderPtr broadcastBuilder = make_shared<BroadcastJoinBuildHTJobStageBuilder>();

  // set the parameters
  broadcastBuilder->setJobId(jobID);
  broadcastBuilder->setJobStageId(nextStageID);
  broadcastBuilder->setSourceTupleSetName(joinPrepStage->getSourceTupleSetSpecifier());
  broadcastBuilder->setTargetTupleSetName(finalAtomicComputationName);
  broadcastBuilder->setTargetComputationName(computationSpecifier);
  broadcastBuilder->setSourceContext(sink);
  broadcastBuilder->setHashSetName(hashSetName);
  broadcastBuilder->setComputePlan(computePlan);

  // We then create a BroadcastJoinBuildHTStage
  Handle<BroadcastJoinBuildHTJobStage> joinBroadcastStage = broadcastBuilder->build();

  // we set the name of the hash we just generated
  pipeline.back()->to<AdvancedPhysicalJoinSidePipe>()->setHashSet(hashSetName);

  // add the stage to the list of stages to be executed
  result->physicalPlanToOutput.emplace_back(joinBroadcastStage);

  // set the remaining parameters of the result
  result->success = true;
  result->newSourceComputation = nullptr;

  return result;
}

AdvancedPhysicalAbstractAlgorithmTypeID AdvancedPhysicalJoinBroadcastedHashsetAlgorithm::getType() {
  return JOIN_BROADCASTED_HASHSET_ALGORITHM;
}


