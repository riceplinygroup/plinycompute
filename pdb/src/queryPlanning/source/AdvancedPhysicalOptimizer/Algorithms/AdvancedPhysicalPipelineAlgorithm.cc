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
#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalPipelineAlgorithm.h>
#include <AdvancedPhysicalOptimizer/Pipes/AdvancedPhysicalJoinSidePipe.h>

namespace pdb {

AdvancedPhysicalPipelineAlgorithm::AdvancedPhysicalPipelineAlgorithm(const AdvancedPhysicalPipelineNodePtr &handle,
                                                                       const std::string &jobID,
                                                                       bool isProbing,
                                                                       bool isOutput,
                                                                       const Handle<SetIdentifier> &source,
                                                                       const vector<AtomicComputationPtr> &pipeComputations,
                                                                       const Handle<ComputePlan> &computePlan,
                                                                       const LogicalPlanPtr &logicalPlan,
                                                                       const ConfigurationPtr &conf)
                                                                       : AdvancedPhysicalAbstractAlgorithm(handle,
                                                                                                           jobID,
                                                                                                           isProbing,
                                                                                                           isOutput,
                                                                                                           source,
                                                                                                           pipeComputations,
                                                                                                           computePlan,
                                                                                                           logicalPlan,
                                                                                                           conf) {

  // check if this pipeline is joining. If so grab all the hash sets
  if(handle->isJoining()) {

    // get the probing hash sets
    auto sets = handle->getProbingHashSets();

    // there should always be one hash set we are probing for a join
    assert(!sets.empty());

    // add the tuple sets we are probing to the list
    probingHashSets.insert(sets.begin(), sets.end());
  }
}

PhysicalOptimizerResultPtr AdvancedPhysicalPipelineAlgorithm::generate(int nextStageID) {

  // get the final atomic computation
  auto finalAtomicComputation = this->pipeComputations.back();

  // get the source atomic computation
  auto sourceAtomicComputation = this->pipeComputations.front();

  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // grab the output of the current node
  std::string outputName = finalAtomicComputation->getOutputName();

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(finalAtomicComputation->getComputationName()).getComputationHandle();

  // I am a pipeline breaker because I have more than one consumers
  Handle<SetIdentifier> sink = nullptr;

  // in the case that the current computation does not require materialization by default
  // we have to set an output to it, we it gets materialized
  if (!curComp->needsMaterializeOutput()) {

    // set the output
    curComp->setOutput(jobID, outputName);

    // create the sink and set the page size
    sink = makeObject<SetIdentifier>(jobID, outputName);
    sink->setPageSize(conf->getPageSize());

    // add this set to the list of intermediate sets
    result->interGlobalSets.push_back(sink);
  } else {
    // this computation needs materialization either way so just create the sink set identifier
    sink = makeObject<SetIdentifier>(curComp->getDatabaseName(), curComp->getSetName());
  }

  // create a tuple set job stage builder
  TupleSetJobStageBuilderPtr tupleStageBuilder = make_shared<TupleSetJobStageBuilder>();

  // the input to the pipeline is the output set of the source node
  tupleStageBuilder->setSourceTupleSetName(sourceAtomicComputation->getOutputName());

  // set the source set identifier
  tupleStageBuilder->setSourceContext(source);

  // is this source a result of an aggregation
  tupleStageBuilder->setInputAggHashOut(source->isAggregationResult());

  // set the job id
  tupleStageBuilder->setJobId(jobID);

  // are we probing a hash set in this pipeline
  tupleStageBuilder->setProbing(isProbing);

  // set the compute plan
  tupleStageBuilder->setComputePlan(computePlan);

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
  tupleStageBuilder->setJobStageId(nextStageID);
  tupleStageBuilder->setTargetTupleSetName(finalAtomicComputation->getInputName());
  tupleStageBuilder->setTargetComputationName(finalAtomicComputation->getComputationName());
  tupleStageBuilder->setOutputTypeName(curComp->getOutputType());
  tupleStageBuilder->setSinkContext(sink);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

  // add all the probing hash sets
  for(auto it : probingHashSets) {
    tupleStageBuilder->addHashSetToProbe(it.first, it.second);
  }

  // create the job stage
  Handle<TupleSetJobStage> jobStage = tupleStageBuilder->build();

  // add the job stage to the result
  result->physicalPlanToOutput.emplace_back(jobStage);
  result->success = true;

  // if this is the output we just created new source
  if(!isOutput) {
    result->newSourceComputation = handle;
  }

  // the new source is now the sink
  this->source = sink;

  // return the result
  return result;
}

AdvancedPhysicalAbstractAlgorithmTypeID AdvancedPhysicalPipelineAlgorithm::getType() {
  return SELECTION_ALGORITHM;
}

PhysicalOptimizerResultPtr AdvancedPhysicalPipelineAlgorithm::generatePipelined(int nextStageID, std::vector<AdvancedPhysicalPipelineNodePtr> &pipelines) {

  // get the source set identifier of the first node in the pipeline
  source = pipelines.front()->getSourceSetIdentifier();

  // go through each stage check if we a probing and copy the atomic computations
  for(auto &p : pipelines) {

    if(p->isJoining()) {

      // set the is probing flag
      this->isProbing = p->isJoining();

      // get the probing hash sets
      auto sets = p->getProbingHashSets();

      // there should always be one hash set we are probing for a join
      assert(!sets.empty());

      // add the tuple sets we are probing to the list
      probingHashSets.insert(sets.begin(), sets.end());
    }

    // get the atomic computations of the pipeline
    auto computations = p->getPipeComputations();

    // append the pipelined operators
    pipeComputations.insert(pipeComputations.begin(), computations.begin(), computations.end());
  }

  // generate the stage
  return generate(nextStageID);
}

}
