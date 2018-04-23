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
#include <AdvancedPhysicalOptimizer/Algorithms/AdvancedPhysicalSelectionAlgorithm.h>

namespace pdb {

AdvancedPhysicalSelectionAlgorithm::AdvancedPhysicalSelectionAlgorithm(const AdvancedPhysicalPipelineNodePtr &handle,
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

PhysicalOptimizerResultPtr AdvancedPhysicalSelectionAlgorithm::generate(int nextStageID) {

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

  // set the compute plan
  tupleStageBuilder->setComputePlan(computePlan);

  // copy the computation names
  for(const auto &it : this->pipeComputations) {
    tupleStageBuilder->addTupleSetToBuildPipeline(it->getOutputName());
  }

  // set the parameters
  tupleStageBuilder->setJobStageId(nextStageID);
  tupleStageBuilder->setTargetTupleSetName(finalAtomicComputation->getInputName());
  tupleStageBuilder->setTargetComputationName(finalAtomicComputation->getComputationName());
  tupleStageBuilder->setOutputTypeName(curComp->getOutputType());
  tupleStageBuilder->setSinkContext(sink);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

  // create the job stage
  Handle<TupleSetJobStage> jobStage = tupleStageBuilder->build();

  // add the job stage to the result
  result->physicalPlanToOutput.emplace_back(jobStage);
  result->success = true;
  result->newSourceComputation = handle;

  // the new source is now the sink
  this->sink = sink;

  // return the result
  return result;
}

}
