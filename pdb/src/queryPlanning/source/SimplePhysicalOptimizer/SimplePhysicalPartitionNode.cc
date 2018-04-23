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
#include "SimplePhysicalOptimizer/SimplePhysicalPartitionNode.h"

namespace pdb {

SimplePhysicalPartitionNode::SimplePhysicalPartitionNode(string jobId,
                                                         AtomicComputationPtr node,
                                                         const Handle<ComputePlan> &computePlan,
                                                         LogicalPlanPtr logicalPlan,
                                                         ConfigurationPtr conf) : SimplePhysicalNode(std::move(jobId),
                                                                                                     std::move(node),
                                                                                                     computePlan,
                                                                                                     logicalPlan,
                                                                                                     std::move(conf)) {}


PhysicalOptimizerResultPtr SimplePhysicalPartitionNode::analyzeOutput(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                                      SimplePhysicalNodePtr &prevNode,
                                                                      const StatisticsPtr &stats,
                                                                      int nextStageID) {

  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // the computation specifier of this partition
  std::string computationSpecifier = node->getComputationName();

  // grab the computation associated with this node
  Handle<Computation> comp = logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // create a SetIdentifier for the output set
  Handle<SetIdentifier> sink = makeObject<SetIdentifier>(comp->getDatabaseName(), comp->getSetName());

  // create the tuple set job stage to run the pipeline with a shuffle sink
  // here is what we are doing :
  // the input to the stage is either the output of the join or the source node we started)
  // the repartitioning flag is set to true, so that we run a pipeline with a hash partition sink
  // the pipeline will apply all the computations to the source set
  // and put them on a page partitioned into multiple vectors, and each vector on that page will be
  // sent to an appropriate node.
  tupleStageBuilder->setJobStageId(nextStageID++);
  tupleStageBuilder->setTargetTupleSetName(node->getInputName());
  tupleStageBuilder->setTargetComputationName(computationSpecifier);
  tupleStageBuilder->setOutputTypeName(comp->getOutputType());
  tupleStageBuilder->setSinkContext(sink);
  tupleStageBuilder->setRepartition(true);
  tupleStageBuilder->setRepartitionVector(true);
  tupleStageBuilder->setAllocatorPolicy(comp->getAllocatorPolicy());
  

  // to push back the job stage
  result->physicalPlanToOutput.emplace_back(tupleStageBuilder->build());

  // this is the output so we are not creating a new source set
  result->newSourceComputation = nullptr;

  // we succeeded
  result->success = true;

  return result;
}

PhysicalOptimizerResultPtr SimplePhysicalPartitionNode::analyzeSingleConsumer(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                                              SimplePhysicalNodePtr &prevNode,
                                                                              const StatisticsPtr &stats,
                                                                              int nextStageID) {
  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // the computation specifier of this partition
  std::string computationSpecifier = node->getComputationName();

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // to create the consuming job stage for partition
  Handle<SetIdentifier> sink;

  // does the current computation already need materialization
  if (curComp->needsMaterializeOutput()) {
    sink = makeObject<SetIdentifier>(curComp->getDatabaseName(), curComp->getSetName());
    sink->setPageSize(conf->getPageSize());
  } else {
    sink = makeObject<SetIdentifier>(jobId, node->getOutputName());
    curComp->setOutput(jobId, node->getOutputName());
    sink->setPageSize(conf->getPageSize());
    result->interGlobalSets.push_back(sink);
  }

  // create the tuple set job stage to run the pipeline with a hash partition sink
  tupleStageBuilder->setJobStageId(nextStageID++);
  tupleStageBuilder->setTargetTupleSetName(node->getInputName());
  tupleStageBuilder->setTargetComputationName(computationSpecifier);
  tupleStageBuilder->setOutputTypeName(curComp->getOutputType());
  tupleStageBuilder->setSinkContext(sink);
  tupleStageBuilder->setRepartition(true);
  tupleStageBuilder->setRepartitionVector(true);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

  // to push back the job stage
  result->physicalPlanToOutput.emplace_back(tupleStageBuilder->build());


  // update the source sets (if the source node is not being used anymore we just remove it)
  result->newSourceComputation = getHandle();

  // we succeeded
  result->success = true;

  // the new source is the sink
  sourceSetIdentifier = sink;

  return result;
}

PhysicalOptimizerResultPtr SimplePhysicalPartitionNode::analyzeMultipleConsumers(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                                                 SimplePhysicalNodePtr &prevNode,
                                                                                 const StatisticsPtr &stats,
                                                                                 int nextStageID) {
  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // the computation specifier of this partition
  std::string computationSpecifier = node->getComputationName();

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // I am a pipeline breaker because I have more than one consumers
  Handle<SetIdentifier> sink = nullptr;

  // in the case that the current computation does not require materialization by default
  // we have to set an output to it, we it gets materialized
  if (!curComp->needsMaterializeOutput()) {

    // set the output
    curComp->setOutput(jobId, node->getOutputName());

    // create the sink and set the page size
    sink = makeObject<SetIdentifier>(jobId, node->getOutputName());
    sink->setPageSize(conf->getPageSize());

    // add this set to the list of intermediate sets
    result->interGlobalSets.push_back(sink);
  } else {
    // this computation needs materialization either way so just create the sink set identifier
    sink = makeObject<SetIdentifier>(curComp->getDatabaseName(), curComp->getSetName());
  }

  // create the tuple set job stage to run the pipeline with a hash partition sink
  tupleStageBuilder->setJobStageId(nextStageID++);
  tupleStageBuilder->setTargetTupleSetName(node->getInputName());
  tupleStageBuilder->setTargetComputationName(computationSpecifier);
  tupleStageBuilder->setOutputTypeName(curComp->getOutputType());
  tupleStageBuilder->setRepartition(true);
  tupleStageBuilder->setRepartitionVector(true);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

  // add the created tuple job stage to the
  result->physicalPlanToOutput.emplace_back(tupleStageBuilder->build());


  // update the source sets to reflect the state after executing the job stages
  result->newSourceComputation = getHandle();

  // we succeeded
  result->success = true;

  // the new source is the sink
  sourceSetIdentifier = sink;

  return result;
}

}
