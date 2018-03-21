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
#include "JobStageBuilders/AggregationJobStageBuilder.h"
#include "JobStageBuilders/TupleSetJobStageBuilder.h"
#include "SimplePhysicalOptimizer/SimplePhysicalNode.h"
#include "SimplePhysicalOptimizer/SimplePhysicalAggregationNode.h"

namespace pdb {

SimplePhysicalAggregationNode::SimplePhysicalAggregationNode(string jobId,
                                                                     AtomicComputationPtr node,
                                                                     const Handle<ComputePlan> &computePlan,
                                                                     LogicalPlanPtr logicalPlan,
                                                                     ConfigurationPtr conf) : SimplePhysicalNode(std::move(jobId),
                                                                                                                     std::move(node),
                                                                                                                     computePlan,
                                                                                                                     logicalPlan,
                                                                                                                     std::move(conf)) {}

PhysicalOptimizerResultPtr SimplePhysicalAggregationNode::analyzeOutput(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                                       SimplePhysicalNodePtr &prevNode,
                                                                       const StatisticsPtr &stats,
                                                                       int nextStageID) {

  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // the computation specifier of this aggregation
  std::string computationSpecifier = node->getComputationName();

  // grab the computation associated with this node
  Handle<Computation> comp = logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // create a SetIdentifier for the output set
  Handle<SetIdentifier> sink = makeObject<SetIdentifier>(comp->getDatabaseName(), comp->getSetName());

  // create the set identifier where we store the data to be aggregated after the TupleSetJobStage
  Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(jobId, node->getOutputName() + "_aggregationData");
  aggregator->setPageSize(conf->getShufflePageSize());

  Handle<SetIdentifier> combiner = nullptr;
  // are we using a combiner (the thing that combines the records by key before sending them to the right node)
  if (comp->isUsingCombiner()) {
    combiner = makeObject<SetIdentifier>(jobId, node->getOutputName() + "_combinerData");
  }

  // cast the computation to AbstractAggregateComp to create the consuming job stage for aggregation
  Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(comp);

  // create the tuple set job stage to run the pipeline with a shuffle sink
  // here is what we are doing :
  // the input to the stage is either the output of the join or the source node we started)
  // the repartitioning flag is set to true, so that we run a pipeline with a shuffle sink
  // the pipeline until the combiner will apply all the computations to the source set
  // and put them on a page partitioned into multiple maps the combiner will then read the maps that belong to
  // the partitions of a certain node and combine them by key. The output pages of the combiner will then be sent
  // to the appropriate nodes depending on the parameters isCollectAsMap and getNumNodesToCollect
  tupleStageBuilder->setJobStageId(nextStageID++);
  tupleStageBuilder->setTargetTupleSetName(node->getInputName());
  tupleStageBuilder->setTargetComputationName(computationSpecifier);
  tupleStageBuilder->setOutputTypeName("IntermediateData");
  tupleStageBuilder->setCombiner(combiner);
  tupleStageBuilder->setSinkContext(aggregator);
  tupleStageBuilder->setRepartition(true);
  tupleStageBuilder->setAllocatorPolicy(comp->getAllocatorPolicy());
  tupleStageBuilder->setCollectAsMap(agg->isCollectAsMap());
  tupleStageBuilder->setNumNodesToCollect(agg->getNumNodesToCollect());

  // to push back the aggregator set
  result->interGlobalSets.push_back(aggregator);

  // to push back the job stage
  result->physicalPlanToOutput.emplace_back(tupleStageBuilder->build());

  // initialize the build hash partition set builder stage
  AggregationJobStageBuilderPtr aggregationBuilder = make_shared<AggregationJobStageBuilder>();

  // to create the consuming job stage for aggregation
  aggregationBuilder->setJobId(jobId);
  aggregationBuilder->setJobStageId(nextStageID);
  aggregationBuilder->setAggComp(agg);
  aggregationBuilder->setSourceContext(aggregator);
  aggregationBuilder->setSinkContext(sink);
  aggregationBuilder->setMaterializeOrNot(true);

  // to push back the aggregation stage;
  result->physicalPlanToOutput.emplace_back(aggregationBuilder->build());

  // this is the output so we are not creating a new source set
  result->newSourceComputation = nullptr;

  // we succeeded
  result->success = true;

  return result;
}

PhysicalOptimizerResultPtr SimplePhysicalAggregationNode::analyzeSingleConsumer(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                                               SimplePhysicalNodePtr &prevNode,
                                                                               const StatisticsPtr &stats,
                                                                               int nextStageID) {
  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // the computation specifier of this aggregation
  std::string computationSpecifier = node->getComputationName();

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // to create the producing job stage for aggregation and set the page size
  Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(jobId, node->getOutputName() + "_aggregationData");
  aggregator->setPageSize(conf->getShufflePageSize());

  // are we using a combiner (the thing that combines the records by key before sending them to the right node)
  Handle<SetIdentifier> combiner = nullptr;
  if (curComp->isUsingCombiner()) {
    combiner = makeObject<SetIdentifier>(jobId, node->getOutputName() + "_combinerData");
  }

  // cast the computation to AbstractAggregateComp to create the consuming job stage for aggregation
  Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(curComp);

  // create the tuple set job stage to run the pipeline with a shuffle sink
  // here is what we are doing :
  // the input to the stage is either the output of the join or the source node we started)
  // the repartitioning flag is set to true, so that we run a pipeline with a shuffle sink
  // the pipeline until the combiner will apply all the computations to the source set
  // and put them on a page partitioned into multiple maps the combiner will then read the maps that belong to
  // the partitions of a certain node and combine them by key. The output pages of the combiner will then be sent
  // to the appropriate nodes depending on the parameters isCollectAsMap and getNumNodesToCollect
  tupleStageBuilder->setJobStageId(nextStageID++);
  tupleStageBuilder->setTargetTupleSetName(node->getInputName());
  tupleStageBuilder->setTargetComputationName(computationSpecifier);
  tupleStageBuilder->setOutputTypeName("IntermediateData");
  tupleStageBuilder->setCombiner(combiner);
  tupleStageBuilder->setSinkContext(aggregator);
  tupleStageBuilder->setRepartition(true);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());
  tupleStageBuilder->setCollectAsMap(agg->isCollectAsMap());
  tupleStageBuilder->setNumNodesToCollect(agg->getNumNodesToCollect());

  // to push back the job stage
  result->physicalPlanToOutput.emplace_back(tupleStageBuilder->build());

  // to create the consuming job stage for aggregation;
  Handle<SetIdentifier> sink;

  // does the current computation already need materialization
  if (curComp->needsMaterializeOutput()) {
    sink = makeObject<SetIdentifier>(curComp->getDatabaseName(), curComp->getSetName());
  } else {
    sink = makeObject<SetIdentifier>(jobId, node->getOutputName() + "_aggregationResult", PartitionedHashSetType, true);
  }

  // initialize the build hash partition set builder stage
  AggregationJobStageBuilderPtr aggregationBuilder = make_shared<AggregationJobStageBuilder>();

  // create an aggregation job
  aggregationBuilder->setJobId(jobId);
  aggregationBuilder->setJobStageId(nextStageID);
  aggregationBuilder->setAggComp(agg);
  aggregationBuilder->setSourceContext(aggregator);
  aggregationBuilder->setSinkContext(sink);
  aggregationBuilder->setMaterializeOrNot(curComp->needsMaterializeOutput());

  // to push back the aggregation stage;
  result->physicalPlanToOutput.emplace_back(aggregationBuilder->build());

  // to push back the aggregator set
  result->interGlobalSets.push_back(aggregator);

  // update the source sets (if the source node is not being used anymore we just remove it)
  result->newSourceComputation = getSimpleNodeHandle();

  // we succeeded
  result->success = true;

  // the new source is the sink
  sourceSetIdentifier = sink;

  return result;
}

PhysicalOptimizerResultPtr SimplePhysicalAggregationNode::analyzeMultipleConsumers(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                                                  SimplePhysicalNodePtr &prevNode,
                                                                                  const StatisticsPtr &stats,
                                                                                  int nextStageID) {
  // create a analyzer result
  PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

  // the computation specifier of this aggregation
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

  // create the set identifier where we store the data to be aggregated after the TupleSetJobStage
  Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(jobId, node->getOutputName() + "_aggregationData");
  aggregator->setPageSize(conf->getShufflePageSize());

  // are we using a combiner (the thing that combines the records by key before sending them to the right node)
  Handle<SetIdentifier> combiner = nullptr;
  if (curComp->isUsingCombiner()) {
    // create a set identifier for the combiner
    combiner = makeObject<SetIdentifier>(jobId, node->getOutputName() + "_combinerData");
  }

  // create the tuple set job stage to run the pipeline with a shuffle sink
  // here is what we are doing :
  // the input to the stage is either the output of the join or the source node we started)
  // the repartitioning flag is set to true, so that we run a pipeline with a shuffle sink
  // the pipeline until the combiner will apply all the computations to the source set
  // and put them on a page partitioned into multiple maps the combiner will then read the maps that belong to
  // the partitions of a certain node and combine them by key. The output pages of the combiner will then be sent
  tupleStageBuilder->setJobStageId(nextStageID++);
  tupleStageBuilder->setTargetTupleSetName(node->getInputName());
  tupleStageBuilder->setTargetComputationName(computationSpecifier);
  tupleStageBuilder->setOutputTypeName("IntermediateData");
  tupleStageBuilder->setCombiner(combiner);
  tupleStageBuilder->setSinkContext(aggregator);
  tupleStageBuilder->setRepartition(true);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

  // add the created tuple job stage to the
  result->physicalPlanToOutput.emplace_back(tupleStageBuilder->build());

  // cast the computation to AbstractAggregateComp to create the consuming job stage for aggregation
  Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(curComp);

  // we need an aggregation stage after this to aggregate the results from the tuple stage we previously created
  // the data will be aggregated in the sink set
  AggregationJobStageBuilderPtr aggregationBuilder = make_shared<AggregationJobStageBuilder>();

  aggregationBuilder->setJobId(jobId);
  aggregationBuilder->setJobStageId(nextStageID);
  aggregationBuilder->setAggComp(agg);
  aggregationBuilder->setSourceContext(aggregator);
  aggregationBuilder->setSinkContext(sink);
  aggregationBuilder->setMaterializeOrNot(curComp->needsMaterializeOutput());

  // to push back the aggregation stage;
  result->physicalPlanToOutput.emplace_back(aggregationBuilder->build());

  // to push back the aggregator set
  result->interGlobalSets.push_back(aggregator);

  // update the source sets to reflect the state after executing the job stages
  result->newSourceComputation = getSimpleNodeHandle();

  // we succeeded
  result->success = true;

  // the new source is the sink
  sourceSetIdentifier = sink;

  return result;
}

}