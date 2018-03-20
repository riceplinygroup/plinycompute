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
#include "SetIdentifier.h"
#include "Statistics.h"
#include "JobStageBuilders/TupleSetJobStageBuilder.h"
#include "SimplePhysicalOptimizer/SimplePhysicalNode.h"

namespace pdb {

SimplePhysicalNode::SimplePhysicalNode(string jobId,
                                               AtomicComputationPtr node,
                                               const Handle<ComputePlan> &computePlan,
                                               LogicalPlanPtr logicalPlan,
                                               ConfigurationPtr conf) : AbstractPhysicalNode(jobId,
                                                                                                 node,
                                                                                                 computePlan,
                                                                                                 logicalPlan,
                                                                                                 conf),
                                                                                                 handle(nullptr) {}

TCAPAnalyzerResultPtr SimplePhysicalNode::analyze(const StatisticsPtr &stats, int nextStageID) {

  // create a job stage builder
  TupleSetJobStageBuilderPtr jobStageBuilder = make_shared<TupleSetJobStageBuilder>();

  // the input to the pipeline is the output set of the source node
  jobStageBuilder->setSourceTupleSetName(node->getOutputName());

  // set the source set identifier
  jobStageBuilder->setSourceContext(sourceSetIdentifier);

  // is this source a result of an aggregation
  jobStageBuilder->setInputAggHashOut(sourceSetIdentifier->isAggregationResult());

  // set the job id
  jobStageBuilder->setJobId(jobId);

  // set the compute plan
  jobStageBuilder->setComputePlan(computePlan);

  // this is a source so there is no last node
  SimpleTCAPAnalyzerNodePtr prevNode = nullptr;

  // run the recursive analysis it will essentially grab the first consumer of the source node
  // and analyze it as if the source had just one consumer
  auto result = SimplePhysicalNode::analyzeSingleConsumer(jobStageBuilder, prevNode, stats, nextStageID);

  // if we failed we want to avoid processing the same consumer twice therefore we are moving it to the back
  if(!result->success) {

    // grab the consumer
    auto tmp = activeConsumers.front();

    // pop it from the front
    activeConsumers.pop_front();

    // push it to the back
    activeConsumers.push_back(tmp);
  }

  return result;
}

TCAPAnalyzerResultPtr SimplePhysicalNode::analyze(TupleSetJobStageBuilderPtr &jobStageBuilder,
                                                      SimpleTCAPAnalyzerNodePtr &prevNode,
                                                      const StatisticsPtr &stats,
                                                      int nextStageID) {

  // depending on the number of consumers this node has we call different methods to analyze the plan
  switch (consumers.size()){
    // we are analyzing an output node
    case 0: return analyzeOutput(jobStageBuilder, prevNode, stats, nextStageID);

    // we are analyzing a node that has a single consumer
    case 1: return analyzeSingleConsumer(jobStageBuilder, prevNode, stats, nextStageID);

    // we are analyzing a node that has multiple consumers
    default: return analyzeMultipleConsumers(jobStageBuilder, prevNode, stats, nextStageID);
  }
}

bool SimplePhysicalNode::hasConsumers() {
  return !activeConsumers.empty();
}

void SimplePhysicalNode::addConsumer(const AbstractTCAPAnalyzerNodePtr &consumer) {
  // call the consumer
  AbstractPhysicalNode::addConsumer(consumer);

  // add the consumer to the active consumers
  activeConsumers.push_back(std::dynamic_pointer_cast<SimplePhysicalNode>(consumer));
}

double SimplePhysicalNode::getCost(Handle<SetIdentifier> source, const StatisticsPtr &stats) {

  // do we have statistics, if not just return 0
  if(stats == nullptr) {
    std::cout << "Ninja" << std::endl;
    return 0;
  }

  // if the set identifier does not exist log that
  if (source == nullptr) {
    PDB_COUT << "WARNING: there is no source set for key=" << source->toSourceSetName() << "\n";
    return 0;
  }

  // calculate the cost based on the formula cost = number_of_bytes / 1000000
  double cost = stats->getNumBytes(source->getDatabase(), source->getSetName());
  return double((size_t) cost / 1000000);
}

TCAPAnalyzerResultPtr SimplePhysicalNode::analyzeSingleConsumer(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                                    SimpleTCAPAnalyzerNodePtr &prevNode,
                                                                    const StatisticsPtr &stats,
                                                                    int nextStageID) {

  // add this node to the pipeline
  tupleStageBuilder->addTupleSetToBuildPipeline(node->getOutputName());

  // this is a source so there is no last node
  SimpleTCAPAnalyzerNodePtr newPrevNode = getHandle();

  // go to the next node
  TCAPAnalyzerResultPtr result = activeConsumers.front()->analyze(tupleStageBuilder,
                                                                  newPrevNode,
                                                                  stats,
                                                                  nextStageID);

  // remove the consumer we just processed if we succeeded
  if(result->success) {
    activeConsumers.pop_front();
  }

  return result;
}

TCAPAnalyzerResultPtr SimplePhysicalNode::analyzeOutput(TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                            SimpleTCAPAnalyzerNodePtr &prevNode,
                                                            const StatisticsPtr &stats,
                                                            int nextStageID) {

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(node->getComputationName()).getComputationHandle();

  // create a SetIdentifier for the output set
  Handle<SetIdentifier> sink = makeObject<SetIdentifier>(curComp->getDatabaseName(), curComp->getSetName());

  // set the parameters
  tupleStageBuilder->setJobStageId(nextStageID);
  tupleStageBuilder->setTargetTupleSetName(node->getInputName());
  tupleStageBuilder->setTargetComputationName(node->getComputationName());
  tupleStageBuilder->setOutputTypeName(curComp->getOutputType());
  tupleStageBuilder->setSinkContext(sink);
  tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

  // create the job stage
  Handle<TupleSetJobStage> jobStage = tupleStageBuilder->build();

  // create a analyzer result
  TCAPAnalyzerResultPtr result = make_shared<TCAPAnalyzerResult>();

  // add the job stage to the result
  result->physicalPlanToOutput.emplace_back(jobStage);
  result->success = true;
  result->newSourceComputation = nullptr;

  return result;
}

TCAPAnalyzerResultPtr SimplePhysicalNode::analyzeMultipleConsumers(TupleSetJobStageBuilderPtr &tupleSetJobStageBuilder,
                                                                       SimpleTCAPAnalyzerNodePtr &prevNode,
                                                                       const StatisticsPtr &stats,
                                                                       int nextStageID) {

  // create a analyzer result
  TCAPAnalyzerResultPtr result = make_shared<TCAPAnalyzerResult>();

  // grab the output of the current node
  std::string outputName = node->getOutputName();

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(node->getComputationName()).getComputationHandle();

  // I am a pipeline breaker because I have more than one consumers
  Handle<SetIdentifier> sink = nullptr;

  // in the case that the current computation does not require materialization by default
  // we have to set an output to it, we it gets materialized
  if (!curComp->needsMaterializeOutput()) {

    // set the output
    curComp->setOutput(jobId, outputName);

    // create the sink and set the page size
    sink = makeObject<SetIdentifier>(jobId, outputName);
    sink->setPageSize(conf->getPageSize());

    // add this set to the list of intermediate sets
    result->interGlobalSets.push_back(sink);
  } else {
    // this computation needs materialization either way so just create the sink set identifier
    sink = makeObject<SetIdentifier>(curComp->getDatabaseName(), curComp->getSetName());
  }

  // set the parameters
  tupleSetJobStageBuilder->setJobStageId(nextStageID);
  tupleSetJobStageBuilder->setTargetTupleSetName(node->getInputName());
  tupleSetJobStageBuilder->setTargetComputationName(node->getComputationName());
  tupleSetJobStageBuilder->setOutputTypeName(curComp->getOutputType());
  tupleSetJobStageBuilder->setSinkContext(sink);
  tupleSetJobStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

  // create the job stage
  Handle<TupleSetJobStage> jobStage = tupleSetJobStageBuilder->build();

  // add the job stage to the result
  result->physicalPlanToOutput.emplace_back(jobStage);
  result->success = true;
  result->newSourceComputation = getHandle();

  // the new source is now the sink
  sourceSetIdentifier = sink;

  return result;
}

SimpleTCAPAnalyzerNodePtr SimplePhysicalNode::getHandle() {

  // if we do not have a handle to this node already
  if(handle == nullptr) {
    handle = std::shared_ptr<SimplePhysicalNode> (this);
  }

  // return the handle to this node
  return handle;
}

}

