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

#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractAlgorithm.h"

AdvancedPhysicalAbstractAlgorithm::AdvancedPhysicalAbstractAlgorithm(const AdvancedPhysicalPipelineNodePtr &handle,
                                                                     const std::string &jobID,
                                                                     bool isProbing,
                                                                     bool isOutput,
                                                                     Handle<SetIdentifier> source,
                                                                     const vector<AtomicComputationPtr> &pipeComputations,
                                                                     Handle<ComputePlan> computePlan,
                                                                     const LogicalPlanPtr &logicalPlan,
                                                                     const ConfigurationPtr &conf)
                                                                     : jobID(jobID),
                                                                       computePlan(computePlan),
                                                                       logicalPlan(logicalPlan),
                                                                       conf(conf),
                                                                       source(source),
                                                                       isProbing(isProbing),
                                                                       isOutput(isOutput) {

  // insert the pipe computations
  this->pipeComputations.insert(this->pipeComputations.begin(),
                                pipeComputations.begin(),
                                pipeComputations.end());

  // add the current node to the pipeline
  pipeline.push_back(handle);
}


PhysicalOptimizerResultPtr AdvancedPhysicalAbstractAlgorithm::generatePipelined(int nextStageID,
                                                                                const StatisticsPtr &stats,
                                                                                std::vector<AdvancedPhysicalPipelineNodePtr> &pipelines) {

  // get the source set identifier of the first node in the pipeline
  source = pipelines.front()->getSourceSetIdentifier();

  // all the computations that are in the pipeline (we need to build this)
  list<AtomicComputationPtr> pipelineComputations;

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
    pipelineComputations.insert(pipelineComputations.end(), computations.begin(), computations.end());
  }

  // insert the pipeline computations at the end
  pipelineComputations.insert(pipelineComputations.end(), pipeComputations.begin(), pipeComputations.end());

  // these are the new computations now
  pipeComputations = pipelineComputations;

  // generate the stage
  return generate(nextStageID, stats);
}

void AdvancedPhysicalAbstractAlgorithm::updateConsumers(const Handle<SetIdentifier> &sink,
                                                        DataStatistics approxSize,
                                                        const StatisticsPtr &stats) {

  // the handle always has to something else than nullptr
  assert(pipeline.back() != nullptr);

  for(auto i = 0; i < pipeline.back()->getNumConsumers(); ++i) {
    auto consumer = pipeline.back()->getConsumer(i)->to<AdvancedPhysicalAbstractPipe>();
    consumer->setSourceSetIdentifier(sink);
  }

  // update the set name
  approxSize.databaseName = sink->getDatabase();
  approxSize.setName = sink->getSetName();

  // update the stats
  if(stats != nullptr){
    stats->addSet(sink->getDatabase(), sink->getSetName(), approxSize);
  }
}

DataStatistics AdvancedPhysicalAbstractAlgorithm::approximateResultSize(const StatisticsPtr &stats) {

  // an algorithm should always have a source set
  assert(source != nullptr);

  // temp variables
  DataStatistics ds;

  // set the set stats
  ds.pageSize = stats != nullptr ? stats->getPageSize(source->getDatabase(), source->getSetName()) : 0;
  ds.numTuples = stats != nullptr ? stats->getNumTuples(source->getDatabase(), source->getSetName()) : 0;
  ds.numBytes = stats != nullptr ? stats->getNumBytes(source->getDatabase(), source->getSetName()) : 0;
  ds.avgTupleSize = stats != nullptr ? stats->getAvgTupleSize(source->getDatabase(), source->getSetName()) : 0;

  // get the size of the source set in bytes
  return ds;
}
void AdvancedPhysicalAbstractAlgorithm::includeHashComputation() {

  // if we are calling this we always must have two producers
  assert(pipeline.front()->getNumProducers() == 2);

  // grab the left side and the right side
  auto lhs = pipeline.front()->getProducer(0)->to<AdvancedPhysicalAbstractPipe>();
  auto rhs = pipeline.front()->getProducer(1)->to<AdvancedPhysicalAbstractPipe>();

  // TODO for now we are just handling two situations one side JOIN_SHUFFLED_HASHSET_ALGORITHM the other JOIN_SUFFLE_SET_ALGORITHM
  if(lhs->getSelectedAlgorithm()->getType() == JOIN_SHUFFLED_HASHSET_ALGORITHM &&
     rhs->getSelectedAlgorithm()->getType() == JOIN_SUFFLE_SET_ALGORITHM) {

    // the last computation is hash computation grab that and add to the front of our pipeline
    pipeComputations.push_front(rhs->getPipeComputations().back());
  }
  else if(rhs->getSelectedAlgorithm()->getType() == JOIN_SHUFFLED_HASHSET_ALGORITHM &&
          lhs->getSelectedAlgorithm()->getType() == JOIN_SUFFLE_SET_ALGORITHM) {

    // the last computation is hash computation grab that and add to the front of our pipeline
    pipeComputations.push_front(lhs->getPipeComputations().back());
  }
}
