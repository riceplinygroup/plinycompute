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

#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractPipeline.h"
#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractAlgorithm.h"

namespace pdb {

AdvancedPhysicalAbstractPipeline::AdvancedPhysicalAbstractPipeline(string &jobId,
                                           const Handle<ComputePlan> &computePlan,
                                           LogicalPlanPtr &logicalPlan,
                                           ConfigurationPtr &conf,
                                           vector<AtomicComputationPtr> &pipeComputations,
                                           size_t id) : AbstractPhysicalNode(jobId,
                                                                             computePlan,
                                                                             logicalPlan,
                                                                             conf),
                                                        pipeComputations(pipeComputations),
                                                        selectedAlgorithm(nullptr),
                                                        id(id) {

  // if this node is a scan set we want to create a set identifier for it
  if(isSource()) {

    // get the name of the computation
    auto computationName = pipeComputations.front()->getComputationName();

    // grab the computation
    Handle<Computation> comp = logicalPlan->getNode(computationName).getComputationHandle();

    // create a set identifier from it
    sourceSetIdentifier = getSetIdentifierFromComputation(comp);
  }
}

PhysicalOptimizerResultPtr AdvancedPhysicalAbstractPipeline::analyze(const StatisticsPtr &stats, int nextStageID) {


  /// 1. check if this this thing is pipelinable to the consumer
  if(consumers.size() == 1 && consumers.front()->to<AdvancedPhysicalAbstractPipeline>()->isPipelinable(getAdvancedPhysicalNodeHandle())) {

    // we start with pipelining this pipeline maybe we will pipeline more
    std::vector<AdvancedPhysicalPipelineNodePtr> pipelines = { getAdvancedPhysicalNodeHandle() };

    // delegate the logic for the pipelining to the next node
    return consumers.front()->to<AdvancedPhysicalAbstractPipeline>()->pipelineMe(nextStageID, pipelines);
  }

  /// 2. is this a final operator
  if(consumers.empty()) {
    return selectOutputAlgorithm()->generate(nextStageID);
  }

  /// 3. ok this is not pipelinable we get all the algorithms we can use and propose them to the next operators
  // TODO for now I assume I have only one consumer
  return consumers.front()->to<AdvancedPhysicalAbstractPipeline>()->propose(getPossibleAlgorithms(stats))->generate(nextStageID);
}

bool AdvancedPhysicalAbstractPipeline::isPipelinable(AdvancedPhysicalPipelineNodePtr node) {

  // check whether we are joining
  if(this->isJoining()) {

    // if by any case the child of a join is not a join side pipeline assert
    assert(node->getType() == JOIN_SIDE);

    // for each side of the join that is not the one we are coming from we check if it is processed
    for(auto &producer : producers) {

      // if on of the producers is not a join side fail
      assert(std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipeline>(producer)->getType() == JOIN_SIDE);

      // cast the consumer to the join
      auto casted_producer = std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipeline>(producer);

      // if the consumer is not processed this operator is not pipelinable if
      // A) this operator is not executed or B) We shuffled the other side so now we have to shuffle this side
      if(node != producer && (!casted_producer->isExecuted() || casted_producer->getSelectedAlgorithm()->getType() == JOIN_HASH_ALGORITHM)) {

        // ok we can not pipeline this
        return false;
      }
    }

    // we are fine to pipeline this join
    return true;
  }

  // if the join side is the producer of a pipeline that is not joining something is seriously wrong
  assert(node->getType() != JOIN_SIDE);

  // only a straight pipeline can be pipelined
  return node->getType() == STRAIGHT;
}


PhysicalOptimizerResultPtr AdvancedPhysicalAbstractPipeline::pipelineMe(int nextStageID, std::vector<AdvancedPhysicalPipelineNodePtr> pipeline) {

  // can I pipeline more if so do it
  if(consumers.size() == 1 && consumers.front()->to<AdvancedPhysicalAbstractPipeline>()->isPipelinable(getAdvancedPhysicalNodeHandle())) {

    // add me to the pipeline
    pipeline.push_back(getAdvancedPhysicalNodeHandle());

    // pipeline this node to the consumer
    consumers.front()->to<AdvancedPhysicalAbstractPipeline>()->pipelineMe(nextStageID, pipeline);
  }

  // ok we can not pipeline lets select the output algorithm and run this thing
  return selectOutputAlgorithm()->generatePipelined(nextStageID, pipeline);
}

const bool AdvancedPhysicalAbstractPipeline::isJoining() {
  return producers.size() >= 2;
}

double AdvancedPhysicalAbstractPipeline::getCost(const StatisticsPtr &stats) {

  // do we have statistics, if not just return 0
  if(stats == nullptr) {
    return 0;
  }

  // if the set identifier does not exist log that
  if (sourceSetIdentifier == nullptr) {
    PDB_COUT << "WARNING: there is no source set for key=" << sourceSetIdentifier->toSourceSetName() << "\n";
    return 0;
  }

  // calculate the cost based on the formula cost = number_of_bytes / 1000000
  double cost = stats->getNumBytes(sourceSetIdentifier->getDatabase(), sourceSetIdentifier->getSetName());
  return double((size_t) cost / 1000000);
}

const AdvancedPhysicalAbstractAlgorithmPtr &AdvancedPhysicalAbstractPipeline::getSelectedAlgorithm() const {
  return selectedAlgorithm;
}

AdvancedPhysicalPipelineNodePtr AdvancedPhysicalAbstractPipeline::getAdvancedPhysicalNodeHandle() {
  // return the handle to this node
  return std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipeline>(getHandle());
}

bool AdvancedPhysicalAbstractPipeline::hasConsumers() {
  return false;
}

string AdvancedPhysicalAbstractPipeline::getNodeIdentifier() {
  return "node_" + to_string(id);
}

bool AdvancedPhysicalAbstractPipeline::isSource() {

  // check whether the first node is a scan set
  return !pipeComputations.empty() && pipeComputations.front()->getAtomicComputationTypeID() == ScanSetAtomicTypeID;
}

AtomicComputationPtr AdvancedPhysicalAbstractPipeline::getPipelineComputationAt(size_t idx) {
  return this->pipeComputations[idx];
}


}


