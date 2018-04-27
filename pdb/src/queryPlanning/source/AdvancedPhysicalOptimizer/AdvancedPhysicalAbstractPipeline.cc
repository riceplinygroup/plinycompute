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
  if(consumers.size() == 1 && std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipeline>(consumers.front())->isPipelinable(getAdvancedPhysicalNodeHandle())) {

    // do the logic for the pipelining

  }

  /// 2. is this a final operator
  if(consumers.empty()) {
    return selectOutputAlgorithm()->generate(nextStageID);
  }

  /// 3. ok this is not pipelinable we get all the algorithms we can use and propose them to the next operators

  // TODO for now I assume I have only one consumer
  consumers.front()->propose(getPossibleAlgorithms(stats));

  return pdb::PhysicalOptimizerResultPtr();
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


