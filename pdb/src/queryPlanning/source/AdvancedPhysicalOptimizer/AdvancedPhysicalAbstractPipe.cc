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

#include <AdvancedPhysicalOptimizer/Pipes/AdvancedPhysicalJoinSidePipe.h>
#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractPipe.h"
#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractAlgorithm.h"

namespace pdb {

AdvancedPhysicalAbstractPipe::AdvancedPhysicalAbstractPipe(string &jobId,
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

PhysicalOptimizerResultPtr AdvancedPhysicalAbstractPipe::analyze(const StatisticsPtr &stats, int nextStageID) {


  /// 1. check if this this thing is pipelinable to the consumer
  if(consumers.size() == 1 && consumers.front()->to<AdvancedPhysicalAbstractPipe>()->isPipelinable(getAdvancedPhysicalNodeHandle())) {

    // we start with pipelining this pipeline maybe we will pipeline more
    std::vector<AdvancedPhysicalPipelineNodePtr> pipelines = { getAdvancedPhysicalNodeHandle() };

    // delegate the logic for the pipelining to the next node
    return consumers.front()->to<AdvancedPhysicalAbstractPipe>()->pipelineMe(nextStageID, pipelines, stats);
  }

  /// 2. is this a final operator
  if(consumers.empty()) {
    return selectOutputAlgorithm()->generate(nextStageID, stats);
  }

  /// 3. ok this is not pipelinable we get all the algorithms we can use and propose them to the next operators
  // TODO for now I assume I have only one consumer
  selectedAlgorithm = consumers.front()->to<AdvancedPhysicalAbstractPipe>()->propose(getPossibleAlgorithms(stats));

  /// 4. should we chain
  if(consumers.size() == 1 && isChainable()) {

    // chain this thing to the next pipe
    return consumers.front()->to<AdvancedPhysicalAbstractPipe>()->chainMe(nextStageID,
                                                                          stats,
                                                                          selectedAlgorithm->generate(nextStageID, stats));
  }

  /// 5. ok no chaining we simply generate the stages from the algorithm
  return selectedAlgorithm->generate(nextStageID, stats);
}

bool AdvancedPhysicalAbstractPipe::isPipelinable(AdvancedPhysicalPipelineNodePtr node) {

  // check whether we are joining
  if(this->isJoining()) {

    // if by any case the child of a join is not a join side pipeline assert
    assert(node->getType() == JOIN_SIDE);

    // for each side of the join that is not the one we are coming from we check if it is processed
    for(auto &tmp : producers) {

      // convert the weak pointer to a shared pointer
      AbstractPhysicalNodePtr producer(tmp);

      // if on of the producers is not a join side fail
      assert(std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipe>(producer)->getType() == JOIN_SIDE);

      // cast the consumer to the join
      auto casted_producer = std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipe>(producer);

      // if the consumer is not processed this operator is not pipelinable if
      // A) this operator is not executed or B) We shuffled the other side so now we have to shuffle this side
      if(node != producer && (!casted_producer->isExecuted() || casted_producer->getSelectedAlgorithm()->getType() == JOIN_SHUFFLED_HASHSET_ALGORITHM)) {

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


PhysicalOptimizerResultPtr AdvancedPhysicalAbstractPipe::pipelineMe(int nextStageID,
                                                                    std::vector<AdvancedPhysicalPipelineNodePtr> pipeline,
                                                                    const StatisticsPtr &stats) {

  /// 1. can I pipeline more if so do it
  if(consumers.size() == 1 && consumers.front()->to<AdvancedPhysicalAbstractPipe>()->isPipelinable(getAdvancedPhysicalNodeHandle())) {

    // add me to the pipeline
    pipeline.push_back(getAdvancedPhysicalNodeHandle());

    // pipeline this node to the consumer
    return consumers.front()->to<AdvancedPhysicalAbstractPipe>()->pipelineMe(nextStageID, pipeline, stats);
  }

  /// 2. is this a final operator we can not pipeline lets select the output algorithm and run this thing
  if(consumers.empty()) {
    return selectOutputAlgorithm()->generatePipelined(nextStageID, stats, pipeline);
  }

  /// 3. ok this is not pipelinable we get all the algorithms we can use and propose them to the next operators
  // TODO for now I assume I have only one consumer
  selectedAlgorithm = consumers.front()->to<AdvancedPhysicalAbstractPipe>()->propose(getPossibleAlgorithms(stats));

  /// 4. should we chain
  if(consumers.size() == 1 && isChainable()) {

    // chain this thing to the next pipe
    return consumers.front()->to<AdvancedPhysicalAbstractPipe>()->chainMe(nextStageID,
                                                                          stats,
                                                                          selectedAlgorithm->generate(nextStageID, stats));
  }

  /// 5. ok no chaining we simply generate the stages from the algorithm
  return selectedAlgorithm->generatePipelined(nextStageID, stats, pipeline);
}

PhysicalOptimizerResultPtr AdvancedPhysicalAbstractPipe::chainMe(int nextStageID,
                                                                 const StatisticsPtr &stats,
                                                                 PhysicalOptimizerResultPtr previous) {

  // analyze me and get my result the stage ID is not the old one plus the number of previously created stage IDs
  auto current = analyze(stats, nextStageID + (int) previous->physicalPlanToOutput.size());

  // append the
  current->physicalPlanToOutput.insert(current->physicalPlanToOutput.begin(),
                                       previous->physicalPlanToOutput.begin(),
                                       previous->physicalPlanToOutput.end());

  // a source should always exist
  assert(sourceSetIdentifier != nullptr);

  // if the if we created a new source se
  current->interGlobalSets.emplace_front(sourceSetIdentifier);

  // both have to be successful
  current->success = current->success && previous->success;

  return current;
}

bool AdvancedPhysicalAbstractPipe::isChainable() {

  // currently we only chain the shuffle set algorithm to the join algorithm
  return selectedAlgorithm->getType() == JOIN_SUFFLE_SET_ALGORITHM;
}

bool AdvancedPhysicalAbstractPipe::isExecuted() {
  return selectedAlgorithm != nullptr;
}

const bool AdvancedPhysicalAbstractPipe::isJoining() {
  return producers.size() >= 2;
}

const bool AdvancedPhysicalAbstractPipe::isAggregating() {

  // go through each computation in this pipe
  for(auto &it : pipeComputations) {

    // returns true if this computation is an aggregation
    if(it->getAtomicComputationTypeID() == ApplyAggTypeID){
      return true;
    }
  }

  // of we could not find it return false
  return false;
}

double AdvancedPhysicalAbstractPipe::getCost(const StatisticsPtr &stats) {

  // do we have statistics, if not just return 0
  if(stats == nullptr) {
    return 0;
  }

  // check if the source set identifier exists if it does not maybe this pipeline is a join therefore we need to
  // get the source set from one if it's children
  if (sourceSetIdentifier == nullptr) {
    // this might be a problem
    PDB_COUT << "WARNING: there is no source set for the node " << getNodeIdentifier() << "\n";
    return 0;
  }

  // calculate the cost based on the formula cost = number_of_bytes / 1000000
  double cost = stats->getNumBytes(sourceSetIdentifier->getDatabase(), sourceSetIdentifier->getSetName());
  return double((size_t) cost / 1000000);
}

const AdvancedPhysicalAbstractAlgorithmPtr &AdvancedPhysicalAbstractPipe::getSelectedAlgorithm() const {
  return selectedAlgorithm;
}

AdvancedPhysicalPipelineNodePtr AdvancedPhysicalAbstractPipe::getAdvancedPhysicalNodeHandle() {
  // return the handle to this node
  return std::dynamic_pointer_cast<AdvancedPhysicalAbstractPipe>(getHandle());
}

bool AdvancedPhysicalAbstractPipe::hasConsumers() {
  // TODO this needs to be implemeted properly
  return false;
}

bool AdvancedPhysicalAbstractPipe::isConsuming(Handle<SetIdentifier> &set) {
  return *sourceSetIdentifier == *set;
}

string AdvancedPhysicalAbstractPipe::getNodeIdentifier() {
  return "node_" + to_string(id);
}

bool AdvancedPhysicalAbstractPipe::isSource() {

  // check whether the first node is a scan set
  return !pipeComputations.empty() && pipeComputations.front()->getAtomicComputationTypeID() == ScanSetAtomicTypeID;
}

AtomicComputationPtr AdvancedPhysicalAbstractPipe::getPipelineComputationAt(size_t idx) {
  return this->pipeComputations[idx];
}
const vector<AtomicComputationPtr> &AdvancedPhysicalAbstractPipe::getPipeComputations() const {
  return pipeComputations;
}

const Handle<SetIdentifier> &AdvancedPhysicalAbstractPipe::getSourceSetIdentifier() const {
  return sourceSetIdentifier;
}

AdvancedPhysicalAbstractAlgorithmPtr AdvancedPhysicalAbstractPipe::propose(std::vector<AdvancedPhysicalAbstractAlgorithmPtr> algorithms) {

  AdvancedPhysicalAbstractAlgorithmPtr best = nullptr;

  // if we are joining then we have a few extra rules when choosing the algorithm
  if(isJoining()) {

    // get the lhs
    auto lhs = producers.front().lock()->to<AdvancedPhysicalAbstractPipe>();

    // grab the rhs
    auto rhs = producers.back().lock()->to<AdvancedPhysicalAbstractPipe>();

    // if we have executed the right side or the left side with a broadcast join a we are here something is wrong
    assert(!(lhs->isExecuted() && lhs->getSelectedAlgorithm()->getType() == JOIN_BROADCASTED_HASHSET_ALGORITHM));
    assert(!(rhs->isExecuted() && rhs->getSelectedAlgorithm()->getType() == JOIN_BROADCASTED_HASHSET_ALGORITHM));

    // if both the left and the right side are not executed then we just go though the algorithms
    // if we can do a broadcast we do it otherwise we just select any of them
    // TODO this is just placeholder logic
    if(!lhs->isExecuted() && !rhs->isExecuted()) {

      // go through each algorithm if we have a broad cast algorithm we chose it always
      for (const auto &algorithm : algorithms) {

        // we prefer the broadcast algorithm, but if we have none we are fine we just select any
        if (algorithm->getType() == JOIN_BROADCASTED_HASHSET_ALGORITHM || best == nullptr) {

          // select the best algorithm
          best = algorithm;
        }
      }

      // if this is false there is something seriously wrong with our system
      assert(best != nullptr);

      // return the best
      return best;
    }

    // at this point we know that at least one of the sides is executed
    // TODO this is just placeholder logic
    auto otherAlgorithm = lhs->isExecuted() ? lhs->getSelectedAlgorithm() : rhs->getSelectedAlgorithm();

    // the must be an algorithm selected for the other side if it is executed
    assert(otherAlgorithm != nullptr);

    // did we shuffle the other side, if we did then find the
    if(otherAlgorithm->getType() == JOIN_SHUFFLED_HASHSET_ALGORITHM) {

      // go through each algorithm if we find the join shuffle set algorithm return it
      for (const auto &algorithm : algorithms) {
        // we prefer the broadcast algorithm, but if we have none we are fine
        if (algorithm->getType() == JOIN_SUFFLE_SET_ALGORITHM) {

          return algorithm;
        }
      }
    }

    // this should not happen
    assert(false);
  }

  // just pick the first on there at this stage this can only be an aggregation or a pipeline algorithm
  return algorithms.front();
}

void AdvancedPhysicalAbstractPipe::setSourceSetIdentifier(const Handle<SetIdentifier> &sourceSetIdentifier) {
  AdvancedPhysicalAbstractPipe::sourceSetIdentifier = sourceSetIdentifier;
}

std::unordered_map<std::string, std::string> AdvancedPhysicalAbstractPipe::getProbingHashSets() {

  // the return value
  std::unordered_map<std::string, std::string> ret;

  for(const auto &p : producers) {

    // grab the join side
    auto joinSide = p.lock()->to<AdvancedPhysicalJoinSidePipe>();

    // if this is executed and it has a hash set
    if(joinSide->isExecuted() && joinSide->hasHashSet()) {

      // add the hash set
      ret[this->getPipelineComputationAt(0)->getOutputName()] = (joinSide->getGeneratedHashSet());
    }
  }

  return ret;
}

}