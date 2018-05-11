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

#include "AdvancedPhysicalOptimizer/AdvancedPhysicalNodeFactory.h"
#include "AdvancedPhysicalOptimizer/Pipes/AdvancedPhysicalAggregationPipe.h"
#include "AdvancedPhysicalOptimizer/Pipes/AdvancedPhysicalJoinSidePipe.h"
#include "AdvancedPhysicalOptimizer/Pipes/AdvancedPhysicalStraightPipe.h"

namespace pdb {

AdvancedPhysicalNodeFactory::AdvancedPhysicalNodeFactory(const string &jobId,
                                                         const Handle<ComputePlan> &computePlan,
                                                         const ConfigurationPtr &conf)
    : AbstractPhysicalNodeFactory(computePlan), jobId(jobId), conf(conf), currentNodeIndex(0) {}


vector<AbstractPhysicalNodePtr> AdvancedPhysicalNodeFactory::generateAnalyzerGraph(std::vector<AtomicComputationPtr> sources) {

  // go through each source in the sources
  for(const AtomicComputationPtr &source : sources) {

    std::cout << source->getAtomicComputationType() << std::endl;

    // go trough each consumer of this node
    for(const auto &consumer : computationGraph.getConsumingAtomicComputations(source->getOutputName())) {

      // we start with a source so we push that back
      currentPipe.push_back(source);

      // add the consumer to the pipe
      currentPipe.push_back(consumer);

      // then we start transversing the graph upwards
      transverseTCAPGraph(consumer);
    }
  }

  // connect the pipes
  connectThePipes();

  // return the generated source nodes
  return this->physicalSourceNodes;
}

void AdvancedPhysicalNodeFactory::transverseTCAPGraph(AtomicComputationPtr curNode) {

  // did we already visit this node
  if(visitedNodes.find(curNode) != visitedNodes.end()) {

    // clear the pipe we are done here
    currentPipe.clear();

    // we are done here
    return;
  }

  // ok now we visited this node
  visitedNodes.insert(curNode);

  // check the type of this node might be a pipeline breaker
  switch (curNode->getAtomicComputationTypeID()) {

    case HashOneTypeID:
    case HashLeftTypeID:
    case HashRightTypeID: {

      // we got a hash operation, create a shuffle pipe
      createPhysicalPipeline<AdvancedPhysicalJoinSidePipe>();
      currentPipe.clear();

      break;
    }
    case ApplyAggTypeID: {

      // we got a aggregation so we need to create an aggregation shuffle pipe
      createPhysicalPipeline<AdvancedPhysicalAggregationPipe>();
      currentPipe.clear();

      break;
    }
    case WriteSetTypeID: {

      // write set also breaks the pipe because this is where the pipe ends
      createPhysicalPipeline<AdvancedPhysicalStraightPipe>();
      currentPipe.clear();
    }
    default: {

      // we only care about these since they tend to be pipeline breakers
      break;
    }
  }

  // grab all the consumers
  auto consumers = computationGraph.getConsumingAtomicComputations(curNode->getOutputName());

  // if we have multiple consumers and there is still stuff left in the pipe
  if(consumers.size() > 1 && !currentPipe.empty()) {

    // this is a pipeline breaker create a pipe
    //currentPipe.push_back(curNode);
    createPhysicalPipeline<AdvancedPhysicalStraightPipe>();
    currentPipe.clear();
  }

  // go through each consumer and transverse to get the next pipe
  for(auto &consumer : consumers) {
    currentPipe.push_back(consumer);
    transverseTCAPGraph(consumer);
  }
}

void AdvancedPhysicalNodeFactory::setConsumers(shared_ptr<AdvancedPhysicalAbstractPipe> node) {

  // all the consumers of these pipes
  std::vector<std::string> consumers;

  // go trough each consumer of this node
  for(const auto &consumer : computationGraph.getConsumingAtomicComputations(this->currentPipe.back()->getOutputName())) {

    // add them to the consumers
    consumers.push_back(consumer->getOutputName());
  }

  // set the consumers
  if(!consumers.empty()) {
    this->consumedBy[node->getNodeIdentifier()] = consumers;
  }
}

void AdvancedPhysicalNodeFactory::connectThePipes() {

  for(auto node : physicalNodes) {

    // get all the consumers of this pipe
    auto consumingAtomicComputation = consumedBy[node.second->getNodeIdentifier()];

    // go through each at
    for(const auto &atomicComputation : consumingAtomicComputation) {

      std::cout << atomicComputation << std::endl;
      // get the consuming pipeline
      auto consumer = startsWith[atomicComputation];

      // add the consuming node of this guy
      node.second->addConsumer(consumer);
    }
  }
}

}