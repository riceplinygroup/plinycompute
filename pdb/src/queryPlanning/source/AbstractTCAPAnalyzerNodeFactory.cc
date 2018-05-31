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
#include "AbstractTCAPAnalyzerNodeFactory.h"

namespace pdb {

AbstractTCAPAnalyzerNodeFactory::AbstractTCAPAnalyzerNodeFactory(const Handle<ComputePlan> &computePlan) : computePlan(computePlan) {
  // initialize the values we need
  this->logicalPlan = this->computePlan->getPlan();
  this->computePlan->nullifyPlanPointer();
  this->computationGraph = this->logicalPlan->getComputations();
}

std::vector<AbstractTCAPAnalyzerNodePtr> AbstractTCAPAnalyzerNodeFactory::generateAnalyzerGraph(std::vector<AtomicComputationPtr> sources) {

  std::vector<AbstractTCAPAnalyzerNodePtr> ret;

  for(const auto &source : sources) {

    auto analyzerNode = createAnalyzerNode(source);

    // store this source since we are returning it
    ret.push_back(analyzerNode);

    // store it in nodes to avoid duplicates
    nodes[source->getOutputName()] = analyzerNode;

    // for each consumer go through them
    for(const auto &consumer : computationGraph.getConsumingAtomicComputations(source->getOutputName())) {
      generateConsumerNode(analyzerNode, consumer);
    }
  }

  // clear nodes map
  nodes.clear();

  return ret;
}

void AbstractTCAPAnalyzerNodeFactory::generateConsumerNode(AbstractTCAPAnalyzerNodePtr source,
                                                           AtomicComputationPtr node) {
  AbstractTCAPAnalyzerNodePtr analyzerNode;

  // do we already have an AbstractTCAPAnalyzerNode for this node
  if(nodes.find(node->getOutputName()) == nodes.end()) {

    // create the node
    analyzerNode = createAnalyzerNode(node);

    // store the node as created
    nodes[node->getOutputName()] = analyzerNode;

    // add the current node to this source
    source->addConsumer(analyzerNode);
  }
  else {
    // grab the already exiting node
    analyzerNode = nodes[node->getOutputName()];

    // add the current node to this source
    source->addConsumer(analyzerNode);

    // we are done here
    return;
  }

  // for each consumer of the current node do the same
  for(const auto &consumer : computationGraph.getConsumingAtomicComputations(node->getOutputName())) {
    generateConsumerNode(analyzerNode, consumer);
  }
}

}
