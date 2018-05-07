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

#ifndef PDB_ADVANCEDPHYSICALNODEFACTORY_H
#define PDB_ADVANCEDPHYSICALNODEFACTORY_H

#include <set>
#include "AbstractPhysicalNodeFactory.h"
#include "AdvancedPhysicalOptimizer/AdvancedPhysicalAbstractPipe.h"

namespace pdb {

class AdvancedPhysicalNodeFactory : public AbstractPhysicalNodeFactory {

 public:

  AdvancedPhysicalNodeFactory(const string &jobId,
                              const Handle<ComputePlan> &computePlan,
                              const ConfigurationPtr &conf);

  /**
   *
   * @param sources
   * @return
   */
  vector<AbstractPhysicalNodePtr> generateAnalyzerGraph(std::vector<AtomicComputationPtr> sources) override;

 protected:

  /**
   *
   * @param curNode
   */
  void transverseTCAPGraph(const AtomicComputationPtr &curNode);

  /**
   * This method updates the @see consumedBy for the node we provide.
   * This method assumes that the last AtomicComputation belonging to this node is stored at @see currentPipe
   * @param node - the node we are updating the consumedBy for
   */
  void setConsumers(shared_ptr<AdvancedPhysicalAbstractPipe> node);

  /**
   * After we create all the pipes we we need to connect them to create a graph consisting of pipes
   */
  void connectThePipes();

  /**
   * This method creates a straight pipe and adds it to the physicalNodes
   */
  template <class T>
  void createPhysicalPipeline() {

    // this must never be empty
    assert(!currentPipe.empty());

    // create the node
    auto node = new T(jobId, computePlan, logicalPlan, conf, currentPipe, currentNodeIndex++);

    // create the node handle
    auto nodeHandle = node->getAdvancedPhysicalNodeHandle();

    // update all the node connections
    setConsumers(nodeHandle);

    // is this a source node
    if(nodeHandle->isSource()) {

      // add the source node
      physicalSourceNodes.push_back(nodeHandle);
    }

    for(auto &c : currentPipe) {
      std::cout << c->getOutputName() << std::endl;
    }

    std::cout << "-----------------------------------------" << std::endl;


    // add the starts with
    startsWith[currentPipe.front()->getOutputName()] = nodeHandle;

    // add the pipe to the physical nodes
    physicalNodes[nodeHandle->getNodeIdentifier()] = nodeHandle;
  }

  /**
   * The id of the job we are trying to generate a physical plan for
   */
  std::string jobId;

  /**
   * The current node index
   */
  size_t currentNodeIndex;

  /**
   * A configuration object for this cluster node
   */
  ConfigurationPtr conf;

  /**
   * All the nodes we already visited
   */
  std::set<AtomicComputationPtr> visitedNodes;

  /**
   * All the nodes that are in the current pipeline
   */
  std::vector<AtomicComputationPtr> currentPipe;

  /**
   * The physical nodes we created
   */
  std::map<std::string, AdvancedPhysicalPipelineNodePtr> physicalNodes;

  /**
   * Source physical nodes we created
   */
  std::vector<AbstractPhysicalNodePtr> physicalSourceNodes;

  /**
   * Maps each pipe to the atomic computation it starts with.
   * The key is the name of the atomic computation the value is the pipe
   */
  std::map<std::string, AdvancedPhysicalPipelineNodePtr> startsWith;

  /**
   * Maps each pipe to the list of atomic computations that consume it
   */
  std::map<std::string, std::vector<std::string>> consumedBy;

  /**
   * All the source nodes we return them from the @see generateAnalyzerGraph
   */
  std::vector<AdvancedPhysicalPipelineNodePtr> sources;
};

}



#endif //PDB_ADVANCEDPHYSICALNODEFACTORY_H
