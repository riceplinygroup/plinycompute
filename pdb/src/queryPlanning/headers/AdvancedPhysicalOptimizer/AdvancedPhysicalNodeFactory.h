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
#include "AdvancedPhysicalOptimizer/AdvancedPhysicalNode.h"
#include "AdvancedPhysicalShufflePipeNode.h"

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

  /**
   *
   * @param curNode
   */
  void transverseTCAPGraph(const AtomicComputationPtr &curNode);

 protected:

  /**
   * This method creates a shuffle pipe and adds it to the physicalNodes
   */
  void createShufflePipe();

  /**
   * This method creates a straight pipe and adds it to the physicalNodes
   */
  void createStraightPipe();

  /**
   * This method creates a aggregation pipe and adds it to the physicalNodes
   */
  void createAggregationPipe();

  /**
   * This method updates all the connections
   * @param node - the node we are updating the connections for
   */
  void updateConnections(shared_ptr<AdvancedPhysicalNode> node);

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
  std::map<std::string, AdvancedPhysicalNodePtr> physicalNodes;

  /**
   * Maps each pipe to the atomic computation it starts with.
   * The key is the name of the atomic computation the value is the pipe
   */
  std::map<std::string, AdvancedPhysicalNodePtr> startsWith;

  /**
   * Maps each pipe to the list of atomic computations that consume it
   */
  std::map<std::string, std::vector<std::string>> consumedBy;

  /**
   * All the source nodes we return them from the @see generateAnalyzerGraph
   */
  std::vector<AdvancedPhysicalNodePtr> sources;

  /**
   * After we create all the pipes we then connect them
   */
  void connectThePipes();
};

}



#endif //PDB_ADVANCEDPHYSICALNODEFACTORY_H
