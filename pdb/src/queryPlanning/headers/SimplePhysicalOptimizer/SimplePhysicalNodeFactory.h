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
#include "AbstractPhysicalNodeFactory.h"

#ifndef PDB_SIMPLETCAPANALYZERNODE_H
#define PDB_SIMPLETCAPANALYZERNODE_H

namespace pdb {

class SimplePhysicalNodeFactory;
typedef std::shared_ptr<SimplePhysicalNodeFactory> SimpleTCAPAnalyzerNodeFactoryPtr;

/**
 * This class is a factory for the nodes of a SimpleTCAPAnalyzer graph
 */
class SimplePhysicalNodeFactory : public AbstractPhysicalNodeFactory {
public:

  SimplePhysicalNodeFactory(const string &jobId,
                                const Handle<ComputePlan> &computePlan,
                                const ConfigurationPtr &conf);

  /**
   * Depending on the type of the tcapNode we are dealing with create the appropriate SimpleTCAPAnalyzerNode
   * Currently we are differentiating between three types of nodes :
   * 1. ApplyAggTypeID -> SimpleTCAPAnalyzerAggregationNode
   * 2. ApplyJoinTypeID -> SimpleTCAPAnalyzerJoinNode
   * 3. Any other node -> SimpleTCAPAnalyzerNode
   *
   * @param tcapNode the TCAP node we are analyzing
   * @return the created node
   */
  AbstractTCAPAnalyzerNodePtr createAnalyzerNode(AtomicComputationPtr tcapNode) override;


private:

  /**
   * This method is used to generate a TCAP analyzer graph, it is recursing
   * @param sources
   * @return
   */
  std::vector<AbstractTCAPAnalyzerNodePtr> generateAnalyzerGraph(std::vector<AtomicComputationPtr> sources) override;

  /**
   * This method generates the node that is consuming this source and adds it to the list of its consumers
   * @param source - the source AbstractTCAPAnalyzerNode we are coming from
   * @param node - the AtomicComputation from which we are going to create the consumer node
   */
  void generateConsumerNode(AbstractTCAPAnalyzerNodePtr source, AtomicComputationPtr node);

  /**
   * This map is used to store nodes we created to avoid creating duplicates.
   * The key is the outputName of the AtomicComputation the value is the created node.
   */
  std::map<std::string, AbstractTCAPAnalyzerNodePtr> nodes;


  /**
   * The id of the job we are trying to generate a physical plan for
   */
  std::string jobId;

  /**
   * A configuration object for this cluster node
   */
  ConfigurationPtr conf;

};

}


#endif //PDB_SIMPLETCAPANALYZERNODE_H
