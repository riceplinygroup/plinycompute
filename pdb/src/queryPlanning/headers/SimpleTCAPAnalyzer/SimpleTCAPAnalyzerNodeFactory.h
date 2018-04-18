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

#ifndef PDB_SIMPLETCAPANALYZERNODE_H
#define PDB_SIMPLETCAPANALYZERNODE_H

namespace pdb {

class SimpleTCAPAnalyzerNodeFactory;
typedef std::shared_ptr<SimpleTCAPAnalyzerNodeFactory> SimpleTCAPAnalyzerNodeFactoryPtr;

/**
 * This class is a factory for the nodes of a SimpleTCAPAnalyzer graph
 */
class SimpleTCAPAnalyzerNodeFactory : public AbstractTCAPAnalyzerNodeFactory {
public:

  SimpleTCAPAnalyzerNodeFactory(const string &jobId,
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
