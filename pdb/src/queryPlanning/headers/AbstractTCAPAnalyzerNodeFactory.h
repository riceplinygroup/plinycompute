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
#ifndef PDB_ABSTRACTPHYSICALNODEFACTORY_H
#define PDB_ABSTRACTPHYSICALNODEFACTORY_H

#include <memory>
#include "AtomicComputation.h"
#include "AbstractTCAPAnalyzerNode.h"

namespace pdb {

class AbstractTCAPAnalyzerNodeFactory;
typedef std::shared_ptr<AbstractTCAPAnalyzerNodeFactory> AbstractTCAPAnalyzerNodeFactoryPtr;

class AbstractTCAPAnalyzerNodeFactory {
public:

  /**
   * This can only be called from the constructor of a class the inherits the AbstractTCAPAnalyzerNodeFactory
   * @param computePlan the compute plan the nodes belong to
   */
  explicit AbstractTCAPAnalyzerNodeFactory(const Handle<ComputePlan> &computePlan);

  /**
   * Takes in an AtomicComputation and creates a TCAPAnalyzerNode out of it.
   * @param tcapNode the AtomicComputation
   * @return the TCAPAnalyzerNode that corresponds to the AtomicComputation
   */
  virtual AbstractTCAPAnalyzerNodePtr createAnalyzerNode(AtomicComputationPtr tcapNode) = 0;

  /**
   * This method is used to generate a TCAP analyzer graph, it is recursing
   * @param sources
   * @return
   */
  std::vector<AbstractTCAPAnalyzerNodePtr> generateAnalyzerGraph(std::vector<AtomicComputationPtr> sources);

 protected:

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
   * The compute plan we are using
   */
  Handle<ComputePlan> computePlan;

  /**
   * Logical plan generated from the compute plan
   */
  LogicalPlanPtr logicalPlan;

  /**
   * The computation graph generated from the logical plan
   */
  AtomicComputationList computationGraph;
};

}

#endif //PDB_ABSTRACTPHYSICALNODEFACTORY_H
