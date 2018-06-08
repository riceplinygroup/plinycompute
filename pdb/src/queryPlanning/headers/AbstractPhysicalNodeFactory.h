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
#include "AbstractPhysicalNode.h"

namespace pdb {

class AbstractPhysicalNodeFactory;
typedef std::shared_ptr<AbstractPhysicalNodeFactory> AbstractPhysicalNodeFactoryPtr;

class AbstractPhysicalNodeFactory {
public:

  /**
   * This can only be called from the constructor of a class the inherits the AbstractPhysicalNodeFactory
   * @param computePlan the compute plan the nodes belong to
   */
  explicit AbstractPhysicalNodeFactory(const Handle<ComputePlan> &computePlan);

  /**
   * This method is used to generate a TCAP analyzer graph, it is recursing
   * @param sources
   * @return
   */
  virtual std::vector<AbstractPhysicalNodePtr> generateAnalyzerGraph(std::vector<AtomicComputationPtr> sources) = 0;

 protected:

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
