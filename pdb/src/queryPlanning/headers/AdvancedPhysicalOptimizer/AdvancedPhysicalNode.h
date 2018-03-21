//
// Created by dimitrije on 3/20/18.
//

#ifndef PDB_ADVANCEDPHYSICALNODE_H
#define PDB_ADVANCEDPHYSICALNODE_H

#include "AbstractPhysicalNode.h"

namespace pdb {

class AdvancedPhysicalNode;
typedef std::shared_ptr<AdvancedPhysicalNode> AdvancedPhysicalNodePtr;

class AdvancedPhysicalNode : public AbstractPhysicalNode {
 public:
  AdvancedPhysicalNode(string &jobId,
                       const Handle<ComputePlan> &computePlan,
                       LogicalPlanPtr &logicalPlan,
                       ConfigurationPtr &conf,
                       vector<AtomicComputationPtr> &pipeComputations);

 protected:

  /**
   * Contains all the atomic computations that make-up this pipe
   */
  vector<AtomicComputationPtr> pipeComputations;

};

}


#endif //PDB_ADVANCEDPHYSICALNODE_H
