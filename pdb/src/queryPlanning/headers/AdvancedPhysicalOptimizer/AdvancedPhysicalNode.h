//
// Created by dimitrije on 3/20/18.
//

#ifndef PDB_ADVANCEDPHYSICALNODE_H
#define PDB_ADVANCEDPHYSICALNODE_H

#include "AbstractPhysicalNode.h"

namespace pdb {
class AdvancedPhysicalNode : public AbstractPhysicalNode {
 public:
  AdvancedPhysicalNode(string &jobId,
                       AtomicComputationPtr &node,
                       const Handle<ComputePlan> &computePlan,
                       LogicalPlanPtr &logicalPlan,
                       ConfigurationPtr &conf);

};

}


#endif //PDB_ADVANCEDPHYSICALNODE_H
