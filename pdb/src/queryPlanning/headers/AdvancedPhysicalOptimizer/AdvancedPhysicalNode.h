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
                       vector<AtomicComputationPtr> &pipeComputations,
                       size_t id);

  PhysicalOptimizerResultPtr analyze(const StatisticsPtr &stats, int nextStageID) override;

  /**
   *
   * @param stats
   * @return
   */
  double getCost(const StatisticsPtr &stats) override;

  /**
   *
   * @return
   */
  bool hasConsumers() override;

  /**
   * Returns the identifier of this node. In the format of "node_{id}"
   * @return the identifier
   */
  string getNodeIdentifier() override;

 protected:

  /**
   * Contains all the atomic computations that make-up this pipe
   */
  vector<AtomicComputationPtr> pipeComputations;

  /**
   * The identifier of this node
   */
  size_t id;
};

}


#endif //PDB_ADVANCEDPHYSICALNODE_H
