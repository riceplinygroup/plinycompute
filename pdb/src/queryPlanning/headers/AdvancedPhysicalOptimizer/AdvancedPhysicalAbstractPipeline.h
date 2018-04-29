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

#ifndef PDB_ADVANCEDPHYSICALNODE_H
#define PDB_ADVANCEDPHYSICALNODE_H

#include "AbstractPhysicalNode.h"

namespace pdb {

/**
 * Forward declaration of the AdvancedPhysicalAbstractAlgorithmPtr
 */
class AdvancedPhysicalAbstractAlgorithm;
typedef std::shared_ptr<AdvancedPhysicalAbstractAlgorithm> AdvancedPhysicalAbstractAlgorithmPtr;

/**
 * We define AdvancedPhysicalAbstractPipelinePtr so we don not have to write the whole thing
 */
class AdvancedPhysicalAbstractPipeline;
typedef std::shared_ptr<AdvancedPhysicalAbstractPipeline> AdvancedPhysicalPipelineNodePtr;

/**
 * The possible types of the pipelines in this algorithms
 */
enum AdvancedPhysicalPipelineTypeID {
  STRAIGHT,
  JOIN_SIDE,
  AGGREGATION
};

/**
 * The base node of the advanced physical planning algorithm
 * The whole planning starts with the analyze method @see analyze
 */
class AdvancedPhysicalAbstractPipeline : public AbstractPhysicalNode {
public:

  AdvancedPhysicalAbstractPipeline(string &jobId,
                                   const Handle<ComputePlan> &computePlan,
                                   LogicalPlanPtr &logicalPlan,
                                   ConfigurationPtr &conf,
                                   vector<AtomicComputationPtr> &pipeComputations,
                                   size_t id);

  /**
   *
   * @param stats
   * @param nextStageID
   * @return
   */
  PhysicalOptimizerResultPtr analyze(const StatisticsPtr &stats, int nextStageID) override;


  /**
   * Pipelines the provided pipeline to this pipeline
   * @param pipeline - the pipeline we are providing
   * @return the result of the pipelining
   */
  virtual PhysicalOptimizerResultPtr pipelineMe(int nextStageID,
                                                std::vector<AdvancedPhysicalPipelineNodePtr> pipeline,
                                                const StatisticsPtr &stats);

  /**
   * Returns true if this is pipelinable
   * @param node
   * @return
   */
  virtual bool isPipelinable(AdvancedPhysicalPipelineNodePtr node);

  /**
   * If this operator is executed returns true false otherwise.
   * @return  true if it is false otherwise
   */
  virtual bool isExecuted() = 0;

  /**
   * Returns the type of this pipeline
   * @return the type
   */
  virtual AdvancedPhysicalPipelineTypeID getType() = 0;

  /**
   * Selects the output algorithm for the output of this pipeline
   * @return selects the output algorithm for this pipeline
   */
  virtual AdvancedPhysicalAbstractAlgorithmPtr selectOutputAlgorithm() = 0;

  /**
   * Returns all the possible algorithms that can be used to execute the pipeline
   * @return a vector of possible algorithms
   */
  virtual std::vector<AdvancedPhysicalAbstractAlgorithmPtr> getPossibleAlgorithms(const StatisticsPtr &stats) = 0;

  /**
   * Out of all algorithms given to him this node picks one a proposes it
   * @param algorithms - the algorithms that are available
   * @return the picked algorithm
   */
  virtual AdvancedPhysicalAbstractAlgorithmPtr propose(std::vector<AdvancedPhysicalAbstractAlgorithmPtr> algorithms) = 0;


  /**
   * Returns true if this pipeline is joining two sets
   * @return true if it does false otherwise
   */
  const bool isJoining();

  /**
   * Returns the the algorithm the we executed. If we did not executed any algorithm we return a null_ptr
   * @return the algorithm if we have executed the pipeline, null_ptr otherwise
   */
  const AdvancedPhysicalAbstractAlgorithmPtr &getSelectedAlgorithm() const;

  /**
   * Return the cost of the pipeline based on the source operator cost
   * the : formula cost = number_of_bytes / 1000000
   * @param stats - the statistics a about the sets
   * @return the cost - the cost
   */
  double getCost(const StatisticsPtr &stats) override;

  /**
   * Returns true if this node is a source (starts with a ScanSet)
   * @return true if does false otherwise
   */
  bool isSource();

  /**
   * Does this pipeline have consumers
   * @return true if it does false otherwise
   */
  bool hasConsumers() override;

  /**
   * Returns the identifier of this node. In the format of "node_{id}"
   * @return the identifier
   */
  string getNodeIdentifier() override;

  /**
   * Return the handle to this object
   * @return the handle
   */
  AdvancedPhysicalPipelineNodePtr getAdvancedPhysicalNodeHandle();


  /**
   * Get i-th atomic computation in this pipeline
   * @param index
   * return the requested atomic computation
   */
  AtomicComputationPtr getPipelineComputationAt(size_t idx);

 protected:

  /**
   * The algorithm we selected to execute this computation
   * This is not null if it is executed false otherwise
   */
  AdvancedPhysicalAbstractAlgorithmPtr selectedAlgorithm;

  /**
   * Contains all the atomic computations that make-up this pipe
   */
  vector<AtomicComputationPtr> pipeComputations;

  /**
   * The identifier of this node
   */
  size_t id;

  /**
   * Source set associated with this node.
   */
  Handle<SetIdentifier> sourceSetIdentifier;
};

}


#endif //PDB_ADVANCEDPHYSICALNODE_H
