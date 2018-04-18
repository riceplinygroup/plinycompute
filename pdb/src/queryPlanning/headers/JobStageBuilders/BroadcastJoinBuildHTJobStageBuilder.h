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
#ifndef PDB_BROADCASTJOINBUILDHTJOBSTAGEBUILDER_H
#define PDB_BROADCASTJOINBUILDHTJOBSTAGEBUILDER_H

#include <SetIdentifier.h>
#include "BroadcastJoinBuildHTJobStage.h"

namespace pdb {

class BroadcastJoinBuildHTJobStageBuilder;
typedef std::shared_ptr<BroadcastJoinBuildHTJobStageBuilder> BroadcastJoinBuildHTJobStageBuilderPtr;

class BroadcastJoinBuildHTJobStageBuilder {
public:

/**
 * The id of the job this job stage belongs to
 * @param jobId - string identifier of the job
 */
  void setJobId(const std::string &jobId);

/**
 * Sets the id of this job stage
 * @param jobStageId - the id that uniquely identifies this stage within the current job
 */
  void setJobStageId(int jobStageId);

/**
 * Sets the source tuple set
 * @param sourceTupleSetSpecifier - the tuple set we use for the source
 */
  void setSourceTupleSetName(const std::string &sourceTupleSetSpecifier);

/**
 * Sets the target tuple set
 * @param targetTupleSetSpecifier - the tuple set we use for the sink
 */
  void setTargetTupleSetName(const std::string &targetTupleSetName);

/**
 * Sets the compute plan to the jobStage we are building
 * @param plan - ComputePlan generated from input computations and the input TCAP string
 */
  void setComputePlan(const Handle<ComputePlan> &computePlan);

/**
 * Sets the target computation specifier
 * @param targetComputationSpecifier - the name of the computation
 */
  void setTargetComputationName(const std::string &targetComputationName);

/**
 * Sets the set identifier by the source set
 * This is used by the @see pdb::FrontendQueryTestServer to get the info about the source set
 * @param sourceContext - the set identifier
 */
  void setSourceContext(const Handle<SetIdentifier> &sourceContext);

/**
 * The name of the hash set that is the result of this stage
 * @param hashSetName - the name
 */
  void setHashSetName(const std::string &hashSetName);

/**
 * Return the build AggregationJobStage
 * @return the AggregationJobStage
 */
  Handle<BroadcastJoinBuildHTJobStage> build();

private:
/**
* The id of the job this job stage belongs to
*/
  std::string jobId;

/**
 * The id of this job stage. It uniquely identifies this stage within this job
 */
  int jobStageId;

/**
 * The tuple set we use for the source
 */
  std::string sourceTupleSetName;

/**
 * The tuple set we use for the sink
 */
  std::string targetTupleSetName;

/**
 * The name of the computation (Join) associated with this stage
 */
  std::string targetComputationName;

/**
 * The set identifier by the source set
 * This is used by the @see FrontendQueryTestServer to get the info about the source set
 */
  Handle<SetIdentifier> sourceContext;

/**
 * The name of the hash set that is the result of this stage
 */
  std::string hashSetName;

/**
 * The ComputePlan generated from input computations and the input TCAP string
 */
  Handle<ComputePlan> computePlan;
};
}

#endif //PDB_BROADCASTJOINBUILDHTJOBSTAGEBUILDER_H
