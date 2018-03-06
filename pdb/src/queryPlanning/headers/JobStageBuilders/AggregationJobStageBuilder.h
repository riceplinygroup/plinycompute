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
#ifndef PDB_AGGREGATIONJOBSTAGEBUILDER_H
#define PDB_AGGREGATIONJOBSTAGEBUILDER_H

#include "AggregationJobStage.h"
#include "SetIdentifier.h"
#include "AbstractAggregateComp.h"

namespace pdb {

class AggregationJobStageBuilder;
typedef std::shared_ptr<AggregationJobStageBuilder> AggregationJobStageBuilderPtr;

class AggregationJobStageBuilder {
public:

  AggregationJobStageBuilder();

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
 * The computation associated with this aggregation
 * @param aggComp - an instance of the AbstractAggregateComp
 */
  void setAggComp(const Handle<AbstractAggregateComp> &aggComp);

/**
 * Sets the set identifier by the source set
 * This is used by the @see FrontendQueryTestServer to get the info about the source set
 * @param sourceContext - the set identifier
 */
  void setSourceContext(const Handle<SetIdentifier> &sourceContext);

/**
 * Sets the set identifier of the output
 * This is used by the @see FrontendQueryTestServer to create the output set
 * @param sinkContext - the set identifier
 */
  void setSinkContext(const Handle<SetIdentifier> &sinkContext);

/**
 * Should we materialize the output of this aggregation or should we keep it as a hash set?
 * @param materializeOrNot - true if we should, false otherwise
 */
  void setMaterializeOrNot(bool materializeOrNot);

/**
 * Return the build AggregationJobStage
 * @return the AggregationJobStage
 */
  Handle<AggregationJobStage> build();

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
 * The computation associated with this aggregation
 */
  Handle<AbstractAggregateComp> aggComp;

/**
 * The set identifier by the source set
 * This is used by the @see FrontendQueryTestServer to get the info about the source set
 */
  Handle<SetIdentifier> sourceContext;

/**
 * The set identifier of the output
 * This is used by the @see FrontendQueryTestServer to create the output set
 */
  Handle<SetIdentifier> sinkContext;

/**
 * Should we materialize this as a set or keep it as a hashSet
 */
  bool materializeOrNot;
};

}

#endif //PDB_AGGREGATIONJOBSTAGEBUILDER_H
