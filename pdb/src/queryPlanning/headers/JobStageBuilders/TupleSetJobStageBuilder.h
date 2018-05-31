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
#ifndef PDB_TUPLESETJOBSTAGE_H
#define PDB_TUPLESETJOBSTAGE_H

#include "Handle.h"
#include "TupleSetJobStage.h"

namespace pdb {

class TupleSetJobStageBuilder;
typedef std::shared_ptr<TupleSetJobStageBuilder> TupleSetJobStageBuilderPtr;

class TupleSetJobStageBuilder {

public:

  /**
   * Initializes the builder
   */
  TupleSetJobStageBuilder();

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
   * Sets the compute plan to the jobStage we are building
   * @param plan - ComputePlan generated from input computations and the input TCAP string
   */
  void setComputePlan(const Handle<ComputePlan> &plan);

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
   * Sets the target computation specifier
   * TODO this thing is used in the execution engine just to set : [NumNodes, NumPartitions, BatchSize] for aggregation or join
   * @param targetComputationSpecifier - the name of the computation
   */
  void setTargetComputationName(const std::string &targetComputationSpecifier);

  /**
   * Add a tuple set to the list of tuple sets that build up the pipeline
   * @param buildMe the tuple set name
   */
  void addTupleSetToBuildPipeline(const std::string &buildMe);

  /**
   * Add a hash set to probe
   * @param outputName - the output name of the computation
   * @param hashSetName - the name of the hash set
   */
  void addHashSetToProbe(const std::string &outputName, const std::string &hashSetName);

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
   * The type associated with the sink context by default it has a type of "IntermediateData"
   * @param outputTypeName the type name
   */
  void setOutputTypeName(const std::string &outputTypeName);

  /**
   * Is this pipeline probing a hashset
   * @param isProbing true if it is, false otherwise
   */
  void setProbing(bool isProbing);

  /**
   * The allocation policy of the computation
   * @param policy
   */
  void setAllocatorPolicy(AllocatorPolicy policy);

  /**
   * This is true if we are running a pipeline with a hash partition sink
   * for JoinMaps
   * @param repartitionJoinOrNot
   */
  void setRepartitionJoin(bool repartitionJoinOrNot);


  /**
   * This is true if we are running a pipeline with a hash partition sink
   * for Vectors
   * @param repartitionVectorOrNot
   */
  void setRepartitionVector(bool repartitionVectorOrNot);


  /**
   * True if we are running a pipeline with a broadcast sink, false otherwise
   * @param broadcastOrNot - the value
   */
  void setBroadcasting(bool broadcastOrNot);

  /**
   * True if we are running pipeline with shuffle sink
   * @param repartitionOrNot - the value
   */
  void setRepartition(bool repartitionOrNot);

  /**
   * If this parameter is set to true, the results of a pipeline with shuffle sink will be send to the
   * 0 through (numNodesToCollect - 1) nodes
   * @param collectAsMapOrNot - the value
   */
  void setCollectAsMap(bool collectAsMapOrNot);

  /**
   * True if the input is a result of an aggregation
   * @param inputAggHashOut - true if it is, false otherwise
   */
  void setInputAggHashOut(bool inputAggHashOut);

  /**
   * If isCollectAsMap is set to true this parameter will set the number of nodes we want to
   * send the shuffle sink data to.
   * @param numNodesToCollect - the value
   */
  void setNumNodesToCollect(int numNodesToCollect);

  /**
   * Sets a combiner for this tuple stage
   * @param combinerContext
   */
  void setCombiner(Handle<SetIdentifier> combinerContext);

  /**
   * Will this pipeline probe a hash set?
   * @return true if the pipeline will probe a hash set
   */
  bool isPipelineProbing();

  /**
   * TODO this is not something that should be in a builder see how we can remove it...
   * Returns the identifier of the source set
   * @return the set
   */
  Handle<SetIdentifier> getSourceSetIdentifier();

  /**
   * TODO this is not something that should be in a builder see how we can remove it...
   * Returns the source tuple set name
   * @return
   */
  const std::string &getSourceTupleSetName() const;

  /**
   * TODO this is not something that should be in a builder see how we can remove it..
   * Returns the last set that builds the pipeline
   * @return the name of the set
   */
  const std::string &getLastSetThatBuildsPipeline() const;

  /**
   * Return the build TupleSetJobStage
   * @return the TupleSetJobStage
   */
  Handle<TupleSetJobStage> build();

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
    Sets the target computation specifier
    TODO this thing is used in the execution engine just to set : [NumNodes, NumPartitions, BatchSize] for aggregation or join
   */
  std::string targetComputationName;

  /**
   * The ComputePlan generated from input computations and the input TCAP string
   */
  Handle<ComputePlan> computePlan;

  /**
   * Tuple sets produced by atomic computations we want to build this pipeline
   * @see pdb::ComputePlan::buildPipeline
   */
  std::vector<std::string> buildTheseTupleSets;

  /**
   * The type of the output, if it is intermediate data between stages it is set to "IntermediateData"
   */
  std::string outputTypeName;

  /**
   * The set identifier by the source set
   * This is used by the @see FrontendQueryTestServer to get the info about the source set
   */
  Handle<SetIdentifier> sourceContext;

  /**
   * The set identifier of the combiner set
   * This is used by the @see FrontendQueryTestServer to create the combiner set (where we put the result of the combiner)
   */
  Handle<SetIdentifier> combinerContext;

  /**
   * The set identifier of the output
   * This is used by the @see FrontendQueryTestServer to create the output set
   */
  Handle<SetIdentifier> sinkContext;

  /**
   * Hash sets we to probe in current stage.
   */
  Handle<Map<String, String>> hashSetsToProbe;

  /**
   * This is true if we are running a pipeline with a broadcast sink
   */
  bool isBroadcasting;

  /**
   * True if we are running pipeline with shuffle sink
   */
  bool isRepartitioning;

  /**
   * This is true if we are running a pipeline with a hash partition sink
   * for JoinMaps
   */
  bool isRepartitionJoin;
  
  /**
   * This is true if we are running a pipeline with a hash partition sink
   * for Vectors
   */
  bool isRepartitionVector;


  /**
   * True if this pipeline is probing a hash set
   */
  bool isProbing;

  /**
   * The allocation policy of the computation
   */
  AllocatorPolicy policy;

  /**
   * Aggregation output should not be kept across stages; if an aggregation has more than one
   * consumers, we need materialize aggregation results.
   */
  bool inputAggHashOut;

  /**
   * If this parameter is set to true, the results of a pipeline with shuffle sink will be send to the
   * 0 through (numNodesToCollect - 1) nodes
   */
  bool isCollectAsMap;

  /**
   * If isCollectAsMap is set to true this parameter will set the number of nodes we want to
   * send the shuffle sink data to.
   */
  int numNodesToCollect;

};

}

#endif //PDB_TUPLESETJOBSTAGE_H
