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
#include "AtomicComputation.h"
#include "JobStageBuilders/HashPartitionedJoinBuildHTJobStageBuilder.h"
#include "JobStageBuilders/BroadcastJoinBuildHTJobStageBuilder.h"
#include "Handle.h"
#include "SimplePhysicalOptimizer/SimplePhysicalNode.h"
#include "SimplePhysicalOptimizer/SimplePhysicalJoinNode.h"
#include "JoinComp.h"

pdb::SimplePhysicalJoinNode::SimplePhysicalJoinNode(string jobId,
                                                    AtomicComputationPtr node,
                                                    const Handle<ComputePlan> &computePlan,
                                                    LogicalPlanPtr logicalPlan,
                                                    ConfigurationPtr conf) : SimplePhysicalNode(std::move(jobId),
                                                                                                std::move(node),
                                                                                                computePlan,
                                                                                                logicalPlan,
                                                                                                std::move(conf)),
                                                                             transversed(false) {}

pdb::PhysicalOptimizerResultPtr pdb::SimplePhysicalJoinNode::analyzeOutput(pdb::TupleSetJobStageBuilderPtr &ptr,
                                                                           SimplePhysicalNodePtr &prevNode,
                                                                           const StatisticsPtr &stats,
                                                                           int nextStageID) {

  // grab the computation associated with this node
  Handle<Computation> comp = logicalPlan->getNode(node->getComputationName()).getComputationHandle();

  // TODO exit since this is what has been done in previous PhysicalOptimizer, maybe do something smarter
  PDB_COUT << "Sink Computation Type: " << comp->getComputationType() << " are not supported as sink node right now\n";
  exit(1);
}

pdb::PhysicalOptimizerResultPtr pdb::SimplePhysicalJoinNode::analyzeSingleConsumer(pdb::TupleSetJobStageBuilderPtr &tupleStageBuilder,
                                                                                   SimplePhysicalNodePtr &prevNode,
                                                                                   const StatisticsPtr &stats,
                                                                                   int nextStageID) {

  // TODO Jia : have no clue why this exists? why use the input name of the current node
  string targetTupleSetName = prevNode == nullptr ? node->getInputName() : prevNode->getNode()->getOutputName();

  Handle<SetIdentifier> sink = nullptr;

  // cast this node to an ApplyJoin
  shared_ptr<ApplyJoin> joinNode = dynamic_pointer_cast<ApplyJoin>(node);

  // the computation specifier of this join
  std::string computationSpecifier = node->getComputationName();

  // grab the output of the current node
  std::string outputName = node->getOutputName();

  // grab the computation associated with this node
  Handle<Computation> curComp = logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // We have two main cases to handle when dealing with a join
  // 1. This is the first time we are processing this join therefore no side of the join has been hashed
  // and then broadcasted or partitioned, therefore we can not probe it
  // 2. This is the second time we are processing this join therefore the one side of the join is hashed and then
  // broadcasted or partitioned, we can therefore probe it!
  if (!transversed) {

    // create a analyzer result
    PhysicalOptimizerResultPtr result = make_shared<PhysicalOptimizerResult>();

    // get the cost of the source set
    double sourceCost = getCost(tupleStageBuilder->getSourceSetIdentifier(), stats);

    // are we already probing a set in the pipeline, and is the cost of the current source smaller than the join
    // threshold? if so might be better to go back and process the other side of the join first
    if (tupleStageBuilder->isPipelineProbing() && (sourceCost <= BROADCAST_JOIN_COST_THRESHOLD)) {

      // we should go back therefore this analysis is not successful
      result->success = false;
      result->physicalPlanToOutput.clear();
      result->interGlobalSets.clear();

      //PDB_COUT << "WARNING: met a join sink with probing, to return and put cost " << sourceCost << " " << tupleStageBuilder->getSourceSetIdentifier()->toSourceSetName() << " to penalized list \n";

      // we return false to signalize that we did no extract a pipeline
      return result;
    }

    // does the cost of the current source exceed the join threshold, if so we need to do a hash partition join
    // therefore we definitely need to hash the current table this is definitely a pipeline breaker
    else if (sourceCost > BROADCAST_JOIN_COST_THRESHOLD) {

      // set the partitioning flag so we can know that when probing
      joinNode->setPartitioningLHS(true);

      // cast the computation to a JoinComp
      Handle<JoinComp<Object, Object, Object>>
          join = unsafeCast<JoinComp<Object, Object, Object>, Computation>(curComp);

      // mark it as a hash partition join
      join->setJoinType(HashPartitionedJoin);

      // I am a pipeline breaker.
      // We first need to create a TupleSetJobStage with a repartition sink
      sink = makeObject<SetIdentifier>(jobId, outputName + "_repartitionData");
      sink->setPageSize(conf->getBroadcastPageSize());

      // set the parameters
      tupleStageBuilder->setJobStageId(nextStageID++);
      tupleStageBuilder->setTargetTupleSetName(targetTupleSetName);
      tupleStageBuilder->setTargetComputationName(computationSpecifier);
      tupleStageBuilder->setOutputTypeName("IntermediateData");
      tupleStageBuilder->setSinkContext(sink);
      tupleStageBuilder->setRepartition(true);
      tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());
      tupleStageBuilder->setRepartitionJoin(true);

      // create the tuple stage to run a pipeline with a hash partition sink
      Handle<TupleSetJobStage> joinPrepStage = tupleStageBuilder->build();

      // add the stage to the list of stages to be executed
      result->physicalPlanToOutput.emplace_back(joinPrepStage);

      // add the sink to the intermediate sets
      result->interGlobalSets.push_back(sink);

      // the source set for the HashPartitionedJoinBuildHTJobStage is the sink set of the TupleSetJobStage
      hashSetName = sink->toSourceSetName();

      // initialize the build hash partition set builder stage
      HashPartitionedJoinBuildHTJobStageBuilderPtr
          hashBuilder = make_shared<HashPartitionedJoinBuildHTJobStageBuilder>();

      // set the parameters
      hashBuilder->setJobId(jobId);
      hashBuilder->setJobStageId(nextStageID++);
      hashBuilder->setSourceTupleSetName(joinPrepStage->getSourceTupleSetSpecifier());
      hashBuilder->setTargetTupleSetName(targetTupleSetName);
      hashBuilder->setTargetComputationName(computationSpecifier);
      hashBuilder->setSourceContext(sink);
      hashBuilder->setHashSetName(hashSetName);
      hashBuilder->setComputePlan(computePlan);

      // create the build hash partitioned join hash table job stage to partition and shuffle the source set
      Handle<HashPartitionedJoinBuildHTJobStage> joinPartitionStage = hashBuilder->build();

      // add the stage to the list of stages to be executed
      result->physicalPlanToOutput.emplace_back(joinPartitionStage);

    } else {

      // The other input has not been processed and we can do broadcasting because
      // costOfCurSource <= JOIN_COST_THRESHOLD. I am a pipeline breaker.
      // We first need to create a TupleSetJobStage with a broadcasting sink
      // then a BroadcastJoinBuildHTJobStage to build a hash table of that data.

      // the set identifier of the set where we store the output of the TupleSetJobStage
      sink = makeObject<SetIdentifier>(jobId, outputName + "_broadcastData");
      sink->setPageSize(conf->getBroadcastPageSize());

      // set the parameters
      tupleStageBuilder->setJobStageId(nextStageID);
      tupleStageBuilder->setTargetTupleSetName(targetTupleSetName);
      tupleStageBuilder->setTargetComputationName(computationSpecifier);
      tupleStageBuilder->setOutputTypeName("IntermediateData");
      tupleStageBuilder->setSinkContext(sink);
      tupleStageBuilder->setBroadcasting(true);
      tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());

      // We are setting isBroadcasting to true so that we run a pipeline with broadcast sink
      Handle<TupleSetJobStage> joinPrepStage = tupleStageBuilder->build();

      // add the stage to the list of stages to be executed
      result->physicalPlanToOutput.emplace_back(joinPrepStage);

      // add the sink to the intermediate sets
      result->interGlobalSets.push_back(sink);

      // grab the hash set name
      hashSetName = sink->toSourceSetName();

      // initialize the build hash partition set builder stage
      BroadcastJoinBuildHTJobStageBuilderPtr broadcastBuilder = make_shared<BroadcastJoinBuildHTJobStageBuilder>();

      // set the parameters
      broadcastBuilder->setJobId(jobId);
      broadcastBuilder->setJobStageId(nextStageID);
      broadcastBuilder->setSourceTupleSetName(joinPrepStage->getSourceTupleSetSpecifier());
      broadcastBuilder->setTargetTupleSetName(targetTupleSetName);
      broadcastBuilder->setTargetComputationName(computationSpecifier);
      broadcastBuilder->setSourceContext(sink);
      broadcastBuilder->setHashSetName(hashSetName);
      broadcastBuilder->setComputePlan(computePlan);

      // We then create a BroadcastJoinBuildHTStage
      Handle<BroadcastJoinBuildHTJobStage> joinBroadcastStage = broadcastBuilder->build();

      // add the stage to the list of stages to be executed
      result->physicalPlanToOutput.emplace_back(joinBroadcastStage);
    }

    // We should not go further, we set it to traversed and leave it to
    // other join inputs, and simply return
    transversed = true;

    // set the remaining parameters of the result
    result->success = true;

    // return to indicate the we succeeded
    return result;
  } else {

    // at this point we know that the other side of the join has been processed now we need to figure out how?
    // There are two options :
    // 1. is that it has been partitioned, in this case we are probing a partitioned hash table
    // 2. is that it has been broadcasted, in that case we are probing a broadcasted hash table

    // check if the other side has been partitioned
    if (joinNode->isPartitioningLHS()) {

      // we have only one consumer node so we know what the next node in line is
      SimplePhysicalNodePtr nextNode = activeConsumers.front();

      // fist we need to shuffle our data from the other side and put it in this sink set
      sink = makeObject<SetIdentifier>(jobId, outputName + "_repartitionData");
      sink->setPageSize(conf->getBroadcastPageSize());

      // set the parameters
      tupleStageBuilder->setJobStageId(nextStageID++);
      tupleStageBuilder->setTargetTupleSetName(targetTupleSetName);
      tupleStageBuilder->setTargetComputationName(computationSpecifier);
      tupleStageBuilder->setOutputTypeName("IntermediateData");
      tupleStageBuilder->setSinkContext(sink);
      tupleStageBuilder->setRepartition(true);
      tupleStageBuilder->setAllocatorPolicy(curComp->getAllocatorPolicy());
      tupleStageBuilder->setRepartitionJoin(true);

      // we first create a pipeline breaker to partition RHS by setting
      // the isRepartitioning=true and isRepartitionJoin=true
      Handle<TupleSetJobStage> joinPrepStage = tupleStageBuilder->build();

      // ok we have added a tuple stage to shuffle the data now we need one to probe the hash set
      // therefore we create a job stage builder
      TupleSetJobStageBuilderPtr probingStageBuilder = make_shared<TupleSetJobStageBuilder>();

      // set the job id
      probingStageBuilder->setJobId(joinPrepStage->getJobId());

      // the join source becomes the original source
      probingStageBuilder->setSourceTupleSetName(tupleStageBuilder->getSourceTupleSetName());

      // grab the last computation to be applied (that thing is our hashing thingy)
      string lastOne = tupleStageBuilder->getLastSetThatBuildsPipeline();

      // add the last and the current
      probingStageBuilder->addTupleSetToBuildPipeline(lastOne);
      probingStageBuilder->addTupleSetToBuildPipeline(node->getOutputName());

      // add the hash set this pipeline is going to probe
      probingStageBuilder->addHashSetToProbe(outputName, hashSetName);
      probingStageBuilder->setProbing(true);
      probingStageBuilder->setSourceContext(sink);
      probingStageBuilder->setSourceTupleSetName(tupleStageBuilder->getSourceTupleSetName());
      probingStageBuilder->setComputePlan(computePlan);

      // I am the previous node
      SimplePhysicalNodePtr newPrevNode = getSimpleNodeHandle();

      // temporary alias the source set with the sink set because the are essentially the same and we need the stats
      stats->addSetAlias(tupleStageBuilder->getSourceSetIdentifier()->getDatabase(),
                         tupleStageBuilder->getSourceSetIdentifier()->getSetName(),
                         sink->getDatabase(),
                         sink->getSetName());

      // we then create a pipeline stage to probe the partitioned hash table
      PhysicalOptimizerResultPtr result = activeConsumers.front()->analyze(probingStageBuilder,
                                                                           newPrevNode,
                                                                           stats,
                                                                           nextStageID);

      // remove the temporary alias
      stats->removeSet(sink->getDatabase(), sink->getSetName());

      // add the stage to the list of stages to be executed
      result->physicalPlanToOutput.emplace_front(joinPrepStage);

      // add the output of this TupleSetJobStage to the list of intermediate sets
      result->interGlobalSets.emplace_front(sink);

      return result;

    } else {

      // we probe the broadcasted hash table
      // if my other input has been processed, I am not a pipeline breaker, but we
      // should set the correct hash set names for probing
      tupleStageBuilder->addTupleSetToBuildPipeline(node->getOutputName());
      tupleStageBuilder->addHashSetToProbe(outputName, hashSetName);
      tupleStageBuilder->setProbing(true);

      // I am the previous node
      SimplePhysicalNodePtr newPrevNode = getSimpleNodeHandle();

      // go and analyze further
      return activeConsumers.front()->analyze(tupleStageBuilder, newPrevNode, stats, nextStageID);
    }
  }
}