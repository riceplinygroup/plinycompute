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
#ifndef TCAP_ANALYZER
#define TCAP_ANALYZER

//by Jia, Apr 2017

#include "ComputePlan.h"
#include "Computation.h"
#include "LogicalPlan.h"
#include "AtomicComputationList.h"
#include "PDBLogger.h"
#include "TupleSetJobStage.h"
#include "AggregationJobStage.h"
#include "BroadcastJoinBuildHTJobStage.h"

namespace pdb {

/*
 * This class encapsulates the analyzer for TCAP string
 * You can use this class transform a TCAP string into a physical plan: a sequence of AbstractJobStage instances
 * This class must be used in the same allocation block with the invoker of method analyze() to avoid problematic shallow copy.
 */

class TCAPAnalyzer {

public:    

//constructor
TCAPAnalyzer (std :: string jobId, Handle<Vector<Handle<Computation>>> myComputations, std :: string myTCAPString, PDBLoggerPtr logger, bool isDynamicPlanning = false);

//destructor
~TCAPAnalyzer ();

//to analyze the TCAP strings and get physical plan 
bool analyze(std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets);

//to analyze the subgraph rooted at a source node (sourceComputation) and get a partial phyical plan
//if current node has two inputs, we need to specify the prev node
bool analyze(std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets, AtomicComputationPtr sourceComputation, int & jobStageId);


//to create tuple set job stage
Handle<TupleSetJobStage>  createTupleSetJobStage(int & jobStageId, std :: string sourceTupleSetName, std :: string targetTupleSetName, std :: string targetComputationName, std :: vector<std :: string> buildTheseTupleSets, std :: string outputTypeName, Handle<SetIdentifier> sourceContext, Handle<SetIdentifier> combinerContext, Handle<SetIdentifier> sinkContext, bool isBroadcasting, bool isRepartitioning, bool needsRemoveInputSet, bool isProbing=false);

//to create broadcast join stage
Handle<BroadcastJoinBuildHTJobStage> createBroadcastJoinBuildHTJobStage (int & jobStageId, std :: string sourceTupleSetName, std :: string targetTupleSetName, std :: string targetComputationName,  Handle<SetIdentifier> sourceContext, std :: string hashSetName, bool needsRemoveInputSet); 

//to create aggregation job stage
Handle<AggregationJobStage>  createAggregationJobStage(int & jobStageId,  Handle<AbstractAggregateComp> aggComp, Handle<SetIdentifier> sourceContext, Handle<SetIdentifier> sinkContext, std :: string outputTypeName, bool materializeOrNot);

//to analyze subgraph rooted at any node (curNode) and get a physical plan
bool analyze (std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets, std :: vector <std :: string> & buildTheseTupleSets, AtomicComputationPtr curSource, Handle<Computation> sourceComputation, Handle<SetIdentifier> curInputSetIdentifier, AtomicComputationPtr curNode, int &jobStageId, AtomicComputationPtr prevComputation=nullptr, bool isProbing=false);

//to get current source sets;
std :: vector <std :: string> & getCurSourceSetNames ();

//to get the source specified by index
std :: string getSourceSetName (int index);

//to get source set based on name
Handle<SetIdentifier> getSourceSetIdentifier (std :: string name);

//to get source computation based on name
AtomicComputationPtr getSourceComputation (std :: string name);

//to update source set names
bool updateSourceSets (Handle<SetIdentifier> oldSet, Handle<SetIdentifier> newSet, AtomicComputationPtr newAtomicComp);


//to get number of sources
int getNumSources ();

//to return the index of the best source 
int getBestSource ();

//to return the cost of the i-th source
double getCostOfSource (int index);

private:

//hash sets to probe in current stage
//needs to be cleared after execution of each stage
Handle<Map<String, String>> hashSetsToProbe;

//input computations
Handle<Vector<Handle<Computation>>> computations;

//input tcap string
std :: string tcapString;

//compute plan generated from input computations and the input tcap string
Handle<ComputePlan> computePlan;

//logical plan generated from the compute plan
LogicalPlanPtr logicalPlan;

//the computation graph generated from the logical plan
AtomicComputationList computationGraph;

//the source nodes of logical plan, which will not change
std :: vector<AtomicComputationPtr> sources;

//the logger
PDBLoggerPtr logger;

//the jobId for this query
std :: string jobId;

//whether to analyze the logical plan dynamically
bool dynamicPlanningOrNot;

//the mapping of set name to source set identifiers, which will only be used in dynamicPlanning mode, and will keep changing
std :: map<std :: string, Handle<SetIdentifier>> curSourceSets;

//the mapping of set name to source computation node, which will only be used in dynamicPlanning mode, and will keep changing
std :: map<std :: string, AtomicComputationPtr> curSourceNodes;

//the vector of set names
std :: vector<std :: string> curSourceSetNames;

};



}

#endif
