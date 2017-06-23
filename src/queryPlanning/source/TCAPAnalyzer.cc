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

#ifndef TCAP_ANALYZER_CC
#define TCAP_ANALYZER_CC

#include "TCAPAnalyzer.h"
#include "SelectionComp.h"
#include "ScanUserSet.h"
#include "MultiSelectionComp.h"
#include <cfloat>

namespace pdb {

TCAPAnalyzer::TCAPAnalyzer (std :: string jobId, Handle<Vector<Handle<Computation>>> myComputations, std :: string myTCAPString, PDBLoggerPtr logger, bool isDynamicPlanning) {
    this->jobId = jobId;
    this->computations = myComputations;
    this->tcapString = myTCAPString;
    this->logger = logger;
    try {
        this->computePlan = makeObject<ComputePlan>(String(myTCAPString), *myComputations);
        this->logicalPlan = this->computePlan->getPlan();
        this->computePlan->nullifyPlanPointer();
        this->computationGraph = this->logicalPlan->getComputations();
        this->sources = this->computationGraph.getAllScanSets();
    }
    catch (pdb :: NotEnoughSpace &n) {
        std :: cout << "FATAL ERROR in TCAPAnalyzer: Not enough memory to allocate the computePlan object";
        logger->fatal("FATAL ERROR in TCAPAnalyzer: Not enough memory to allocate the computePlan object");
        this->computePlan = nullptr;
        this->logicalPlan = nullptr;
        this->sources.clear();
    }
    hashSetsToProbe = nullptr;
    this->dynamicPlanningOrNot = isDynamicPlanning;
    if (this->dynamicPlanningOrNot == true) {
       //initialize source sets and source nodes;
       for (int i = 0; i < this->sources.size(); i++) {
           AtomicComputationPtr curSource = sources[i];
           Handle<SetIdentifier> curInputSetIdentifier = nullptr;
           std :: string sourceSpecifier = curSource->getComputationName();
           Handle<Computation> sourceComputation = this->logicalPlan->getNode(sourceSpecifier).getComputationHandle();
           if ((sourceComputation->getComputationType() == "ScanUserSet") || (sourceComputation->getComputationType() == "ScanSet")){
               Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(sourceComputation);
               curInputSetIdentifier = makeObject<SetIdentifier>(scanner->getDatabaseName(), scanner->getSetName());
           } else if (sourceComputation->getComputationType() == "ClusterAggregationComp") {
               Handle<AbstractAggregateComp> aggregator = unsafeCast<AbstractAggregateComp, Computation>(sourceComputation);
               curInputSetIdentifier = makeObject<SetIdentifier>(aggregator->getDatabaseName(), aggregator->getSetName());

           } else if (sourceComputation->getComputationType() == "SelectionComp") {
               Handle<SelectionComp<Object,Object>> selector = unsafeCast<SelectionComp<Object,Object>, Computation>(sourceComputation);
               curInputSetIdentifier = makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
           } else if (sourceComputation->getComputationType() == "MultiSelectionComp") {
               Handle<MultiSelectionComp<Object,Object>> selector = unsafeCast<MultiSelectionComp<Object,Object>, Computation>(sourceComputation);
               curInputSetIdentifier = makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
           } else  {
               std :: cout << "Source Computation Type: " << sourceComputation->getComputationType() << " are not supported as source node right now" << std :: endl;
               this->logger->fatal("Source Computation Type: " + sourceComputation->getComputationType() + " are not supported as source node right now");
           }
           std :: string mySourceSetName = curInputSetIdentifier->getDatabase() + ":" + curInputSetIdentifier->getSetName();
           curSourceSetNames.push_back(mySourceSetName);
           curSourceSets[mySourceSetName] = curInputSetIdentifier;
           curSourceNodes[mySourceSetName] = curSource;
       }
       //check current string
       PDB_COUT << "All initial sources: " << std :: endl;
       for (std :: string & myStr : curSourceSetNames) {
           PDB_COUT << myStr << std :: endl;
       }
    }
}

TCAPAnalyzer::~TCAPAnalyzer () {
    this->sources.clear();
    this->logicalPlan = nullptr;
    this->computePlan->nullifyPlanPointer();
    this->computePlan = nullptr;
    this->hashSetsToProbe = nullptr;
}


bool TCAPAnalyzer::analyze(std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets) {
    int jobStageId = 0;
    for (int i = 0; i < this->sources.size(); i++) {
        AtomicComputationPtr curSource = sources[i];
        bool ret = analyze (physicalPlanToOutput, interGlobalSets, curSource, jobStageId);
        if (ret == false) {
            return false;
        }
    }
    return true;
}

//to analyze the subgraph rooted at a source node and only returns a set of job stages corresponding with the subgraph
bool TCAPAnalyzer::getNextStages(std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets, AtomicComputationPtr curSource, Handle<SetIdentifier>  curInputSetIdentifier, int & jobStageId) {
   std :: string sourceSpecifier = curSource->getComputationName();
   Handle<Computation> sourceComputation = this->logicalPlan->getNode(sourceSpecifier).getComputationHandle();
    std :: string outputName = curSource->getOutputName();
    std :: vector<AtomicComputationPtr> consumers = this->computationGraph.getConsumingAtomicComputations(outputName);
    int numConsumersForCurNode = consumers.size();
    for (int i = 0; i < numConsumersForCurNode; i++) {
        AtomicComputationPtr curNode = consumers[i];
        std :: vector <std :: string> tupleSetNames;
        tupleSetNames.push_back(outputName);
        bool ret = analyze(physicalPlanToOutput, interGlobalSets, tupleSetNames, curSource, sourceComputation, curInputSetIdentifier, curNode, jobStageId, curSource);
        if (ret == false) {
            return false;
        }
    }
    return true;
}


//a source computation for a pipeline can be ScanSet, Selection, ClusterAggregation, and ClusterJoin.
bool TCAPAnalyzer::analyze(std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets, AtomicComputationPtr curSource, int & jobStageId) {
    //first get source set identifier
    std :: string sourceSpecifier = curSource->getComputationName();
    Handle<SetIdentifier> curInputSetIdentifier = nullptr;    
    Handle<Computation> sourceComputation = this->logicalPlan->getNode(sourceSpecifier).getComputationHandle();
    if ((sourceComputation->getComputationType() == "ScanUserSet") || (sourceComputation->getComputationType() == "ScanSet")){
        Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(sourceComputation);
        curInputSetIdentifier = makeObject<SetIdentifier>(scanner->getDatabaseName(), scanner->getSetName());
    } else if (sourceComputation->getComputationType() == "ClusterAggregationComp") {
        Handle<AbstractAggregateComp> aggregator = unsafeCast<AbstractAggregateComp, Computation>(sourceComputation);
        curInputSetIdentifier = makeObject<SetIdentifier>(aggregator->getDatabaseName(), aggregator->getSetName());

    } else if (sourceComputation->getComputationType() == "SelectionComp") {
        Handle<SelectionComp<Object,Object>> selector = unsafeCast<SelectionComp<Object,Object>, Computation>(sourceComputation);
        curInputSetIdentifier = makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
   
    } else if (sourceComputation->getComputationType() == "MultiSelectionComp") {
        Handle<MultiSelectionComp<Object,Object>> selector = unsafeCast<MultiSelectionComp<Object,Object>, Computation>(sourceComputation);
        curInputSetIdentifier = makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
    }else  {
        std :: cout << "Source Computation Type: " << sourceComputation->getComputationType() << " are not supported as source node right now" << std :: endl;
        this->logger->fatal("Source Computation Type: " + sourceComputation->getComputationType() + " are not supported as source node right now");
    }

    std :: string outputName = curSource->getOutputName();
    std :: vector<AtomicComputationPtr> consumers = this->computationGraph.getConsumingAtomicComputations(outputName);
    int numConsumersForCurNode = consumers.size();
    for (int i = 0; i < numConsumersForCurNode; i++) {
        AtomicComputationPtr curNode = consumers[i];
        std :: vector <std :: string> tupleSetNames;
        tupleSetNames.push_back(outputName);
        bool ret = analyze(physicalPlanToOutput, interGlobalSets, tupleSetNames, curSource, sourceComputation, curInputSetIdentifier, curNode, jobStageId, curSource);
        if (ret == false) {
            return false;
        }
    }
    return true;
}

Handle<TupleSetJobStage>  TCAPAnalyzer::createTupleSetJobStage(int & jobStageId, std :: string sourceTupleSetName, std :: string targetTupleSetName, std :: string targetComputationName, std :: vector<std :: string> buildTheseTupleSets, std :: string outputTypeName, Handle<SetIdentifier> sourceContext, Handle<SetIdentifier> combinerContext, Handle<SetIdentifier> sinkContext, bool isBroadcasting, bool isRepartitioning, bool needsRemoveInputSet, bool isProbing, AllocatorPolicy myPolicy) {
    Handle<TupleSetJobStage> jobStage = makeObject<TupleSetJobStage> (jobStageId);
    jobStageId ++;
    jobStage->setComputePlan(this->computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
    jobStage->setTupleSetsToBuildPipeline(buildTheseTupleSets);
    jobStage->setSourceContext(sourceContext);
    jobStage->setSinkContext(sinkContext);
    jobStage->setOutputTypeName(outputTypeName);
    jobStage->setAllocatorPolicy(myPolicy);
    if ((hashSetsToProbe != nullptr) && (isProbing == true)) {
        jobStage->setProbing(true);
        jobStage->setHashSetsToProbe(this->hashSetsToProbe);
        this->hashSetsToProbe = nullptr;
    }
    if (combinerContext != nullptr) {
        jobStage->setCombinerContext(combinerContext);
        jobStage->setCombining(true);
        jobStage->setNeedsRemoveCombinerSet(true);
    }
    jobStage->setNeedsRemoveInputSet(needsRemoveInputSet);
    if (sourceContext->isAggregationResult() == true) {
        jobStage->setInputAggHashOut(true);
        jobStage->setNeedsRemoveInputSet(true);//aggregation output should not be kept across stages; if an aggregation has more than one consumers, we need materialize aggregation results.
    }    
    jobStage->setBroadcasting(isBroadcasting);
    jobStage->setRepartition(isRepartitioning);
    jobStage->setJobId(this->jobId);
    PDB_COUT << "TCAPAnalyzer generates tupleSetJobStage:" << std :: endl;
    //jobStage->print();
    return jobStage;
}

Handle<AggregationJobStage>  TCAPAnalyzer::createAggregationJobStage(int & jobStageId,  Handle<AbstractAggregateComp> aggComp, Handle<SetIdentifier> sourceContext, Handle<SetIdentifier> sinkContext, std :: string outputTypeName, bool materializeResultOrNot) {
    Handle<AggregationJobStage> aggStage = makeObject<AggregationJobStage>(jobStageId, materializeResultOrNot, aggComp);
    jobStageId ++;
    aggStage->setSourceContext(sourceContext);
    aggStage->setSinkContext(sinkContext);
    aggStage->setOutputTypeName(aggComp->getOutputType());
    aggStage->setNeedsRemoveInputSet(false);
    aggStage->setJobId(this->jobId);
    return aggStage;
}

Handle<BroadcastJoinBuildHTJobStage> TCAPAnalyzer::createBroadcastJoinBuildHTJobStage (int & jobStageId, std :: string sourceTupleSetName, std :: string targetTupleSetName, std :: string targetComputationName, Handle<SetIdentifier> sourceContext, std :: string hashSetName, bool needsRemoveInputSet) {
    Handle<BroadcastJoinBuildHTJobStage> broadcastJoinStage = makeObject<BroadcastJoinBuildHTJobStage> (this->jobId, jobStageId, hashSetName);
    jobStageId ++;
    broadcastJoinStage->setComputePlan (this->computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
    broadcastJoinStage->setSourceContext (sourceContext);
    broadcastJoinStage->setNeedsRemoveInputSet (needsRemoveInputSet); 
    return broadcastJoinStage;
}

bool TCAPAnalyzer::updateSourceSets (Handle<SetIdentifier> oldSet, Handle<SetIdentifier> newSet, AtomicComputationPtr newAtomicComp) {
    bool ret = true;
    
    //get old set name
    std :: string oldSetName = oldSet->getDatabase() + ":" + oldSet->getSetName();
    PDB_COUT << "oldSetName = " << oldSetName << std :: endl;

    //check current string
    PDB_COUT << "current source set names: " << std :: endl;
    for (std :: string & myStr : curSourceSetNames) {
        PDB_COUT << myStr << std :: endl;
    }

    //remove the old stuff
    for (std :: vector<std :: string>::iterator iter = curSourceSetNames.begin(); iter != curSourceSetNames.end(); iter++) {
        PDB_COUT << "curSetName = " << *iter << std :: endl;
        std :: string curStr = *iter;
        if (curStr == oldSetName) {
            iter = curSourceSetNames.erase(iter);
            break;
        }
    } 
    if (curSourceSets.count(oldSetName) > 0) {
        curSourceSets.erase(oldSetName);
    } else {
        ret = false;
    }
    if (curSourceNodes.count(oldSetName) > 0) {
        curSourceNodes.erase(oldSetName);
    } else {
        ret = false;
    }

    if ((newSet != nullptr) && (newAtomicComp != nullptr)){
        //get new set name
        std :: string newSetName = newSet->getDatabase() + ":" + newSet->getSetName();
        //add new set
        curSourceSetNames.push_back(newSetName);
        curSourceSets[newSetName] = newSet;
        curSourceNodes[newSetName] = newAtomicComp;

    }
    return ret;
}

bool TCAPAnalyzer::analyze (std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets, std :: vector <std :: string> & buildTheseTupleSets, AtomicComputationPtr curSource, Handle<Computation> sourceComputation, Handle<SetIdentifier> curInputSetIdentifier, AtomicComputationPtr curNode, int &jobStageId, AtomicComputationPtr prevNode, bool isProbing, AllocatorPolicy myPolicy) {
    //to get consumers
    std :: string outputName = curNode->getOutputName();
    std :: vector<AtomicComputationPtr> consumers = this->computationGraph.getConsumingAtomicComputations(outputName);
    int numConsumersForCurNode = consumers.size();
     
    //to get my type
    std :: string mySpecifier = curNode->getComputationName();
    Handle<Computation> myComputation = this->logicalPlan->getNode(mySpecifier).getComputationHandle();

    if (myComputation->getAllocatorPolicy() == AllocatorPolicy :: noReuseAllocator) {
        myPolicy = AllocatorPolicy :: noReuseAllocator;
    } 

    PDB_COUT << "Current node's output name=" << outputName << std :: endl;
    PDB_COUT << "Current node's computation type=" << myComputation->getComputationType() << std :: endl;
    PDB_COUT << "Current node's computation name=" << mySpecifier << std :: endl;
    PDB_COUT << "Current node's atomic computation type=" << curNode->getAtomicComputationType() << std :: endl;
    PDB_COUT << "numConsumersForCurNode=" << numConsumersForCurNode << std :: endl;        
    
    if (numConsumersForCurNode == 0) {
        //to get my output set 
        std :: string dbName = myComputation->getDatabaseName();
        std :: string setName = myComputation->getSetName();
        Handle<SetIdentifier> sink = makeObject<SetIdentifier> (dbName, setName);
        if (myComputation->getComputationType() == "ClusterAggregationComp") {
            //to create the producing job stage for aggregation
            Handle<SetIdentifier>aggregator = makeObject<SetIdentifier>(this->jobId, outputName+"_aggregationData");
            Handle<SetIdentifier>combiner = nullptr;
            if (myComputation->isUsingCombiner() == true) {
                combiner = makeObject<SetIdentifier>(this->jobId, outputName+"_combinerData");
            }
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage(jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier, buildTheseTupleSets, "IntermediateData", curInputSetIdentifier, combiner, aggregator, false, true, false, isProbing, myPolicy); 
            //to push back the job stage
            physicalPlanToOutput.push_back(jobStage);
            //to create the consuming job stage for aggregation
            Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(myComputation);
            Handle<AggregationJobStage> aggStage = createAggregationJobStage(jobStageId, agg, aggregator, sink, agg->getOutputType(), true);
            //to push back the aggregation stage;
            physicalPlanToOutput.push_back(aggStage);
            //to push back the aggregator set
            interGlobalSets.push_back(aggregator);
            if (this->dynamicPlanningOrNot == true) {
                this->updateSourceSets (curInputSetIdentifier, nullptr, nullptr);
            }
            return true;

        } else if ((myComputation->getComputationType() == "WriteUserSet") || (myComputation->getComputationType() == "SelectionComp") || (myComputation->getComputationType() == "MultiSelectionComp")) {
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier, buildTheseTupleSets, myComputation->getOutputType(), curInputSetIdentifier, nullptr, sink, false, false, false, isProbing, myPolicy);
            physicalPlanToOutput.push_back(jobStage);
            if (this->dynamicPlanningOrNot == true) {
                this->updateSourceSets (curInputSetIdentifier, nullptr, nullptr);
            }
            return true;
        } else {
            std :: cout << "Sink Computation Type: " << myComputation->getComputationType() << " are not supported as sink node right now" << std :: endl;
            this->logger->fatal("Source Computation Type: " + myComputation->getComputationType() + " are not supported as sink node right now");
            return false;
        } 
        
    } else if (numConsumersForCurNode == 1) {
        AtomicComputationPtr nextNode = consumers[0];
        //check my type
        //if I am aggregation or join, I am a pipeline breaker
        //we currently do not support join
        if (curNode->getAtomicComputationType() == "Aggregate") {
            Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(this->jobId, outputName+"_aggregationData");
            Handle<SetIdentifier>combiner = nullptr;
            if (myComputation->isUsingCombiner() == true) {
                combiner = makeObject<SetIdentifier>(this->jobId, outputName+"_combinerData");
            }
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier, buildTheseTupleSets, "IntermediateData", curInputSetIdentifier, combiner, aggregator, false, true, false, isProbing, myPolicy);
            physicalPlanToOutput.push_back(jobStage);
            //to create the consuming job stage for aggregation
            Handle<AggregationJobStage> aggStage;
            Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(myComputation);
            Handle<SetIdentifier> sink;
            if (myComputation->needsMaterializeOutput() == true) {
                //to get my output set
                std :: string dbName = myComputation->getDatabaseName();
                std :: string setName = myComputation->getSetName();
                sink = makeObject<SetIdentifier> (dbName, setName);
                aggStage = createAggregationJobStage(jobStageId, agg, aggregator, sink, agg->getOutputType(), true);
            } else {
                sink = makeObject<SetIdentifier> (this->jobId, outputName+"_aggregationResult", PartitionedHashSetType, true);
                aggStage = createAggregationJobStage(jobStageId, agg, aggregator, sink, agg->getOutputType(), false);
            }
            //to push back the aggregation stage;
            physicalPlanToOutput.push_back(aggStage);
            //to push back the aggregator set
            interGlobalSets.push_back(aggregator);
            if (this->dynamicPlanningOrNot == true) {
                this->updateSourceSets(curInputSetIdentifier, sink, curNode);
                return true;
            } else {
                buildTheseTupleSets.clear();
                buildTheseTupleSets.push_back(curNode->getOutputName());
                //Now I am the source!
                return analyze(physicalPlanToOutput, interGlobalSets, buildTheseTupleSets, curNode, myComputation, sink, nextNode, jobStageId, curNode);
            }
        } else if (curNode->getAtomicComputationType() == "JoinSets") {
            std :: shared_ptr<ApplyJoin> joinNode = dynamic_pointer_cast<ApplyJoin> (curNode);
            if (joinNode->isTraversed() == false) {
                //if the other input has not been processed, I am a pipeline breaker.
                //We first need to create a TupleSetJobStage with a broadcasting sink

                //sourceSet (curInputSetIdentifier)
                //sinkSet
                Handle<SetIdentifier> sink = makeObject<SetIdentifier>(this->jobId, outputName+"_broadcastData");

                //computePlan
                //sourceTupleSetName
                //sinkTupleSetName
                //sinkComputationName
                
                //isBroadcasting = true
                //isRepartitioning = false
                //collect probing information
                //isCombining = false
                std :: string targetTupleSetName;
                if(prevNode == nullptr) {
                    targetTupleSetName = curNode->getInputName();//join has two input names
                } else {
                    targetTupleSetName = prevNode->getOutputName();
                }
                Handle<TupleSetJobStage> joinPrepStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), targetTupleSetName, mySpecifier, buildTheseTupleSets, "IntermediateData", curInputSetIdentifier, nullptr, sink, true, false, false, isProbing, myPolicy);
                physicalPlanToOutput.push_back(joinPrepStage);
                interGlobalSets.push_back(sink);

                //We then create a BroadcastJoinBuildHTStage
                std :: string hashSetName = sink->getDatabase() + ":" + sink->getSetName();
                std :: cout << "TCAPAnalyzer: hashSetName = " << hashSetName << std :: endl;
                Handle<BroadcastJoinBuildHTJobStage> joinBroadcastStage = createBroadcastJoinBuildHTJobStage (jobStageId, curSource->getOutputName(), targetTupleSetName, mySpecifier, sink, hashSetName, false);
                physicalPlanToOutput.push_back(joinBroadcastStage);                
                //set the probe information
                if (hashSetsToProbe == nullptr) {
                    hashSetsToProbe = makeObject<Map<String, String>> ();
                }
                (*hashSetsToProbe)[outputName] = hashSetName;
                //We should not go further, we set it to traversed and leave it to other join inputs, and simply return
                joinNode->setTraversed(true);
                if (this->dynamicPlanningOrNot == true) {
                    updateSourceSets(curInputSetIdentifier, nullptr, nullptr);
                }
                return true;
            } else {
                //if my other input has been processed, I am not a pipeline breaker, but we should set the correct hash set names for probing
                buildTheseTupleSets.push_back(curNode->getOutputName());
                return analyze(physicalPlanToOutput, interGlobalSets, buildTheseTupleSets, curSource, sourceComputation, curInputSetIdentifier, nextNode, jobStageId, curNode, true, myPolicy);
            }

        } else {
            //I am not a pipeline breaker
            buildTheseTupleSets.push_back(curNode->getOutputName());
            return analyze(physicalPlanToOutput, interGlobalSets, buildTheseTupleSets, curSource, sourceComputation, curInputSetIdentifier, nextNode, jobStageId, curNode, isProbing, myPolicy);
            
        } 


    } else {
        //I am a pipeline breaker because I have more than one consumers
        //TODO: check whether I am the final operator in a computation. If I am not the final operator in a computation, there is an error.
        Handle<SetIdentifier> sink = nullptr;
        if(myComputation->needsMaterializeOutput() == false) {
            myComputation->setOutput(this->jobId, outputName);
            sink = makeObject<SetIdentifier>(this->jobId, outputName);
            interGlobalSets.push_back(sink);
        } else {
            //to get my output set 
            std :: string dbName = myComputation->getDatabaseName();
            std :: string setName = myComputation->getSetName();
            sink = makeObject<SetIdentifier> (dbName, setName);
        }
        if ((myComputation->getComputationType() == "SelectionComp") ||  (myComputation->getComputationType() == "MultiSelectionComp")) {
            buildTheseTupleSets.push_back(curNode->getOutputName());
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), curNode->getOutputName(), mySpecifier, buildTheseTupleSets, myComputation->getOutputType(), curInputSetIdentifier, nullptr, sink, false, false, false, isProbing, myPolicy);
            physicalPlanToOutput.push_back(jobStage);
        } else if (myComputation->getComputationType() == "ClusterAggregationComp") {
            Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(this->jobId, outputName+"_aggregationData");
            Handle<SetIdentifier>combiner = nullptr;
            if (myComputation->isUsingCombiner() == true) {
                combiner = makeObject<SetIdentifier>(this->jobId, outputName+"_combinerData");
            }
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier, buildTheseTupleSets, "IntermediateData", curInputSetIdentifier, combiner, aggregator, false, true, false, isProbing, myPolicy);
            physicalPlanToOutput.push_back(jobStage);
            //to create the consuming job stage for aggregation
            Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(myComputation);
            Handle<AggregationJobStage> aggStage = createAggregationJobStage(jobStageId, agg, aggregator, sink, agg->getOutputType(), myComputation->needsMaterializeOutput());
            //to push back the aggregation stage;
            physicalPlanToOutput.push_back(aggStage);
            //to push back the aggregator set
            interGlobalSets.push_back(aggregator);

        } else {
            std :: cout << "Computation Type: " << myComputation->getComputationType() << " are not supported to have more than one consumers right now" << std :: endl;
            this->logger->fatal("Computation Type: " + myComputation->getComputationType() + " are not supported to have more than one consumers right now");
            return false;
        }

        if (this->dynamicPlanningOrNot == true) {
            this->updateSourceSets(curInputSetIdentifier, sink, curNode);
            return true;

        } else {

            //now I am a source
            //analyze(physicalPlanToOutput, interGlobalSets, curNode, jobStageId);
            buildTheseTupleSets.clear();
            //if ((myComputation->getComputationType() != "SelectionComp") &&  (myComputation->getComputationType() != "MultiSelectionComp")) {
            buildTheseTupleSets.push_back(curNode->getOutputName());
            //}
            for (int i = 0; i < numConsumersForCurNode; i++) {
                AtomicComputationPtr nextNode = consumers[i];
                bool ret = analyze(physicalPlanToOutput, interGlobalSets, buildTheseTupleSets, curNode, myComputation, sink, nextNode, jobStageId, curNode);
                if (ret == false) {
                   return false;
                }
            }
            return true;

        }
    }
}

//to get current source sets;
std :: vector <std :: string> & TCAPAnalyzer :: getCurSourceSetNames () {
    return curSourceSetNames;
}

//to get the source specified by index
std :: string TCAPAnalyzer :: getSourceSetName (int index) {
    return curSourceSetNames[index];
}


//to get source set based on name
Handle<SetIdentifier>  TCAPAnalyzer :: getSourceSetIdentifier (std :: string name) {
    std :: cout << "search set with name = " << name << std :: endl;
    if (curSourceSets.count (name) == 0) {
        std :: cout << "name not found for " << name << std :: endl;
        return nullptr;
    } else {
        return curSourceSets[name];
    }
}

//to get source computation based on name
AtomicComputationPtr TCAPAnalyzer :: getSourceComputation (std :: string name) {
    if (curSourceNodes.count (name) == 0) {
        return nullptr;
    } else {
        return curSourceNodes[name];
    }

}

//to get number of sources
int TCAPAnalyzer :: getNumSources () {
    return curSourceSetNames.size();
}

//to return the index of the best source
int TCAPAnalyzer :: getBestSource (StatisticsPtr stats) {
    //use the cost model to return the index of the best source
    if (stats == 0) {
        return 0;
    }
    else {
       int bestIndexToReturn = 0;
       double minCost = DBL_MAX;
       for (int i = 0; i < curSourceSetNames.size(); i++) {
          double curCost = getCostOfSource(i, stats);
          if (curCost < minCost) {
              minCost = curCost;
              bestIndexToReturn = i;
          }
       }
//       std :: cout << "The Best Source is " << bestIndexToReturn << ": " << curSourceSetNames[bestIndexToReturn] << std :: endl;
       return bestIndexToReturn;
    }
}

//to return the cost of the i-th source
double TCAPAnalyzer :: getCostOfSource (int index, StatisticsPtr stats) {
    //TODO: to get the cost of source specified by the index
    std :: string key = curSourceSetNames[index];
    Handle<SetIdentifier> curSet = this->getSourceSetIdentifier(key);
    double cost =  stats->getNumBytes(curSet->getDatabase(), curSet->getSetName()); 
    cost = double ((int)cost/1000000);
//    std :: cout << "key=" << key << ", cost=" << cost << std :: endl;
    return cost;
}


}

#endif
