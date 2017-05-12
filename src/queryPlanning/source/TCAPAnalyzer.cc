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


namespace pdb {

TCAPAnalyzer::TCAPAnalyzer (std :: string jobId, Handle<Vector<Handle<Computation>>> myComputations, std :: string myTCAPString, PDBLoggerPtr logger) {
    this->jobId = jobId;
    this->computations = myComputations;
    this->tcapString = myTCAPString;
    this->logger = logger;
    try {
        this->computePlan = makeObject<ComputePlan>(String(myTCAPString), *myComputations);
        this->logicalPlan = this->computePlan->getPlan();
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
}

TCAPAnalyzer::~TCAPAnalyzer () {
    this->sources.clear();
    this->logicalPlan = nullptr;
    this->computePlan->nullifyPlanPointer();
    this->computePlan = nullptr;
    
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

//a source computation for a pipeline can be ScanSet, Selection, ClusterAggregation, and ClusterJoin.
bool TCAPAnalyzer::analyze(std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets, AtomicComputationPtr curSource, int & jobStageId) {
    //first get source set identifier
    std :: string sourceSpecifier = curSource->getComputationName();
    Handle<SetIdentifier> curInputSetIdentifier = nullptr;    
    Handle<Computation> sourceComputation = this->computePlan->getPlan()->getNode(sourceSpecifier).getComputationHandle();
    if ((sourceComputation->getComputationType() == "ScanUserSet") || (sourceComputation->getComputationType() == "ScanSet")){
        Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(sourceComputation);
        curInputSetIdentifier = makeObject<SetIdentifier>(scanner->getDatabaseName(), scanner->getSetName());
    } else if (sourceComputation->getComputationType() == "ClusterAggregationComp") {
        Handle<AbstractAggregateComp> aggregator = unsafeCast<AbstractAggregateComp, Computation>(sourceComputation);
        curInputSetIdentifier = makeObject<SetIdentifier>(aggregator->getDatabaseName(), aggregator->getSetName());

    } else if (sourceComputation->getComputationType() == "SelectionComp") {
        Handle<SelectionComp<Object,Object>> selector = unsafeCast<SelectionComp<Object,Object>, Computation>(sourceComputation);
        curInputSetIdentifier = makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
   
    } else  {
        std :: cout << "Source Computation Type: " << sourceComputation->getComputationType() << " are not supported as source node right now" << std :: endl;
        this->logger->fatal("Source Computation Type: " + sourceComputation->getComputationType() + " are not supported as source node right now");
    }

    std :: string outputName = curSource->getOutputName();
    std :: vector<AtomicComputationPtr> consumers = this->computationGraph.getConsumingAtomicComputations(outputName);
    int numConsumersForCurNode = consumers.size();
    for (int i = 0; i < numConsumersForCurNode; i++) {
        AtomicComputationPtr curNode = consumers[i];
        bool ret = analyze(physicalPlanToOutput, interGlobalSets, curSource, sourceComputation, curInputSetIdentifier, curNode, jobStageId);
        if (ret == false) {
            return false;
        }
    }
    return true;
}

Handle<TupleSetJobStage>  TCAPAnalyzer::createTupleSetJobStage(int & jobStageId, std :: string sourceTupleSetName, std :: string targetTupleSetName, std :: string targetComputationName, std :: string outputTypeName, Handle<SetIdentifier> sourceContext, Handle<SetIdentifier> combinerContext, Handle<SetIdentifier> sinkContext, bool isBroadcasting, bool isRepartitioning, bool needsRemoveInputSet) {
    Handle<TupleSetJobStage> jobStage = makeObject<TupleSetJobStage> (jobStageId);
    jobStageId ++;
    jobStage->setComputePlan(this->computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
    jobStage->setSourceContext(sourceContext);
    jobStage->setSinkContext(sinkContext);
    jobStage->setOutputTypeName(outputTypeName);
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


bool TCAPAnalyzer::analyze (std :: vector<Handle<AbstractJobStage>> & physicalPlanToOutput, std :: vector<Handle<SetIdentifier>> & interGlobalSets, AtomicComputationPtr curSource, Handle<Computation> sourceComputation, Handle<SetIdentifier> curInputSetIdentifier, AtomicComputationPtr curNode, int &jobStageId) {
    //to get consumers
    std :: string outputName = curNode->getOutputName();
    std :: vector<AtomicComputationPtr> consumers = this->computationGraph.getConsumingAtomicComputations(outputName);
    int numConsumersForCurNode = consumers.size();
     
    //to get my type
    std :: string mySpecifier = curNode->getComputationName();
    Handle<Computation> myComputation = this->computePlan->getPlan()->getNode(mySpecifier).getComputationHandle();

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
            Handle<SetIdentifier>combiner = makeObject<SetIdentifier>(this->jobId, outputName+"_combinerData");
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage(jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier,"Aggregation", curInputSetIdentifier, combiner, aggregator, false, true, false); 
            //to push back the job stage
            physicalPlanToOutput.push_back(jobStage);
            //to create the consuming job stage for aggregation
            Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(myComputation);
            Handle<AggregationJobStage> aggStage = createAggregationJobStage(jobStageId, agg, aggregator, sink, agg->getOutputType(), true);
            //to push back the aggregation stage;
            physicalPlanToOutput.push_back(aggStage);
            //to push back the aggregator set
            interGlobalSets.push_back(aggregator);
            return true;

        } else if ((myComputation->getComputationType() == "WriteUserSet") || (myComputation->getComputationType() == "SelectionComp")) {
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier, myComputation->getOutputType(), curInputSetIdentifier, nullptr, sink, false, false, false);
            physicalPlanToOutput.push_back(jobStage);
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
            Handle<SetIdentifier> combiner = makeObject<SetIdentifier>(this->jobId, outputName+"_combinerData");
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier, "Aggregation", curInputSetIdentifier, combiner, aggregator, false, true, false);
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

            //Now I am the source!
            return analyze(physicalPlanToOutput, interGlobalSets, curNode, myComputation, sink, nextNode, jobStageId);

        } else if (curNode->getAtomicComputationType() == "JoinSets") {
            //if all my inputs are processed, I am not a pipeline breaker, we should create a TupleSetJobStage, and set the correct hash set names for probing
            

            //if at least one of my inputs are not processed, I am a pipeline breaker.
            //We first need to create a TupleSetJobStage with a broadcasting sink
            //We then create a BroadcastJoinBuildHTStage
            //We should not go further, we set it to untraversed and leave it to other join inputs, and simply return
            return false;

        } else {
            //I am not a pipeline breaker
            return analyze(physicalPlanToOutput, interGlobalSets, curSource, sourceComputation, curInputSetIdentifier, nextNode, jobStageId);
            
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
        if (myComputation->getComputationType() == "SelectionComp") {
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier, myComputation->getOutputType(), curInputSetIdentifier, nullptr, sink, false, false, false);
            physicalPlanToOutput.push_back(jobStage);
        } else if (myComputation->getComputationType() == "ClusterAggregationComp") {
            Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(this->jobId, outputName+"_aggregationData");
            Handle<SetIdentifier> combiner = makeObject<SetIdentifier>(this->jobId, outputName+"_combinerData");
            Handle<TupleSetJobStage> jobStage = createTupleSetJobStage (jobStageId, curSource->getOutputName(), curNode->getInputName(), mySpecifier, "Aggregation", curInputSetIdentifier, combiner, aggregator, false, true, false);
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
        //now I am a source
        //analyze(physicalPlanToOutput, interGlobalSets, curNode, jobStageId);       
        for (int i = 0; i < numConsumersForCurNode; i++) {
            AtomicComputationPtr nextNode = consumers[i];
            bool ret = analyze(physicalPlanToOutput, interGlobalSets, curNode, myComputation, sink, nextNode, jobStageId);
            if (ret == false) {
                return false;
            }
        }
        return true;
    }
}




}

#endif
