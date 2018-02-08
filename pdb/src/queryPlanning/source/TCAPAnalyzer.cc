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
#include "JoinComp.h"
#include "MultiSelectionComp.h"
#include "ScanUserSet.h"
#include "SelectionComp.h"
#include <cfloat>

#ifndef JOIN_COST_THRESHOLD
#define JOIN_COST_THRESHOLD 15000
#endif
namespace pdb {

TCAPAnalyzer::TCAPAnalyzer(std::string &jobId,
                           Handle<Vector<Handle<Computation>>> computations,
                           std::string TCAPString,
                           PDBLoggerPtr logger,
                           ConfigurationPtr &conf) {
  this->jobId = jobId;
  this->computations = computations;
  this->tcapString = TCAPString;
  this->logger = logger;
  this->conf = conf;

  try {
    // parse the plan and initialize the values we need
    this->computePlan = makeObject<ComputePlan>(String(TCAPString), *computations);
    this->logicalPlan = this->computePlan->getPlan();
    this->computePlan->nullifyPlanPointer();
    this->computationGraph = this->logicalPlan->getComputations();
    this->sources = this->computationGraph.getAllScanSets();

  } catch (pdb::NotEnoughSpace &n) {
    PDB_COUT << "FATAL ERROR in TCAPAnalyzer: Not enough memory to allocate the computePlan object";
    logger->fatal("FATAL ERROR in TCAPAnalyzer: Not enough memory to allocate the computePlan object");
    this->computePlan = nullptr;
    this->logicalPlan = nullptr;
    this->sources.clear();
  }

  hashSetsToProbe = nullptr;

  // initialize source sets and source nodes
  initializeSourceSets();

  // go through all the source set names and print them
  printSourceSetNames();
}

TCAPAnalyzer::~TCAPAnalyzer() {
  this->sources.clear();
  this->logicalPlan = nullptr;
  this->computePlan->nullifyPlanPointer();
  this->computePlan = nullptr;
  this->hashSetsToProbe = nullptr;
}

// to analyze the sub-graph rooted at a source node and only returns a set of job
// stages corresponding with the sub-graph
bool TCAPAnalyzer::getNextStagesOptimized(std::vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
                                          std::vector<Handle<SetIdentifier>> &interGlobalSets,
                                          StatisticsPtr &stats,
                                          int &jobStageId) {

  // analyze all sources and select a source based on cost model
  // get the job stages and intermediate data sets for this source
  std::string sourceName = getBestSource(stats);

  // grab the TCAP computation for this source
  AtomicComputationPtr curSource = getSourceComputation(sourceName);

  // grabs the name of this computation
  std::string sourceSpecifier = curSource->getComputationName();

  // grab name of the tuple set produced by this source
  std::string outputName = curSource->getOutputName();

  // for this source we grab it's corresponding set identifier
  Handle<SetIdentifier> sourceSetIdentifier = getSourceSetIdentifier(sourceName);

  // acquire a handle to the real computation object for this source
  Handle<Computation> sourceComputation = this->logicalPlan->getNode(sourceSpecifier).getComputationHandle();

  // get a list of all all TCAP computations that consume this source
  std::vector<AtomicComputationPtr> consumers = this->computationGraph.getConsumingAtomicComputations(outputName);

  // this source might have multiple consumers we get the index of the next in line to be processed
  unsigned int curConsumerIndex = getNextConsumerIndex(sourceName);

  // if the next consumer index is larger then the actual number of consumers then this source is not used anymore
  if (curConsumerIndex >= consumers.size()) {
    PDB_COUT << "The source we selected has no more consumers, so we are removing it!" << sourceName << "\n";
    removeSource(sourceName);
    return false;
  }

  // grab the TCAP computation for the consumer
  AtomicComputationPtr consumerNode = consumers[curConsumerIndex];

  // each computation in TCAP creates a tuple set the output name of the source computation is the first one that needs
  // to be created. In a pipeline we mostly care about the source and the sink tuple set, but we need the intermediate
  // set names to get the associated TCAP computations when we are building the pipeline
  std::vector<std::string> tupleSetNames = {outputName};

  // perform the actual physical planning
  bool success = analyze(physicalPlanToOutput,
                         interGlobalSets,
                         tupleSetNames,
                         consumerNode,
                         curSource,
                         curSource,
                         sourceComputation,
                         sourceSetIdentifier,
                         jobStageId,
                         false,
                         "",
                         defaultAllocator);

  // check if we succeeded in generating a sequence of stages
  if (success) {
    // if we have then the source has one less consumer
    incrementConsumerIndex(sourceName);
  }

  return success;
}

// to get if there are any sources
bool TCAPAnalyzer::hasSources() {
  return !curSourceSetNames.empty();
}

bool TCAPAnalyzer::hasConsumers(std::string &name) {
  return getNumConsumers(name) > 0;
}

void TCAPAnalyzer::initializeSourceSets() {

  // go through each source computation of the TCAP
  for (const AtomicComputationPtr &curSource : this->sources) {

    // grab the name of the computation
    std::string sourceSpecifier = curSource->getComputationName();

    // grab the actual computation that corresponds to this source
    Handle<Computation> sourceComputation = this->logicalPlan->getNode(sourceSpecifier).getComputationHandle();

    // create a set identifier for this computation
    Handle<SetIdentifier> curInputSetIdentifier = getSetIdentifierFromComputation(sourceComputation);

    // grab the source set name from this set identifier "databaseName:setName"
    std::string mySourceSetName = curInputSetIdentifier->toSourceSetName();

    // log the source set name
    std::cout << "mySourceSetName=" << mySourceSetName << std::endl;

    // try to find the mySourceSetName in the source vector
    auto it = find(curSourceSetNames.begin(), curSourceSetNames.end(), mySourceSetName);

    // do we have that source set already stored if not create a new entry
    if (it == curSourceSetNames.end()) {

      // log that we are adding a new source
      PDB_COUT << std::distance(curSourceSetNames.begin(), it) << ": add new source: " << mySourceSetName << "\n";

      // add the now source name
      curSourceSetNames.push_back(mySourceSetName);

      // store it's set identifier
      curSourceSets[mySourceSetName] = curInputSetIdentifier;

      // set the number of the processed (already executed consumers) to zero since we are just starting
      curProcessedConsumers[mySourceSetName] = 0;

      // store the computation that uses this source
      curSourceNodes[mySourceSetName].push_back(curSource);

      continue;
    }

    // ok we already have the node log that we are adding just the computation to the curSourceNodes
    PDB_COUT << "curSourceSetName[" << std::distance(curSourceSetNames.begin(), it) << "]=" << *it << "\n";
    PDB_COUT << "add new computation for source: " << mySourceSetName << "\n";

    // store the computation that uses this source
    curSourceNodes[mySourceSetName].push_back(curSource);
  }
}

void TCAPAnalyzer::printSourceSetNames() const {

  // go through all the set names and print them
  cout << "All current sources: " << endl;
  for (const string &sourceSetName : curSourceSetNames) {
    cout << sourceSetName << endl;
  }
}

// to get number of consumers for a certain source
unsigned int TCAPAnalyzer::getNumConsumers(std::string &name) {

  // do we even have a source set with this name if not the number of consumer is zero
  if (this->getSourceSetIdentifier(name) == nullptr) {
    return 0;
  }

  // figure out the initial number of consumers
  std::string outputName = this->getSourceComputation(name)->getOutputName();
  int initialNumber = (int) this->computationGraph.getConsumingAtomicComputations(outputName).size();

  // grab the number of processed consumers
  unsigned int processedConsumerNumber = this->getNextConsumerIndex(name);

  // the current number of consumers is equal to the initial number of consumers minus the number of processed consumers
  return initialNumber - processedConsumerNumber;
}

Handle<SetIdentifier> TCAPAnalyzer::getSetIdentifierFromComputation(Handle<Computation> computation) {

  switch (computation->getComputationTypeID()) {
    case ScanUserSetTypeID :
    case ScanSetTypeID : {

      // this is a ScanUserSet cast it
      Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(computation);

      // create a set identifier from it
      return makeObject<SetIdentifier>(scanner->getDatabaseName(), scanner->getSetName());
    }
    case ClusterAggregationCompTypeID : {

      // this is an AbstractAggregateComp cast it
      Handle<AbstractAggregateComp> aggregator = unsafeCast<AbstractAggregateComp, Computation>(computation);

      // create the set identifier from it
      return makeObject<SetIdentifier>(aggregator->getDatabaseName(), aggregator->getSetName());
    }
    case SelectionCompTypeID : {

      // this is a SelectionComp cast it
      Handle<SelectionComp<Object, Object>>
          selector = unsafeCast<SelectionComp<Object, Object>, Computation>(computation);

      // create the set identifier from it
      return makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
    }
    case MultiSelectionCompTypeID : {

      // this is a MultiSelectionComp cast it
      Handle<MultiSelectionComp<Object, Object>>
          selector = unsafeCast<MultiSelectionComp<Object, Object>, Computation>(computation);

      // create the set identifier from it
      return makeObject<SetIdentifier>(selector->getDatabaseName(), selector->getSetName());
    }
    default: {
      // this is bad, we can not cast this thing...
      PDB_COUT << "Source Computation Type: " << computation->getComputationType()
               << " are not supported as source node right now" << std::endl;
      this->logger->fatal("Source Computation Type: " + computation->getComputationType() +
          " are not supported as source node right now");
      PDB_COUT << "Master exit...Please restart cluster\n";
      exit(1); // TODO we are killing the server for a bad query might not be smart!
    }
  }
}

bool TCAPAnalyzer::analyze(std::vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
                           std::vector<Handle<SetIdentifier>> &interGlobalSets,
                           std::vector<std::string> &buildTheseTupleSets,
                           AtomicComputationPtr curNode,
                           AtomicComputationPtr prevNode,
                           AtomicComputationPtr curSource,
                           Handle<Computation> sourceComputation,
                           Handle<SetIdentifier> curInputSetIdentifier,
                           int &jobStageId,
                           bool isProbing,
                           std::string joinSource,
                           AllocatorPolicy allocatorPolicy) {

  // grab the output of the current node
  std::string outputName = curNode->getOutputName();

  // grab all the consumers (TCAP) of the current node
  std::vector<AtomicComputationPtr> consumers = this->computationGraph.getConsumingAtomicComputations(outputName);
  size_t numConsumersForCurNode = consumers.size();

  // grab the name of the real computation
  std::string computationSpecifier = curNode->getComputationName();

  // grab the real computation from the logical plan
  Handle<Computation> curComp = this->logicalPlan->getNode(computationSpecifier).getComputationHandle();

  // set the allocation policy for this computation
  allocatorPolicy = curComp->getAllocatorPolicy();

  if (numConsumersForCurNode == 0) {

    // we are dealing with a computation that is outputted,
    // in that case we just need to check if it requires an aggregation or not
    return analyzeOutputComputation(physicalPlanToOutput,
                                    interGlobalSets,
                                    buildTheseTupleSets,
                                    curSource,
                                    curInputSetIdentifier,
                                    curNode,
                                    jobStageId,
                                    isProbing,
                                    allocatorPolicy,
                                    joinSource,
                                    outputName,
                                    computationSpecifier,
                                    curComp);

  } else if (numConsumersForCurNode == 1) {

    // we are dealing with a computation that has only one consumer.
    // this could be a pipeline breaker or not, therefore we need to analyze further
    return analyzeSingleConsumerComputation(physicalPlanToOutput,
                                            interGlobalSets,
                                            buildTheseTupleSets,
                                            curSource,
                                            sourceComputation,
                                            curInputSetIdentifier,
                                            curNode,
                                            jobStageId,
                                            prevNode,
                                            isProbing,
                                            allocatorPolicy,
                                            joinSource,
                                            outputName,
                                            consumers,
                                            computationSpecifier,
                                            curComp);

  } else {

    // we have multiple consumers, this is definitely a pipeline breaker.
    // in that case we just need to check if it requires an aggregation or not
    return analyzeMultiConsumerComputation(physicalPlanToOutput,
                                           interGlobalSets,
                                           buildTheseTupleSets,
                                           curSource,
                                           curInputSetIdentifier,
                                           curNode,
                                           jobStageId,
                                           isProbing,
                                           allocatorPolicy,
                                           joinSource,
                                           outputName,
                                           computationSpecifier,
                                           curComp);

  }
}

bool TCAPAnalyzer::analyzeMultiConsumerComputation(vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
                                                   vector<Handle<SetIdentifier>> &interGlobalSets,
                                                   vector<string> &buildTheseTupleSets,
                                                   AtomicComputationPtr &curSource,
                                                   const Handle<SetIdentifier> &curInputSetIdentifier,
                                                   AtomicComputationPtr &curNode,
                                                   int &jobStageId,
                                                   bool isProbing,
                                                   const AllocatorPolicy &allocatorPolicy,
                                                   string &joinSource,
                                                   const string &outputName,
                                                   const string &computationSpecifier,
                                                   Handle<Computation> &curComp) {

  // I am a pipeline breaker because I have more than one consumers
  Handle<SetIdentifier> sink = nullptr;

  // in the case that the current computation does not require materialization by default
  // we have to set an output to it, we it gets materialized
  if (!curComp->needsMaterializeOutput()) {

    // set the output
    curComp->setOutput(jobId, outputName);

    // create the sink and set the page size
    sink = makeObject<SetIdentifier>(jobId, outputName);
    sink->setPageSize(conf->getPageSize());

    // add this set to the list of intermediate sets
    interGlobalSets.push_back(sink);
  } else {
    // this computation needs materialization either way so just create the sink set identifier
    sink = makeObject<SetIdentifier>(curComp->getDatabaseName(), curComp->getSetName());
  }

  switch (curComp->getComputationTypeID()) {
    case ClusterAggregationCompTypeID: {

      // create the set identifier where we store the data to be aggregated after the TupleSetJobStage
      Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(jobId, outputName + "_aggregationData");
      aggregator->setPageSize(conf->getShufflePageSize());

      // are we using a combiner (the thing that combines the records by key before sending them to the right node)
      Handle<SetIdentifier> combiner = nullptr;
      if (curComp->isUsingCombiner()) {
        // create a set identifier
        combiner = makeObject<SetIdentifier>(jobId, outputName + "_combinerData");
      }

      // grab the source tuple set
      string sourceTupleSetName = curSource->getOutputName();

      // did we previously create a tuple stage that was probing a partitioned hash table
      if (!joinSource.empty()) {
        sourceTupleSetName = joinSource;
        joinSource = "";
      }

      // create the tuple set job stage to run the pipeline with a shuffle sink
      // here is what we are doing :
      // the input to the stage is either the output of the join or the source node we started)
      // the repartitioning flag is set to true, so that we run a pipeline with a shuffle sink
      // the pipeline until the combiner will apply all the computations to the source set
      // and put them on a page partitioned into multiple maps the combiner will then read the maps that belong to
      // the partitions of a certain node and combine them by key. The output pages of the combiner will then be sent
      // to a the appropriate node since the isCollectAsMap = false (dafault parameter)
      Handle<TupleSetJobStage> jobStage = createTupleSetJobStage(jobStageId,
                                                                 sourceTupleSetName,
                                                                 curNode->getInputName(),
                                                                 computationSpecifier,
                                                                 buildTheseTupleSets,
                                                                 "IntermediateData",
                                                                 curInputSetIdentifier,
                                                                 combiner,
                                                                 aggregator,
                                                                 false,
                                                                 true,
                                                                 isProbing,
                                                                 allocatorPolicy);

      // add the created tuple job stage to the
      physicalPlanToOutput.emplace_back(jobStage);

      // cast the computation to AbstractAggregateComp to create the consuming job stage for aggregation
      Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(curComp);

      // we need an aggregation stage after this to aggregate the results from the tuple stage we previously created
      // the data will be aggregated in the sink set
      Handle<AggregationJobStage> aggStage = createAggregationJobStage(jobStageId,
                                                                       agg,
                                                                       aggregator,
                                                                       sink,
                                                                       curComp->needsMaterializeOutput());

      // to push back the aggregation stage;
      physicalPlanToOutput.emplace_back(aggStage);

      // to push back the aggregator set
      interGlobalSets.push_back(aggregator);

      break;
    }
    default: {

      // add the output name of the current TCAP computation to the buildTheseTupleSets so we apply this computation
      // in the tuple stage
      buildTheseTupleSets.push_back(curNode->getOutputName());

      // the input to the pipeline is the output set of the source node
      string sourceTupleSetName = curSource->getOutputName();

      // did we previously create a tuple stage that was probing a partitioned hash table
      if (!joinSource.empty()) {
        // if so we are using that as the source
        sourceTupleSetName = joinSource;
        joinSource = "";
      }

      // create the tuple job stage to execute all the computations in the pipeline
      Handle<TupleSetJobStage> jobStage = createTupleSetJobStage(jobStageId,
                                                                 sourceTupleSetName,
                                                                 curNode->getOutputName(),
                                                                 computationSpecifier,
                                                                 buildTheseTupleSets,
                                                                 curComp->getOutputType(),
                                                                 curInputSetIdentifier,
                                                                 nullptr,
                                                                 sink,
                                                                 false,
                                                                 false,
                                                                 isProbing,
                                                                 allocatorPolicy);


      // add add the stage to the list of stages to be executed
      physicalPlanToOutput.emplace_back(jobStage);

      break;
    }
  }

  // update the source sets to reflect the state after executing the job stages
  updateSourceSets(curInputSetIdentifier, sink, curNode);

  return true;
}

bool TCAPAnalyzer::analyzeOutputComputation(vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
                                            vector<Handle<SetIdentifier>> &interGlobalSets,
                                            const vector<string> &buildTheseTupleSets,
                                            AtomicComputationPtr &curSource,
                                            const Handle<SetIdentifier> &curInputSetIdentifier,
                                            AtomicComputationPtr &curNode,
                                            int &jobStageId,
                                            bool isProbing,
                                            const AllocatorPolicy &myPolicy,
                                            string &joinSource,
                                            const string &outputName,
                                            const string &computationSpecifier,
                                            Handle<Computation> &comp) {

  // create a SetIdentifier for the output set
  Handle<SetIdentifier> sink = makeObject<SetIdentifier>(comp->getDatabaseName(), comp->getSetName());

  // check whether we need an AggregationJobStage or not
  switch (comp->getComputationTypeID()) {
    case ClusterAggregationCompTypeID: {

      // create the set identifier where we store the data to be aggregated after the TupleSetJobStage
      Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(jobId, outputName + "_aggregationData");
      aggregator->setPageSize(conf->getShufflePageSize());

      Handle<SetIdentifier> combiner = nullptr;
      // are we using a combiner (the thing that combines the records by key before sending them to the right node)
      if (comp->isUsingCombiner()) {
        combiner = makeObject<SetIdentifier>(jobId, outputName + "_combinerData");
      }

      // did we previously create a tuple stage that was probing a partitioned hash table
      string sourceTupleSetName = curSource->getOutputName();
      if (!joinSource.empty()) {
        sourceTupleSetName = joinSource;
        joinSource = "";
      }

      // cast the computation to AbstractAggregateComp to create the consuming job stage for aggregation
      Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(comp);

      // create the tuple set job stage to run the pipeline with a shuffle sink
      // here is what we are doing :
      // the input to the stage is either the output of the join or the source node we started)
      // the repartitioning flag is set to true, so that we run a pipeline with a shuffle sink
      // the pipeline until the combiner will apply all the computations to the source set
      // and put them on a page partitioned into multiple maps the combiner will then read the maps that belong to
      // the partitions of a certain node and combine them by key. The output pages of the combiner will then be sent
      // to the appropriate nodes depending on the parameters isCollectAsMap and getNumNodesToCollect
      Handle<TupleSetJobStage> jobStage = createTupleSetJobStage(jobStageId,
                                                                 sourceTupleSetName,
                                                                 curNode->getInputName(),
                                                                 computationSpecifier,
                                                                 buildTheseTupleSets,
                                                                 "IntermediateData",
                                                                 curInputSetIdentifier,
                                                                 combiner,
                                                                 aggregator,
                                                                 false,
                                                                 true,
                                                                 isProbing,
                                                                 myPolicy,
                                                                 false,
                                                                 agg->isCollectAsMap(),
                                                                 agg->getNumNodesToCollect());
      // to push back the job stage
      physicalPlanToOutput.emplace_back(jobStage);

      // to create the consuming job stage for aggregation
      Handle<AggregationJobStage> aggStage = createAggregationJobStage(jobStageId,
                                                                       agg,
                                                                       aggregator,
                                                                       sink,
                                                                       true);
      // to push back the aggregation stage;
      physicalPlanToOutput.emplace_back(aggStage);

      // to push back the aggregator set
      interGlobalSets.push_back(aggregator);

      // update the source sets (if the source node is not being used anymore we just remove it)
      updateSourceSets(curInputSetIdentifier, nullptr, nullptr);

      return true;
    }
    case WriteUserSetTypeID:
    case SelectionCompTypeID:
    case MultiSelectionCompTypeID: {

      // the input to the pipeline is the output set of the source node
      string sourceTupleSetName = curSource->getOutputName();

      // did we previously create a tuple stage that was probing a partitioned hash table
      if (!joinSource.empty()) {
        sourceTupleSetName = joinSource;
        joinSource = "";
      }

      // create the tuple job stage to execute all the computations in the pipeline
      Handle<TupleSetJobStage> jobStage = createTupleSetJobStage(jobStageId,
                                                                 sourceTupleSetName,
                                                                 curNode->getInputName(),
                                                                 computationSpecifier,
                                                                 buildTheseTupleSets,
                                                                 comp->getOutputType(),
                                                                 curInputSetIdentifier,
                                                                 nullptr,
                                                                 sink,
                                                                 false,
                                                                 false,
                                                                 isProbing,
                                                                 myPolicy);

      // add add the stage to the list of stages to be executed
      physicalPlanToOutput.emplace_back(jobStage);

      // update the source sets (if the source node is not being used anymore we just remove it)
      updateSourceSets(curInputSetIdentifier, nullptr, nullptr);

      return true;
    }
    default: {
      cout << "Sink Computation Type: " << comp->getComputationType()
           << " are not supported as sink node right now" << endl;
      logger->fatal("Source Computation Type: " + comp->getComputationType() +
          " are not supported as sink node right now");
      exit(1);
    }
  }
}

bool TCAPAnalyzer::analyzeSingleConsumerComputation(vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
                                                    vector<Handle<SetIdentifier>> &interGlobalSets,
                                                    vector<string> &buildTheseTupleSets,
                                                    AtomicComputationPtr &curSource,
                                                    const Handle<Computation> &sourceComputation,
                                                    const Handle<SetIdentifier> &curInputSetIdentifier,
                                                    AtomicComputationPtr &curNode,
                                                    int &jobStageId,
                                                    AtomicComputationPtr &prevNode,
                                                    bool isProbing,
                                                    const AllocatorPolicy &allocationPolicy,
                                                    string &joinSource,
                                                    const string &outputName,
                                                    const vector<AtomicComputationPtr> &consumers,
                                                    const string &computationSpecifier,
                                                    Handle<Computation> &curComp) {


  // we need to handle three situations
  // 1. If the current node is an aggregate (ApplyAgg) this is definitely a pipeline breaker
  // 2. If the current node is a join (ApplyJoin), we need to check whether this is a pipeline breaker or not
  // 3. If it it is any other type of TCAP computation we are just fine and we can absorb it into the pipeline
  switch (curNode->getAtomicComputationTypeID()) {
    case ApplyAggTypeID: {

      // so we are dealing with an aggregation that has only one consumer, handle that
      return handleSingleConsumerAggregation(physicalPlanToOutput,
                                             interGlobalSets,
                                             buildTheseTupleSets,
                                             curSource,
                                             curInputSetIdentifier,
                                             curNode,
                                             jobStageId,
                                             isProbing,
                                             allocationPolicy,
                                             joinSource,
                                             outputName,
                                             computationSpecifier,
                                             curComp);

    }
    case ApplyJoinTypeID: {

      // we are dealing with a join that has only one consumer, handle that
      return handleSingleConsumerJoin(physicalPlanToOutput,
                                      interGlobalSets,
                                      buildTheseTupleSets,
                                      curSource,
                                      sourceComputation,
                                      curInputSetIdentifier,
                                      curNode,
                                      jobStageId,
                                      prevNode,
                                      isProbing,
                                      allocationPolicy,
                                      joinSource,
                                      outputName,
                                      consumers,
                                      computationSpecifier,
                                      curComp);

    }
    default: {

      // we have only one consumer node so we know what the next node in line is
      AtomicComputationPtr nextNode = consumers[0];

      // I am not a pipeline breaker just add me to the buildTheseTupleSets so I get applied
      buildTheseTupleSets.push_back(curNode->getOutputName());

      // go to the next TCAP computation that is consuming this one
      return analyze(physicalPlanToOutput,
                     interGlobalSets,
                     buildTheseTupleSets,
                     nextNode,
                     curNode,
                     curSource,
                     sourceComputation,
                     curInputSetIdentifier,
                     jobStageId,
                     isProbing,
                     joinSource,
                     allocationPolicy);
    }
  }
}
bool TCAPAnalyzer::handleSingleConsumerJoin(vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
                                            vector<Handle<SetIdentifier>> &interGlobalSets,
                                            vector<string> &buildTheseTupleSets,
                                            AtomicComputationPtr &curSource,
                                            const Handle<Computation> &sourceComputation,
                                            const Handle<SetIdentifier> &curInputSetIdentifier,
                                            AtomicComputationPtr &curNode,
                                            int &jobStageId,
                                            AtomicComputationPtr &prevNode,
                                            bool isProbing,
                                            const AllocatorPolicy &allocatorPolicy,
                                            string &joinSource,
                                            const string &outputName,
                                            const vector<AtomicComputationPtr> &consumers,
                                            const string &computationSpecifier,
                                            Handle<Computation> &curComp) {

  shared_ptr<ApplyJoin> joinNode = dynamic_pointer_cast<ApplyJoin>(curNode);

  // TODO Jia : have no clue why this exists?
  string targetTupleSetName = prevNode == nullptr ? curNode->getInputName() : prevNode->getOutputName();

  Handle<SetIdentifier> sink = nullptr;

  // We have two main cases to handle when dealing with a join
  // 1. This is the first time we are processing this join therefore no side of the join has been hashed
  // and then broadcasted or partitioned, therefore we can not probe it
  // 2. This is the second time we are processing this join therefore the one side of the join is hashed and then
  // broadcasted or partitioned, we can therefore probe it!
  if (!joinNode->isTraversed()) {

    // are we already probing a set in the pipeline, and is the cost of the current source smaller than the join
    // threshold? if so might be better to go back and process the other side of the join first
    if (isProbing && (costOfCurSource <= JOIN_COST_THRESHOLD)) {

      // clear the list of tuple sets to build
      buildTheseTupleSets.clear();

      // add this source to he list of penalized source sets
      if (std::find(penalizedSourceSets.begin(), penalizedSourceSets.end(), curSourceSetName)
          == penalizedSourceSets.end()) {
        PDB_COUT << "WARNING: met a join sink with probing, to return and put "
                 << curSourceSetName << " to penalized list\n";
        penalizedSourceSets.push_back(curSourceSetName);
      }

      // we return false to signalize that we did no extract a pipeline
      return false;
    }

    string hashSetName;

    // does the cost of the current source exceed the join threshold, if so we need to do a hash partition join
    // therefore we definitely need to hash the current table this is definitely a pipeline breaker
    if (costOfCurSource > JOIN_COST_THRESHOLD) {

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

      // did we previously create a tuple stage that was shuffling the data so we can probe a partitioned hash set
      string sourceTupleSetName = curSource->getOutputName();
      if (!joinSource.empty()) {
        sourceTupleSetName = joinSource;
        joinSource = "";
      }

      // create the tuple stage to run a pipeline with a hash partition sink
      Handle<TupleSetJobStage> joinPrepStage = createTupleSetJobStage(jobStageId,
                                                                      sourceTupleSetName,
                                                                      targetTupleSetName,
                                                                      computationSpecifier,
                                                                      buildTheseTupleSets,
                                                                      "IntermediateData",
                                                                      curInputSetIdentifier,
                                                                      nullptr,
                                                                      sink,
                                                                      false,
                                                                      true,
                                                                      isProbing,
                                                                      allocatorPolicy,
                                                                      true);

      // add the stage to the list of stages to be executed
      physicalPlanToOutput.emplace_back(joinPrepStage);

      // add the sink to the intermediate sets
      interGlobalSets.push_back(sink);

      // the source set for the HashPartitionedJoinBuildHTJobStage is the sink set of the TupleSetJobStage
      hashSetName = sink->toSourceSetName();

      // create the build hash partitioned join hash table job stage to partition and shuffle the source set
      Handle<HashPartitionedJoinBuildHTJobStage> joinPartitionStage = createHashPartitionedJoinBuildHTJobStage(jobStageId,
                                                                                                               sourceTupleSetName,
                                                                                                               targetTupleSetName,
                                                                                                               computationSpecifier,
                                                                                                               sink,
                                                                                                               hashSetName);
      // add the stage to the list of stages to be executed
      physicalPlanToOutput.emplace_back(joinPartitionStage);

    } else {

      // The other input has not been processed and we can do broadcasting because
      // costOfCurSource <= JOIN_COST_THRESHOLD. I am a pipeline breaker.
      // We first need to create a TupleSetJobStage with a broadcasting sink
      // then a BroadcastJoinBuildHTJobStage to build a hash table of that data.

      // the set identifier of the set where we store the output of the TupleSetJobStage
      sink = makeObject<SetIdentifier>(jobId, outputName + "_broadcastData");
      sink->setPageSize(conf->getBroadcastPageSize());

      // did we previously create a tuple stage that was shuffling the data so we can probe a partitioned hash set
      string sourceTupleSetName = curSource->getOutputName();
      if (!joinSource.empty()) {
        sourceTupleSetName = joinSource;
        joinSource = "";
      }

      // We are setting isBroadcasting to true so that we run a pipeline with broadcast sink
      Handle<TupleSetJobStage> joinPrepStage = createTupleSetJobStage(jobStageId,
                                                                      sourceTupleSetName,
                                                                      targetTupleSetName,
                                                                      computationSpecifier,
                                                                      buildTheseTupleSets,
                                                                      "IntermediateData",
                                                                      curInputSetIdentifier,
                                                                      nullptr,
                                                                      sink,
                                                                      true,
                                                                      false,
                                                                      isProbing,
                                                                      allocatorPolicy);

      // add the stage to the list of stages to be executed
      physicalPlanToOutput.emplace_back(joinPrepStage);

      // add the sink to the intermediate sets
      interGlobalSets.push_back(sink);

      // We then create a BroadcastJoinBuildHTStage
      hashSetName = sink->toSourceSetName();
      Handle<BroadcastJoinBuildHTJobStage> joinBroadcastStage = createBroadcastJoinBuildHTJobStage(jobStageId,
                                                                                                   sourceTupleSetName,
                                                                                                   targetTupleSetName,
                                                                                                   computationSpecifier,
                                                                                                   sink,
                                                                                                   hashSetName);
      // add the stage to the list of stages to be executed
      physicalPlanToOutput.emplace_back(joinBroadcastStage);
    }

    // set the probe information
    hashSetsToProbe = hashSetsToProbe != nullptr ? hashSetsToProbe : makeObject<Map<String, String>>();

    (*hashSetsToProbe)[outputName] = hashSetName;
    // We should not go further, we set it to traversed and leave it to
    // other join
    // inputs, and simply return
    joinNode->setTraversed(true);

    // update the source sets (if the source node is not being used anymore we just remove it)
    updateSourceSets(curInputSetIdentifier, nullptr, nullptr);

    // return to indicate the we succeeded
    return true;
  } else {

    // at this point we know that the other side of the join has been processed now we need to figure out how?
    // There are two options :
    // 1. is that it has been partitioned, in this case we are shuffling and then probing a partitioned hash table
    // 2. is that it has been broadcasted, in that case we are probing a broadcasted hash table

    // check if the other side has been partitioned
    if (joinNode->isPartitioningLHS()) {

      // we have only one consumer node so we know what the next node in line is
      AtomicComputationPtr nextNode = consumers[0];

      // we meed to shuffle the data and store it here
      sink = makeObject<SetIdentifier>(jobId, outputName + "_repartitionData");
      sink->setPageSize(conf->getBroadcastPageSize());

      // did we previously create a tuple stage that was shuffling the data so we can probe a partitioned hash set
      string sourceTupleSetName = curSource->getOutputName();
      if (!joinSource.empty()) {
        sourceTupleSetName = joinSource;
        joinSource = "";
      }

      // we first create a pipeline breaker to partition RHS by setting
      // the isRepartitioning=true and isRepartitionJoin=true
      Handle<TupleSetJobStage> joinPrepStage = createTupleSetJobStage(jobStageId,
                                                                      sourceTupleSetName,
                                                                      targetTupleSetName,
                                                                      computationSpecifier,
                                                                      buildTheseTupleSets,
                                                                      "IntermediateData",
                                                                      curInputSetIdentifier,
                                                                      nullptr,
                                                                      sink,
                                                                      false,
                                                                      true,
                                                                      isProbing,
                                                                      allocatorPolicy,
                                                                      true);

      // the join source becomes the original source
      string newJoinSource = curSource->getOutputName();

      // add the stage to the list of stages to be executed
      physicalPlanToOutput.emplace_back(joinPrepStage);

      // add the output of this TupleSetJobStage to the list of intermediate sets
      interGlobalSets.push_back(sink);

      // grab the last computation to be applied (that thing is our hashing thingy)
      string lastOne = buildTheseTupleSets.back();

      // this last tuple set job stage we added is a pipeline breaker, clear the list of tuple sets to build (computations to be applied)
      buildTheseTupleSets.clear();

      outputForJoinSets.emplace_back(outputName);

      buildTheseTupleSets.push_back(lastOne);
      buildTheseTupleSets.push_back(curNode->getOutputName());

      // Now I am the source!
      // we then create a pipeline stage to probe the partitioned hash table
      return analyze(physicalPlanToOutput,
                     interGlobalSets,
                     buildTheseTupleSets,
                     nextNode,
                     curNode,
                     curNode,
                     curComp,
                     sink,
                     jobStageId,
                     true,
                     newJoinSource,
                     allocatorPolicy);

    } else {

      // we have only one consumer node so we know what the next node in line is
      AtomicComputationPtr nextNode = consumers[0];

      // we probe the broadcasted hash table
      // if my other input has been processed, I am not a pipeline breaker, but we
      // should set the correct hash set names for probing
      buildTheseTupleSets.push_back(curNode->getOutputName());
      outputForJoinSets.emplace_back(outputName);
      return analyze(physicalPlanToOutput,
                     interGlobalSets,
                     buildTheseTupleSets,
                     nextNode,
                     curNode,
                     curSource,
                     sourceComputation,
                     curInputSetIdentifier,
                     jobStageId,
                     true,
                     joinSource,
                     allocatorPolicy);
    }
  }
}
bool TCAPAnalyzer::handleSingleConsumerAggregation(vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
                                                   vector<Handle<SetIdentifier>> &interGlobalSets,
                                                   const vector<string> &buildTheseTupleSets,
                                                   AtomicComputationPtr &curSource,
                                                   const Handle<SetIdentifier> &curInputSetIdentifier,
                                                   AtomicComputationPtr &curNode,
                                                   int &jobStageId,
                                                   bool isProbing,
                                                   const AllocatorPolicy &allocationPolicy,
                                                   string &joinSource,
                                                   const string &outputName,
                                                   const string &computationSpecifier,
                                                   Handle<Computation> &curComp) {

  // to create the producing job stage for aggregation and set the page size
  Handle<SetIdentifier> aggregator = makeObject<SetIdentifier>(jobId, outputName + "_aggregationData");
  aggregator->setPageSize(conf->getShufflePageSize());

  // are we using a combiner (the thing that combines the records by key before sending them to the right node)
  Handle<SetIdentifier> combiner = nullptr;
  if (curComp->isUsingCombiner()) {
    combiner = makeObject<SetIdentifier>(jobId, outputName + "_combinerData");
  }

  // did we previously create a tuple stage that was probing a partitioned hash table
  string sourceTupleSetName = curSource->getOutputName();
  if (!joinSource.empty()) {
    sourceTupleSetName = joinSource;
    joinSource = "";
  }

  // cast the computation to AbstractAggregateComp to create the consuming job stage for aggregation
  Handle<AbstractAggregateComp> agg = unsafeCast<AbstractAggregateComp, Computation>(curComp);

  // create the tuple set job stage to run the pipeline with a shuffle sink
  // here is what we are doing :
  // the input to the stage is either the output of the join or the source node we started)
  // the repartitioning flag is set to true, so that we run a pipeline with a shuffle sink
  // the pipeline until the combiner will apply all the computations to the source set
  // and put them on a page partitioned into multiple maps the combiner will then read the maps that belong to
  // the partitions of a certain node and combine them by key. The output pages of the combiner will then be sent
  // to the appropriate nodes depending on the parameters isCollectAsMap and getNumNodesToCollect
  Handle<TupleSetJobStage> jobStage = createTupleSetJobStage(jobStageId,
                                                             sourceTupleSetName,
                                                             curNode->getInputName(),
                                                             computationSpecifier,
                                                             buildTheseTupleSets,
                                                             "IntermediateData",
                                                             curInputSetIdentifier,
                                                             combiner,
                                                             aggregator,
                                                             false,
                                                             true,
                                                             isProbing,
                                                             allocationPolicy,
                                                             false,
                                                             agg->isCollectAsMap(),
                                                             agg->getNumNodesToCollect());


  // to push back the job stage
  physicalPlanToOutput.emplace_back(jobStage);

  // to create the consuming job stage for aggregation
  Handle<AggregationJobStage> aggStage;
  Handle<SetIdentifier> sink;

  // does the current computation already need materialization
  if (curComp->needsMaterializeOutput()) {

    // grab the sink set
    string dbName = curComp->getDatabaseName();
    string setName = curComp->getSetName();
    sink = makeObject<SetIdentifier>(dbName, setName);

    // create an aggregation job stage with materialization
    aggStage = createAggregationJobStage(jobStageId, agg, aggregator, sink, true);

  } else {

    sink = makeObject<SetIdentifier>(jobId, outputName + "_aggregationResult", PartitionedHashSetType, true);
    // we are not materializing therefore create an aggregation job stage without materialization
    aggStage = createAggregationJobStage(jobStageId, agg, aggregator, sink, false);
  }

  // to push back the aggregation stage;
  physicalPlanToOutput.emplace_back(aggStage);

  // to push back the aggregator set
  interGlobalSets.push_back(aggregator);

  // update the source sets (if the source node is not being used anymore we just remove it)
  updateSourceSets(curInputSetIdentifier, sink, curNode);

  return true;
}

// to remove source
void TCAPAnalyzer::removeSource(std::string setName) {

  // do we have the source in the current sources
  if (curSourceNodes.find(setName) != curSourceNodes.end()) {

    // does the source have a computations that consume them
    if (!curSourceNodes[setName].empty()) {

      // we are always using the first computation that is in the vector
      // and this computation is now removed
      curSourceNodes[setName].erase(curSourceNodes[setName].begin());

      // in the case we still have computations that use this source set we set their number of
      // processed consumers to 0
      curProcessedConsumers[setName] = 0;
    }

    // if there are still some computations left we are finished no need to remove anything
    if (!curSourceNodes[setName].empty()) {
      return;
    }

    // there are not computations left that use this source set remove it from the list of ative source sets
    curSourceNodes.erase(setName);
  }

  // if the source is in the list of all source set names remove it!
  auto it = std::find(curSourceSetNames.begin(), curSourceSetNames.end(), setName);
  if (it != curSourceSetNames.end()) {
    curSourceSetNames.erase(it);
  }

  // if it is in the list of penalized source sets remove it!
  auto jt = std::find(penalizedSourceSets.begin(), penalizedSourceSets.end(), setName);
  if (jt != penalizedSourceSets.end()) {
    penalizedSourceSets.erase(jt);
  }

  // remove the set identifier for this source if it exists
  if (curSourceSets.find(setName) != curSourceSets.end()) {
    curSourceSets.erase(setName);
  }

  // remove the entry for the processed nodes if it exists!
  if (curProcessedConsumers.find(setName) != curProcessedConsumers.end()) {
    curProcessedConsumers.erase(setName);
  }

  PDB_COUT << "removed set " << setName << "\n";
}

void TCAPAnalyzer::updateSourceSets(Handle<SetIdentifier> oldSet,
                                    Handle<SetIdentifier> newSet,
                                    AtomicComputationPtr newAtomicComp) {

  // do we have a new set that we need to add to the sources
  if ((newSet != nullptr) && (newAtomicComp != nullptr)) {

    // get new set name
    std::string newSetName = newSet->toSourceSetName();

    // add new set
    curSourceSetNames.push_back(newSetName);
    curSourceSets[newSetName] = newSet;
    curSourceNodes[newSetName].push_back(newAtomicComp);

    // if we don't have this new source in the list of processed consumers
    if (curProcessedConsumers.find(newSetName) != curProcessedConsumers.end()) {
      // if we do not set it's number to zero
      curProcessedConsumers[newSetName] = 0;
    }
  }

  // get old set name
  std::string oldSetName = oldSet->toSourceSetName();

  // if we have the old node remove the old stuff
  if (curSourceNodes.find(oldSetName) == curSourceNodes.end()) {
    return;
  }

  // grab the TCAP computation
  AtomicComputationPtr oldNode = curSourceNodes[oldSetName][0];

  // grab the source specifier
  std::string sourceSpecifier = oldNode->getComputationName();

  // grab the consumers for this source
  std::vector<AtomicComputationPtr>
      consumers = this->computationGraph.getConsumingAtomicComputations(oldNode->getOutputName());

  // do we still have any consumers left for this source
  unsigned int numProcessedConsumers = curProcessedConsumers[oldSetName];
  if (consumers.size() == numProcessedConsumers) {
    // if we don't remove the source
    removeSource(oldSetName);
  }

  // print out the new state
  printSourceSetNames();
}

// to get source set based on name
Handle<SetIdentifier> TCAPAnalyzer::getSourceSetIdentifier(std::string name) {
  return curSourceSets.find(name) != curSourceSets.end() ? curSourceSets[name] : nullptr;
}

// to get source computation based on name
AtomicComputationPtr TCAPAnalyzer::getSourceComputation(std::string name) {
  return curSourceNodes.count(name) == 0 ? nullptr : curSourceNodes[name][0];
}

// to return the index of the best source
std::string TCAPAnalyzer::getBestSource(const StatisticsPtr &stats) {

  // if we don't have any statistics we just grab the first source we can!
  if (stats == nullptr) {
    return curSourceSetNames[0];
  }

  // we go now through all the sources and try to find the one with the minimal cost..
  int bestIndexToReturn = 0;
  double minCost = DBL_MAX;
  for (int i = 0; i < curSourceSetNames.size(); i++) {

    // get the cost of the current source based on the stats
    double curCost = getCostOfSource(curSourceSetNames[i], stats);

    // check if the source is in the list of penalized sets
    if (std::find(penalizedSourceSets.begin(), penalizedSourceSets.end(), curSourceSetNames[i])
        != penalizedSourceSets.end()) {
      // if is we give it a much higher cost..
      curCost = curCost * 1000;
      PDB_COUT << "Meet a penalized source set: " << curSourceSetNames[i]
               << ", we increase its cost by 1000 times to be " << curCost << "\n";
    }

    // is the cost of this set less then the cost we currently have
    if (curCost < minCost) {
      // if it is this is our new cost
      minCost = curCost;
      bestIndexToReturn = i;
    }

    // below is optimization for nearest neighbor search
    // TODO Jia: why do we have this statement?
    if (JOIN_HASH_TABLE_SIZE_RATIO > 1.5 && curCost == minCost) {
      minCost = curCost;
      bestIndexToReturn = i;
    }
  }

  // print out the source we selected and the cost
  PDB_COUT << "The Best Source (cost= " << minCost << ") is " << bestIndexToReturn << ": "
           << curSourceSetNames[bestIndexToReturn] << "\n";

  // set the current source and the current cost
  this->costOfCurSource = minCost;
  this->curSourceSetName = curSourceSetNames[bestIndexToReturn];

  // return the name of the current source
  return curSourceSetNames[bestIndexToReturn];
}

// to return the cost of the i-th source
double TCAPAnalyzer::getCostOfSource(const std::string &source, StatisticsPtr stats) {

  // get the set identifier for the source
  Handle<SetIdentifier> curSet = this->getSourceSetIdentifier(source);

  // if the set identifier does not exist log that
  if (curSet == nullptr) {
    PDB_COUT << "WARNING: there is no source set for key=" << source << "\n";
    return 0;
  }

  // calculate the cost based on the formula cost = number_of_bytes / 1000000
  double cost = stats->getNumBytes(curSet->getDatabase(), curSet->getSetName());
  return double((size_t) cost / 1000000);
}

// to return the index of next consumer to process for a certain source
unsigned int TCAPAnalyzer::getNextConsumerIndex(std::string name) {
  return curProcessedConsumers[name];
}

// to increment the index of next consumer to process for a certain source
void TCAPAnalyzer::incrementConsumerIndex(std::string name) {
  curProcessedConsumers[name] = curProcessedConsumers[name] + 1;
}

Handle<TupleSetJobStage> TCAPAnalyzer::createTupleSetJobStage(int &jobStageId,
                                                              std::string sourceTupleSetName,
                                                              std::string targetTupleSetName,
                                                              std::string targetComputationName,
                                                              std::vector<std::string> buildTheseTupleSets,
                                                              std::string outputTypeName,
                                                              Handle<SetIdentifier> sourceContext,
                                                              Handle<SetIdentifier> combinerContext,
                                                              Handle<SetIdentifier> sinkContext,
                                                              bool isBroadcasting,
                                                              bool isRepartitioning,
                                                              bool isProbing,
                                                              AllocatorPolicy myPolicy,
                                                              bool isRepartitionJoin,
                                                              bool isCollectAsMap,
                                                              int numNodesToCollect) {

  // create an instance of the tuple set job stage
  Handle<TupleSetJobStage> jobStage = makeObject<TupleSetJobStage>(jobStageId);

  // increase the job stage id since we are creating a new stage
  jobStageId++;

  // set the parameters
  jobStage->setComputePlan(this->computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
  jobStage->setTupleSetsToBuildPipeline(buildTheseTupleSets);
  jobStage->setSourceContext(sourceContext);
  jobStage->setSinkContext(sinkContext);
  jobStage->setOutputTypeName(outputTypeName);
  jobStage->setAllocatorPolicy(myPolicy);
  jobStage->setRepartitionJoin(isRepartitionJoin);
  jobStage->setBroadcasting(isBroadcasting);
  jobStage->setRepartition(isRepartitioning);
  jobStage->setJobId(this->jobId);
  jobStage->setCollectAsMap(isCollectAsMap);
  jobStage->setNumNodesToCollect(numNodesToCollect);


  // do we have all the parameters set to do probing
  if ((hashSetsToProbe != nullptr) && (!outputForJoinSets.empty()) && isProbing) {

    // set probing to true
    jobStage->setProbing(true);

    // copy the names of the hash sets to probe
    Handle<Map<String, String>> hashSetToProbeForMe = makeObject<Map<String, String>>();
    for (const auto &outputForJoinSet : outputForJoinSets) {
      (*hashSetToProbeForMe)[outputForJoinSet] = (*hashSetsToProbe)[outputForJoinSet];
    }

    // set them
    jobStage->setHashSetsToProbe(hashSetToProbeForMe);

    // clear the list that stored them
    outputForJoinSets.clear();
  }

  // are we using a combiner
  if (combinerContext != nullptr) {

    // set the parameters for the combiner
    jobStage->setCombinerContext(combinerContext);
    jobStage->setCombining(true);
  }

  // aggregation output should not be kept across
  // stages; if an aggregation has more than one
  // consumers, we need materialize aggregation
  // results.
  if (sourceContext->isAggregationResult()) {
    jobStage->setInputAggHashOut(true);
  }


  PDB_COUT << "TCAPAnalyzer generates tupleSetJobStage:" << "\n";
  return jobStage;
}

Handle<AggregationJobStage> TCAPAnalyzer::createAggregationJobStage(int &jobStageId,
                                                                    Handle<AbstractAggregateComp> aggComp,
                                                                    Handle<SetIdentifier> sourceContext,
                                                                    Handle<SetIdentifier> sinkContext,
                                                                    bool materializeResultOrNot) {
  // create an instance of the AggregationJobStage
  Handle<AggregationJobStage> aggStage = makeObject<AggregationJobStage>(jobStageId,
                                                                         materializeResultOrNot,
                                                                         aggComp);
  // increase the job stage id since we are creating a new stage
  jobStageId++;

  // set the parameters
  aggStage->setSourceContext(sourceContext);
  aggStage->setSinkContext(sinkContext);
  aggStage->setOutputTypeName(aggComp->getOutputType());
  aggStage->setJobId(this->jobId);

  PDB_COUT << "TCAPAnalyzer generates AggregationJobStage:" << "\n";

  return aggStage;
}

Handle<BroadcastJoinBuildHTJobStage> TCAPAnalyzer::createBroadcastJoinBuildHTJobStage(int &jobStageId,
                                                                                      std::string sourceTupleSetName,
                                                                                      std::string targetTupleSetName,
                                                                                      std::string targetComputationName,
                                                                                      Handle<SetIdentifier> sourceContext,
                                                                                      std::string hashSetName) {
  // create an instance of the BroadcastJoinBuildHTJobStage
  Handle<BroadcastJoinBuildHTJobStage> broadcastJoinStage = makeObject<BroadcastJoinBuildHTJobStage>(this->jobId,
                                                                                                     jobStageId,
                                                                                                     hashSetName);
  // increase the job stage id since we are creating a new stage
  jobStageId++;

  // set the parameters
  broadcastJoinStage->setComputePlan(this->computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
  broadcastJoinStage->setSourceContext(sourceContext);

  return broadcastJoinStage;
}

Handle<HashPartitionedJoinBuildHTJobStage> TCAPAnalyzer::createHashPartitionedJoinBuildHTJobStage(int &jobStageId,
                                                                                                  std::string sourceTupleSetName,
                                                                                                  std::string targetTupleSetName,
                                                                                                  std::string targetComputationName,
                                                                                                  Handle<SetIdentifier> sourceContext,
                                                                                                  std::string hashSetName) {
  // create an instance of the HashPartitionedJoinBuildHTJobStage
  Handle<HashPartitionedJoinBuildHTJobStage> hashPartitionedJobStage = makeObject<HashPartitionedJoinBuildHTJobStage>(this->jobId,
                                                                                                                      jobStageId,
                                                                                                                      hashSetName);
  // increase the job stage id since we are creating a new stage
  jobStageId++;

  // set the parameters
  hashPartitionedJobStage->setComputePlan(this->computePlan, sourceTupleSetName, targetTupleSetName, targetComputationName);
  hashPartitionedJobStage->setSourceContext(sourceContext);

  return hashPartitionedJobStage;
}

}

#endif
