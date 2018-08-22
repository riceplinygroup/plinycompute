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

#ifndef TEST_64_H
#define TEST_64_H


// to test user graph analysis

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "ScanEmployeeSet.h"
#include "WriteStringSet.h"
#include "EmployeeSelection.h"
#include "QueryGraphAnalyzer.h"
#include "AbstractJobStage.h"
#include "SetIdentifier.h"
#include "PhysicalOptimizer.h"
#include <ctime>
#include <chrono>
#include <SumResult.h>
#include <IntAggregation.h>
#include <IntSimpleJoin.h>
#include <StringSelectionOfStringIntPair.h>
#include <SimplePhysicalOptimizer/SimplePhysicalNodeFactory.h>
#include <ScanBuiltinEmployeeSet.h>
#include <AllSelectionWithCreation.h>
#include <WriteBuiltinEmployeeSet.h>
#include <ScanSupervisorSet.h>
#include <SimpleGroupBy.h>
#include <OptimizedMethodJoin.h>
#include <ScanStringIntPairSet.h>
#include <ScanIntSet.h>
#include <ScanStringSet.h>
#include <CartesianJoin.h>
#include <WriteStringIntPairSet.h>
#include <FinalSelection.h>
#include <SimpleSelection.h>
#include <SimpleAggregation.h>
#include <ScanLDADocumentSet.h>
#include <LDADocTopicProbSelection.h>
#include <LDAInitialTopicProbSelection.h>
#include <LDATopicWordAggregate.h>
#include <LDADocIDAggregate.h>
#include <LDADocWordTopicJoin.h>
#include <LDAInitialWordTopicProbSelection.h>
#include <LDADocWordTopicAssignmentIdentity.h>
#include <LDADocAssignmentMultiSelection.h>
#include <LDADocTopicAggregate.h>
#include <LDATopicAssignmentMultiSelection.h>
#include <LDATopicWordProbMultiSelection.h>
#include <LDAWordTopicAggregate.h>
#include <WriteTopicsPerWord.h>
#include <WriteIntDoubleVectorPairSet.h>

using namespace pdb;
int main(int argc, char *argv[]) {
  const UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

  pdb::Handle<pdb::Vector<double>> alpha = pdb::makeObject<pdb::Vector<double>>(0, 0);
  pdb::Handle<pdb::Vector<double>> beta = pdb::makeObject<pdb::Vector<double>>(0, 0);
  alpha->fill(1.0);
  beta->fill(1.0);


  /* Initialize the topic mixture probabilities for each document */
  Handle<Computation> myInitialScanSet = makeObject<ScanLDADocumentSet>("LDA_db", "LDA_input_set");
  Handle<Computation> myDocID = makeObject<LDADocIDAggregate>();
  myDocID->setInput(myInitialScanSet);
  Handle<Computation> myDocTopicProb = makeObject<LDAInitialTopicProbSelection>(*alpha);
  myDocTopicProb->setInput(myDocID);

  /* Initialize the (wordID, topic prob vector) */
  Handle<Computation> myMetaScanSet = makeObject<ScanIntSet>("LDA_db", "LDA_meta_data_set");
  Handle<Computation> myWordTopicProb = makeObject<LDAInitialWordTopicProbSelection>(0);
  myWordTopicProb->setInput(myMetaScanSet);

  Handle<Computation> input1 = myDocTopicProb;
  Handle<Computation> input2 = myWordTopicProb;

  /* [1] Set up the join that will assign all of the words in the corpus to topics */
  Handle<Computation> myDocWordTopicJoin = makeObject<LDADocWordTopicJoin>(0);
  myDocWordTopicJoin->setInput(0, myInitialScanSet);
  myDocWordTopicJoin->setInput(1, input1);
  myDocWordTopicJoin->setInput(2, input2);

  /* Do an identity selection */
  Handle<Computation> myIdentitySelection = makeObject<LDADocWordTopicAssignmentIdentity>();
  myIdentitySelection->setInput(myDocWordTopicJoin);

  /* [2] Set up the sequence of actions that re-compute the topic probabilities for each document */

  /* Get the set of topics assigned for each doc */
  Handle<Computation> myDocWordTopicCount = makeObject<LDADocAssignmentMultiSelection>();
  myDocWordTopicCount->setInput(myIdentitySelection);

  /* Aggregate the topics */
  Handle<Computation> myDocTopicCountAgg = makeObject<LDADocTopicAggregate>();
  myDocTopicCountAgg->setInput(myDocWordTopicCount);

  /* Get the new set of doc-topic probabilities */
  myDocTopicProb = makeObject<LDADocTopicProbSelection>(*alpha);
  myDocTopicProb->setInput(myDocTopicCountAgg);

  /* [3] Set up the sequence of actions that re-compute the word probs for each topic */

  /* Get the set of words assigned for each topic in each doc */
  Handle<Computation> myTopicWordCount = makeObject<LDATopicAssignmentMultiSelection>();
  myTopicWordCount->setInput(myIdentitySelection);

  /* Aggregate them */
  Handle<Computation> myTopicWordCountAgg = makeObject<LDATopicWordAggregate>();
  myTopicWordCountAgg->setInput(myTopicWordCount);

  /* Use those aggregations to get per-topic probabilities */
  Handle<Computation> myTopicWordProb = makeObject<LDATopicWordProbMultiSelection>(*beta, 0);
  myTopicWordProb->setInput(myTopicWordCountAgg);

  /* Get the per-word probabilities */
  myWordTopicProb = makeObject<LDAWordTopicAggregate>();
  myWordTopicProb->setInput(myTopicWordProb);

  /*
   * [4] Write the intermediate results doc-topic probability and word-topic probability to sets
   *     Use them in the next iteration
   */
  std::string myWriterForTopicsPerWordSetName = std::string("TopicsPerWord") + std::to_string((1) % 2);
  Handle<Computation> myWriterForTopicsPerWord = makeObject<WriteTopicsPerWord>("LDA_db", myWriterForTopicsPerWordSetName);
  myWriterForTopicsPerWord->setInput(myWordTopicProb);

  std::string myWriterForTopicsPerDocSetName = std::string("TopicsPerDoc") + std::to_string((1) % 2);
  Handle<Computation> myWriterForTopicsPerDoc = makeObject<WriteIntDoubleVectorPairSet>("LDA_db", myWriterForTopicsPerDocSetName);
  myWriterForTopicsPerDoc->setInput(myDocTopicProb);


  std::vector<Handle<Computation>> queryGraph;
  queryGraph.push_back(myWriterForTopicsPerWord);
  queryGraph.push_back(myWriterForTopicsPerDoc);
  QueryGraphAnalyzer queryAnalyzer(queryGraph);
  std::string tcapString = queryAnalyzer.parseTCAPString();
  std::cout << "TCAP OUTPUT:" << std::endl;
  std::cout << tcapString << std::endl;
  std::vector<Handle<Computation>> computations;
  std::cout << "PARSE COMPUTATIONS..." << std::endl;
  queryAnalyzer.parseComputations(computations);
  Handle<Vector<Handle<Computation>>> computationsToSend = makeObject<Vector<Handle<Computation>>>();

  for (const auto &computation : computations) {
    computationsToSend->push_back(computation);
  }

  AbstractPhysicalNodeFactoryPtr analyzerNodeFactory;

  std::vector<AtomicComputationPtr> sourcesComputations;
  PDBLoggerPtr logger = make_shared<PDBLogger>("testSelectionAnalysis.log");
  ConfigurationPtr conf = make_shared<Configuration>();
  std::string jobId = "TestSelectionJob";

  try {
    // parse the plan and initialize the values we need
    Handle<ComputePlan> computePlan = makeObject<ComputePlan>(String(tcapString), *computationsToSend);
    LogicalPlanPtr logicalPlan = computePlan->getPlan();
    AtomicComputationList computationGraph = logicalPlan->getComputations();
    sourcesComputations = computationGraph.getAllScanSets();

    // this is the tcap analyzer node factory we want to use create the graph for the physical analysis
    analyzerNodeFactory = make_shared<SimplePhysicalNodeFactory>(jobId, computePlan, conf);
  }
  catch (pdb::NotEnoughSpace &n) {

    std::cout << "Test failed" << std::endl;
    return -1;
  }

  // generate the analysis graph (it is a list of source nodes for that graph)
  auto graph = analyzerNodeFactory->generateAnalyzerGraph(sourcesComputations);

  // initialize the physicalAnalyzer - used to generate the pipelines and pipeline stages we need to execute
  PhysicalOptimizer physicalOptimizer(graph, logger);

  std::vector<Handle<AbstractJobStage>> queryPlan;
  std::vector<Handle<SetIdentifier>> interGlobalSets;
  std::cout << "PARSE TCAP STRING..." << std::endl;

  int jobStageId = 0;

  // temp variables
  DataStatistics ds;


  // create the data statistics
  StatisticsPtr stats = make_shared<Statistics>();
  ds.numBytes = 1000;
  stats->addSet("LDA_db", "LDA_input_set", ds);

  ds.numBytes = 10000;
  stats->addSet("LDA_db", "LDA_meta_data_set", ds);


  ds.numBytes = 1000000;
  stats->addSet("TestSelectionJob", "aggOutForClusterAggregationComp1_aggregationResult", ds);

  while (physicalOptimizer.hasSources()) {

    // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
    bool success = physicalOptimizer.getNextStagesOptimized(queryPlan,
                                                            interGlobalSets,
                                                            stats,
                                                            jobStageId);

    if (success) {
      std::cout << "PRINT PHYSICAL PLAN..." << std::endl;
      for (int i = 0; i < queryPlan.size(); i++) {
        std::cout << "to print the " << i << "-th plan" << std::endl;
        queryPlan[i]->print();
      }

      // remove them all
      queryPlan.clear();
    }
  }

  int code = system("scripts/cleanupSoFiles.sh force");
  if (code < 0) {
      std::cout << "Can't cleanup so files" << std::endl;
  }

  graph.clear();

  getAllocator().cleanInactiveBlocks(36 * 1024 * 1024);

}

#endif
