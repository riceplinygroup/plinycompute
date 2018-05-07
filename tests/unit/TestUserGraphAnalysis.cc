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

using namespace pdb;
int main(int argc, char *argv[]) {
  const UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

  // create all of the computation objects
  Handle<Computation> myScanSet1 = makeObject<ScanStringIntPairSet>("test93_db", "test93_set1");
  Handle<Computation> myScanSet2 = makeObject<ScanStringIntPairSet>("test93_db", "test93_set2");
  Handle<Computation> myScanSet3 = makeObject<ScanStringIntPairSet>("test93_db", "test93_set3");
  Handle<Computation> myJoin = makeObject<OptimizedMethodJoin>();
  myJoin->setInput(0, myScanSet1);
  myJoin->setInput(1, myScanSet2);
  Handle<Computation> myOtherJoin = makeObject<OptimizedMethodJoin>();
  myOtherJoin->setInput(0, myJoin);
  myOtherJoin->setInput(1, myScanSet3);
  Handle<Computation> mySelection = makeObject<StringSelectionOfStringIntPair>();
  mySelection->setInput(myOtherJoin);
  Handle<Computation> myWriter = makeObject<WriteStringSet>("test93_db", "output_set1");
  myWriter->setInput(mySelection);

  std::vector<Handle<Computation>> queryGraph;
  queryGraph.push_back(myWriter);
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
  ds.numBytes = 100000000000;

  // create the data statistics
  StatisticsPtr stats = make_shared<Statistics>();
  stats->addSet("test93_db", "test93_set1", ds);
  stats->addSet("test93_db", "test93_set2", ds);
  stats->addSet("test93_db", "test93_set3", ds);

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

  int code = system("scripts/cleanupSoFiles.sh");
  if (code < 0) {
    std::cout << "Can't cleanup so files" << std::endl;
  }

  getAllocator().cleanInactiveBlocks(36 * 1024 * 1024);

}

#endif
