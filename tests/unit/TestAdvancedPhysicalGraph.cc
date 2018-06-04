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
#include <WriteBuiltinEmployeeSet.h>
#include <AllSelectionWithCreation.h>
#include <ScanBuiltinEmployeeSet.h>
#include <AdvancedPhysicalOptimizer/AdvancedPhysicalNodeFactory.h>
#include <SimpleGroupBy.h>
#include <ScanSupervisorSet.h>

using namespace pdb;
int main(int argc, char *argv[]) {

  const UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

  // create all of the computation objects
  Handle<Computation> myScanSet = makeObject<ScanSupervisorSet>("test87_db", "test87_set");
  Handle<Computation> myAgg = makeObject<SimpleGroupBy>("test87_db", "output_set");
  myAgg->setAllocatorPolicy(noReuseAllocator);
  myAgg->setInput(myScanSet);

  std::vector<Handle<Computation>> queryGraph;
  queryGraph.push_back(myAgg);
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
    analyzerNodeFactory = make_shared<AdvancedPhysicalNodeFactory>(jobId, computePlan, conf);
  }
  catch (pdb::NotEnoughSpace &n) {

    std::cout << "Test failed" << std::endl;
    return -1;
  }

  // generate the analysis graph (it is a list of source nodes for that graph)
  auto graph = analyzerNodeFactory->generateAnalyzerGraph(sourcesComputations);

  getAllocator().cleanInactiveBlocks(36 * 1024 * 1024);

  // initialize the physicalAnalyzer - used to generate the pipelines and pipeline stages we need to execute
  PhysicalOptimizer physicalOptimizer(graph, logger);

  std::vector<Handle<AbstractJobStage>> queryPlan;
  std::vector<Handle<SetIdentifier>> interGlobalSets;
  std::cout << "PARSE TCAP STRING..." << std::endl;

  int jobStageId = 0;
  StatisticsPtr statsForOptimization = nullptr;
  while (physicalOptimizer.hasSources()) {

    // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
    bool success = physicalOptimizer.getNextStagesOptimized(queryPlan,
                                                            interGlobalSets,
                                                            statsForOptimization,
                                                            jobStageId);

    if (success) {
      std::cout << "PRINT PHYSICAL PLAN..." << std::endl;
      for (int i = 0; i < queryPlan.size(); i++) {
        std::cout << "to print the " << i << "-th plan" << std::endl;
        queryPlan[i]->print();
      }
    }
  }

  int code = system("scripts/cleanupSoFiles.sh");

  if (code < 0) {
      std::cout << "Can't cleanup so files" << std::endl;
  }
}

#endif
