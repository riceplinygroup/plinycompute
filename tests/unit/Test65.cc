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
#ifndef TEST_65_H
#define TEST_65_H

// to test user graph analysis

#include "Handle.h"
#include "Lambda.h"
#include "Supervisor.h"
#include "Employee.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SelectionComp.h"
#include "AggregateComp.h"
#include "ScanSupervisorSet.h"
#include "DepartmentTotal.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "InputTupleSetSpecifier.h"
#include "QueryGraphAnalyzer.h"
#include "TCAPAnalyzer.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

#include "SimpleAggregation.h"
#include "SimpleSelection.h"
#include "SimpleSelection.h"

using namespace pdb;

int main(int argc, char *argv[]) {

  // create all of the computation objects
  const UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};
  Handle<Computation> myScanSet = makeObject<ScanSupervisorSet>("chris_db", "chris_set");
  Handle<Computation> myFilter = makeObject<SimpleSelection>();
  myFilter->setInput(myScanSet);
  Handle<Computation> myAgg = makeObject<SimpleAggregation>("chris_db", "output_set1");
  myAgg->setInput(myFilter);
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
  PDBLoggerPtr logger = make_shared<PDBLogger>("testSelectionAnalysis.log");
  ConfigurationPtr conf = make_shared<Configuration>();
  std::string jobId = "TestSelectionJob";

  TCAPAnalyzer tcapAnalyzer(jobId, computationsToSend, tcapString, logger, conf);
  std::vector<Handle<AbstractJobStage>> queryPlan;
  std::vector<Handle<SetIdentifier>> interGlobalSets;
  std::cout << "PARSE TCAP STRING..." << std::endl;

  int jobStageId = 0;
  StatisticsPtr statsForOptimization = nullptr;
  while (tcapAnalyzer.hasSources()) {

    // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
    bool success = tcapAnalyzer.getNextStagesOptimized(queryPlan,
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