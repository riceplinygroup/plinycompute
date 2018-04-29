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

#include <qunit.h>
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
#include <AdvancedPhysicalOptimizer/AdvancedPhysicalNodeFactory.h>

class Tests {

  QUnit::UnitTest qunit;

  /**
   * This test tests a simple aggregation
   */
  void test1() {

    const pdb::UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

    Handle<Computation> scanSet = makeObject<ScanSupervisorSet>("test87_db", "test87_set");
    Handle<Computation> agg = makeObject<SimpleGroupBy>("test87_db", "output_set");
    agg->setAllocatorPolicy(noReuseAllocator);
    agg->setInput(scanSet);

    // the query graph has only the aggregation
    std::vector<Handle<Computation>> queryGraph = {agg};

    // create the graph analyzer
    QueryGraphAnalyzer queryAnalyzer(queryGraph);

    // parse the tcap string
    std::string tcapString = queryAnalyzer.parseTCAPString();

    // the computations we want to send
    std::vector<Handle<Computation>> computations;
    queryAnalyzer.parseComputations(computations);

    // copy the computations
    Handle<Vector<Handle<Computation>>> computationsToSend = makeObject<Vector<Handle<Computation>>>();
    for (const auto &computation : computations) {
      computationsToSend->push_back(computation);
    }

    // initialize the logger
    PDBLoggerPtr logger = make_shared<PDBLogger>("testSelectionAnalysis.log");

    // create a dummy configuration object4
    ConfigurationPtr conf = make_shared<Configuration>();

    // the job id
    std::string jobId = "TestSelectionJob";

    // parse the plan and initialize the values we need
    Handle<ComputePlan> computePlan = makeObject<ComputePlan>(String(tcapString), *computationsToSend);
    LogicalPlanPtr logicalPlan = computePlan->getPlan();
    AtomicComputationList computationGraph = logicalPlan->getComputations();
    std::vector<AtomicComputationPtr> sourcesComputations = computationGraph.getAllScanSets();

    // this is the tcap analyzer node factory we want to use create the graph for the physical analysis
    auto analyzerNodeFactory = make_shared<AdvancedPhysicalNodeFactory>(jobId, computePlan, conf);

    // generate the analysis graph (it is a list of source nodes for that graph)
    auto graph = analyzerNodeFactory->generateAnalyzerGraph(sourcesComputations);

    // initialize the physicalAnalyzer - used to generate the pipelines and pipeline stages we need to execute
    PhysicalOptimizer physicalOptimizer(graph, logger);

    // output variables
    int jobStageId = 0;
    StatisticsPtr statsForOptimization = nullptr;
    std::vector<Handle<AbstractJobStage>> queryPlan;
    std::vector<Handle<SetIdentifier>> interGlobalSets;

    while (physicalOptimizer.hasSources()) {

      // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
      bool success = physicalOptimizer.getNextStagesOptimized(queryPlan,
                                                              interGlobalSets,
                                                              statsForOptimization,
                                                              jobStageId);

      if (success) {

        // get the tuple set job stage
        Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

        // get the pipline computations
        std::vector<std::string> buildMe;
        tupleStage->getTupleSetsToBuildPipeline(buildMe);

        QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
        QUNIT_IS_EQUAL(tupleStage->getStageId(), 0);
        QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test87_db");
        QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test87_set");
        QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "TestSelectionJob");
        QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "aggOutForClusterAggregationComp1_aggregationData");
        QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "IntermediateData");
        QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_0");
        QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "ClusterAggregationComp_1");
        QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), noReuseAllocator);

        QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_0");
        QUNIT_IS_EQUAL(buildMe[1], "methodCall_0OutFor_ClusterAggregationComp1");
        QUNIT_IS_EQUAL(buildMe[2], "nativ_1OutForClusterAggregationComp1");

        // get the aggregation stage
        Handle<AggregationJobStage> aggStage = unsafeCast<AggregationJobStage, AbstractJobStage>(queryPlan[1]);

        QUNIT_IS_EQUAL(aggStage->getJobId(), "TestSelectionJob");
        QUNIT_IS_EQUAL(aggStage->getStageId(), 1);
        QUNIT_IS_EQUAL(aggStage->getSourceContext()->getDatabase(), "TestSelectionJob");
        QUNIT_IS_EQUAL(aggStage->getSourceContext()->getSetName(), "aggOutForClusterAggregationComp1_aggregationData");
        QUNIT_IS_EQUAL(aggStage->getSinkContext()->getDatabase(), "test87_db");
        QUNIT_IS_EQUAL(aggStage->getSinkContext()->getSetName(), "output_set");
        QUNIT_IS_EQUAL(aggStage->getOutputTypeName(), "pdb::DepartmentEmployeeAges");
      }
    }

  }

  void test2() {

    const pdb::UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

    // setup a simple selection
    Handle<Computation> myScanSet = makeObject<ScanBuiltinEmployeeSet>("chris_db", "chris_set");
    Handle<Computation> myQuery = makeObject<AllSelectionWithCreation>();
    myQuery->setInput(myScanSet);
    Handle<Computation> myWriteSet = makeObject<WriteBuiltinEmployeeSet>("chris_db", "output_set1");
    myWriteSet->setInput(myQuery);

    // the query graph has only the aggregation
    std::vector<Handle<Computation>> queryGraph = { myWriteSet };

    // create the graph analyzer
    QueryGraphAnalyzer queryAnalyzer(queryGraph);

    // parse the tcap string
    std::string tcapString = queryAnalyzer.parseTCAPString();

    // the computations we want to send
    std::vector<Handle<Computation>> computations;
    queryAnalyzer.parseComputations(computations);

    // copy the computations
    Handle<Vector<Handle<Computation>>> computationsToSend = makeObject<Vector<Handle<Computation>>>();
    for (const auto &computation : computations) {
      computationsToSend->push_back(computation);
    }

    // initialize the logger
    PDBLoggerPtr logger = make_shared<PDBLogger>("testSelectionAnalysis.log");

    // create a dummy configuration object4
    ConfigurationPtr conf = make_shared<Configuration>();

    // the job id
    std::string jobId = "TestSelectionJob";

    // parse the plan and initialize the values we need
    Handle<ComputePlan> computePlan = makeObject<ComputePlan>(String(tcapString), *computationsToSend);
    LogicalPlanPtr logicalPlan = computePlan->getPlan();
    AtomicComputationList computationGraph = logicalPlan->getComputations();
    std::vector<AtomicComputationPtr> sourcesComputations = computationGraph.getAllScanSets();

    // this is the tcap analyzer node factory we want to use create the graph for the physical analysis
    auto analyzerNodeFactory = make_shared<AdvancedPhysicalNodeFactory>(jobId, computePlan, conf);

    // generate the analysis graph (it is a list of source nodes for that graph)
    auto graph = analyzerNodeFactory->generateAnalyzerGraph(sourcesComputations);

    // initialize the physicalAnalyzer - used to generate the pipelines and pipeline stages we need to execute
    PhysicalOptimizer physicalOptimizer(graph, logger);

    // output variables
    int jobStageId = 0;
    StatisticsPtr statsForOptimization = nullptr;
    std::vector<Handle<AbstractJobStage>> queryPlan;
    std::vector<Handle<SetIdentifier>> interGlobalSets;

    while (physicalOptimizer.hasSources()) {

      // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
      bool success = physicalOptimizer.getNextStagesOptimized(queryPlan,
                                                              interGlobalSets,
                                                              statsForOptimization,
                                                              jobStageId);

      if (success) {

        // get the tuple set job stage
        Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

        // get the pipline computations
        std::vector<std::string> buildMe;
        tupleStage->getTupleSetsToBuildPipeline(buildMe);

        QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
        QUNIT_IS_EQUAL(tupleStage->getStageId(), 0);
        QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "chris_db");
        QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "chris_set");
        QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "chris_db");
        QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "output_set1");
        QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "pdb::Employee");
        QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_0");
        QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "WriteUserSet_2");
        QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);
        QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_0");
        QUNIT_IS_EQUAL(buildMe[1], "nativ_0OutForSelectionComp1");
        QUNIT_IS_EQUAL(buildMe[2], "filteredInputForSelectionComp1");
        QUNIT_IS_EQUAL(buildMe[3], "nativ_1OutForSelectionComp1");
      }
    }
  }


  void test3() {

    const pdb::UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

    // setup a simple selection
    Handle<Computation> myScanSet = makeObject<ScanBuiltinEmployeeSet>("chris_db", "chris_set");
    Handle<Computation> myQuery = makeObject<AllSelectionWithCreation>();
    myQuery->setInput(myScanSet);
    Handle<Computation> myWriteSet = makeObject<WriteBuiltinEmployeeSet>("chris_db", "output_set1");
    myWriteSet->setInput(myQuery);

    // the query graph has only the aggregation
    std::vector<Handle<Computation>> queryGraph = { myWriteSet };

    // create the graph analyzer
    QueryGraphAnalyzer queryAnalyzer(queryGraph);

    // parse the tcap string
    std::string tcapString = queryAnalyzer.parseTCAPString();

    // the computations we want to send
    std::vector<Handle<Computation>> computations;
    queryAnalyzer.parseComputations(computations);

    // copy the computations
    Handle<Vector<Handle<Computation>>> computationsToSend = makeObject<Vector<Handle<Computation>>>();
    for (const auto &computation : computations) {
      computationsToSend->push_back(computation);
    }

    // initialize the logger
    PDBLoggerPtr logger = make_shared<PDBLogger>("testSelectionAnalysis.log");

    // create a dummy configuration object4
    ConfigurationPtr conf = make_shared<Configuration>();

    // the job id
    std::string jobId = "TestSelectionJob";

    // parse the plan and initialize the values we need
    Handle<ComputePlan> computePlan = makeObject<ComputePlan>(String(tcapString), *computationsToSend);
    LogicalPlanPtr logicalPlan = computePlan->getPlan();
    AtomicComputationList computationGraph = logicalPlan->getComputations();
    std::vector<AtomicComputationPtr> sourcesComputations = computationGraph.getAllScanSets();

    // this is the tcap analyzer node factory we want to use create the graph for the physical analysis
    auto analyzerNodeFactory = make_shared<AdvancedPhysicalNodeFactory>(jobId, computePlan, conf);

    // generate the analysis graph (it is a list of source nodes for that graph)
    auto graph = analyzerNodeFactory->generateAnalyzerGraph(sourcesComputations);

    // initialize the physicalAnalyzer - used to generate the pipelines and pipeline stages we need to execute
    PhysicalOptimizer physicalOptimizer(graph, logger);

    // output variables
    int jobStageId = 0;
    StatisticsPtr statsForOptimization = nullptr;
    std::vector<Handle<AbstractJobStage>> queryPlan;
    std::vector<Handle<SetIdentifier>> interGlobalSets;

    while (physicalOptimizer.hasSources()) {

      // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
      bool success = physicalOptimizer.getNextStagesOptimized(queryPlan,
                                                              interGlobalSets,
                                                              statsForOptimization,
                                                              jobStageId);

      if (success) {

        // get the tuple set job stage
        Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

        // get the pipline computations
        std::vector<std::string> buildMe;
        tupleStage->getTupleSetsToBuildPipeline(buildMe);

        QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
        QUNIT_IS_EQUAL(tupleStage->getStageId(), 0);
        QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "chris_db");
        QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "chris_set");
        QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "chris_db");
        QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "output_set1");
        QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "pdb::Employee");
        QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_0");
        QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "WriteUserSet_2");
        QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);
        QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_0");
        QUNIT_IS_EQUAL(buildMe[1], "nativ_0OutForSelectionComp1");
        QUNIT_IS_EQUAL(buildMe[2], "filteredInputForSelectionComp1");
        QUNIT_IS_EQUAL(buildMe[3], "nativ_1OutForSelectionComp1");
      }
    }
  }

public:

  Tests(std::ostream & out, int verboseLevel = QUnit::verbose): qunit(out, verboseLevel) {}

  /**
   * Runs the tests
   * @return if the tests succeeded
   */
  int run() {

    // run tests
    test1();
    test2();

    // return the errors
    return qunit.errors();
  }

};


int main() {
  return Tests(std::cerr).run();
}
