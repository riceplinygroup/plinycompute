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
#include <ScanIntSet.h>
#include <ScanStringSet.h>
#include <CartesianJoin.h>
#include <WriteStringIntPairSet.h>
#include <ScanStringIntPairSet.h>
#include <OptimizedMethodJoin.h>

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

  /**
   * This tests a simple selection
   */
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


  /**
   * This tests a simple join
   */
  void test3() {

    const pdb::UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

    // create all of the computation objects
    Handle<Computation> myScanSet1 = makeObject<ScanIntSet>("test77_db", "test77_set1");
    myScanSet1->setBatchSize(100);
    Handle<Computation> myScanSet2 = makeObject<ScanStringSet>("test77_db", "test77_set2");
    myScanSet2->setBatchSize(16);
    Handle<Computation> myJoin = makeObject<CartesianJoin>();
    myJoin->setInput(0, myScanSet1);
    myJoin->setInput(1, myScanSet2);
    Handle<Computation> myWriter = makeObject<WriteStringIntPairSet>("test77_db", "output_set1");
    myWriter->setInput(myJoin);

    // the query graph has only the aggregation
    std::vector<Handle<Computation>> queryGraph = { myWriter };

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
    std::vector<Handle<AbstractJobStage>> queryPlan;
    std::vector<Handle<SetIdentifier>> interGlobalSets;

    // temp variables
    DataStatistics ds;
    ds.numBytes = 100000000;

    // create the data statistics
    StatisticsPtr stats = make_shared<Statistics>();
    stats->addSet("test77_db", "test77_set1", ds);

    int step = 0;

    while (physicalOptimizer.hasSources()) {

      // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
      bool success = physicalOptimizer.getNextStagesOptimized(queryPlan,
                                                              interGlobalSets,
                                                              stats,
                                                              jobStageId);
      // go to the next step
      step += success;

      switch(step) {

        // do not do anything
        case 0 : break;

        case 1 : {

          // get the tuple set job stage
          Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

          // get the pipline computations
          std::vector<std::string> buildMe;
          tupleStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getStageId(), 0);
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test77_db");
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test77_set2");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "CartesianJoined__in0___in1__broadcastData");
          QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "IntermediateData");
          QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_1");
          QUNIT_IS_EQUAL(tupleStage->getTargetTupleSetSpecifier(), "hashOneFor_2_0_in1");
          QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "JoinComp_2");
          QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);

          QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_1");
          QUNIT_IS_EQUAL(buildMe[1], "hashOneFor_2_0_in1");

          // get the build hash table
          Handle<BroadcastJoinBuildHTJobStage> buildHashTable = unsafeCast<BroadcastJoinBuildHTJobStage, AbstractJobStage>(queryPlan[1]);

          QUNIT_IS_EQUAL(buildHashTable->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(buildHashTable->getStageId(), 0);
          QUNIT_IS_EQUAL(buildHashTable->getHashSetName(), "TestSelectionJob:CartesianJoined__in0___in1__broadcastData");

          // remove the stages we don't need them anymore
          queryPlan.clear();

          break;
        }

        // second set of operators
        case 2 : {

          // get the tuple set job stage
          Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

          // get the pipline computations
          std::vector<std::string> buildMe;
          tupleStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getStageId(), 2);
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test77_db");
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test77_set1");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "test77_db");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "output_set1");
          QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "pdb::StringIntPair");
          QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_0");
          QUNIT_IS_EQUAL(tupleStage->getTargetTupleSetSpecifier(), "nativ_1OutForJoinComp2");
          QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "WriteUserSet_3");
          QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);
          QUNIT_IS_EQUAL(tupleStage->isProbing(), true);


          QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_0");
          QUNIT_IS_EQUAL(buildMe[1], "hashOneFor_2_0_in0");
          QUNIT_IS_EQUAL(buildMe[2], "CartesianJoined__in0___in1_");
          QUNIT_IS_EQUAL(buildMe[3], "nativOutFor_native_lambda_0_JoinComp_2");
          QUNIT_IS_EQUAL(buildMe[4], "filtedOutFor_native_lambda_0_JoinComp_2");
          QUNIT_IS_EQUAL(buildMe[5], "nativ_1OutForJoinComp2");

          auto hashSetsToProbe = tupleStage->getHashSets();

          // there should be only one hash set we need to probe
          QUNIT_IS_EQUAL("CartesianJoined__in0___in1_", (*hashSetsToProbe->begin()).key);
          QUNIT_IS_EQUAL("TestSelectionJob:CartesianJoined__in0___in1__broadcastData", (*hashSetsToProbe->begin()).value);

          break;
        }

        default: {

          // this situation should never happen
          QUNIT_IS_TRUE(false);
        }
      }
    }
  }

  /**
   * This tests a join after join using broadcasting
   */
  void test4() {

    const pdb::UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

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

    // the query graph has only the aggregation
    std::vector<Handle<Computation>> queryGraph = { myWriter };

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
    std::vector<Handle<AbstractJobStage>> queryPlan;
    std::vector<Handle<SetIdentifier>> interGlobalSets;

    // temp variables
    DataStatistics ds;
    ds.numBytes = 100000000;

    // create the data statistics
    StatisticsPtr stats = make_shared<Statistics>();
    stats->addSet("test93_db", "test93_set1", ds);

    int step = 0;

    while (physicalOptimizer.hasSources()) {

      // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
      bool success = physicalOptimizer.getNextStagesOptimized(queryPlan,
                                                              interGlobalSets,
                                                              stats,
                                                              jobStageId);
      // go to the next step
      step += success;

      switch(step) {

        // do not do anything
        case 0 : break;

        case 1 : {

          // get the tuple set job stage
          Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

          // get the pipline computations
          std::vector<std::string> buildMe;
          tupleStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getStageId(), 0);
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test93_db");
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test93_set2");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "JoinedFor_equals2JoinComp2_broadcastData");
          QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "IntermediateData");
          QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_1");
          QUNIT_IS_EQUAL(tupleStage->getTargetTupleSetSpecifier(), "methodCall_1ExtractedFor_JoinComp2_hashed");
          QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "JoinComp_2");
          QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);

          QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_1");
          QUNIT_IS_EQUAL(buildMe[1], "methodCall_1ExtractedFor_JoinComp2");
          QUNIT_IS_EQUAL(buildMe[2], "methodCall_1ExtractedFor_JoinComp2_hashed");

          // get the build hash table
          Handle<BroadcastJoinBuildHTJobStage> buildHashTable = unsafeCast<BroadcastJoinBuildHTJobStage, AbstractJobStage>(queryPlan[1]);

          QUNIT_IS_EQUAL(buildHashTable->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(buildHashTable->getStageId(), 0);
          QUNIT_IS_EQUAL(buildHashTable->getHashSetName(), "TestSelectionJob:JoinedFor_equals2JoinComp2_broadcastData");

          // remove the stages we don't need them anymore
          queryPlan.clear();

          break;
        }

        case 2 : {

          // get the tuple set job stage
          Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

          // get the pipline computations
          std::vector<std::string> buildMe;
          tupleStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getStageId(), 2);
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test93_db");
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test93_set3");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "JoinedFor_equals2JoinComp4_broadcastData");
          QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "IntermediateData");
          QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_3");
          QUNIT_IS_EQUAL(tupleStage->getTargetTupleSetSpecifier(), "methodCall_1ExtractedFor_JoinComp4_hashed");
          QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "JoinComp_4");
          QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);

          QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_3");
          QUNIT_IS_EQUAL(buildMe[1], "methodCall_1ExtractedFor_JoinComp4");
          QUNIT_IS_EQUAL(buildMe[2], "methodCall_1ExtractedFor_JoinComp4_hashed");

          // get the build hash table
          Handle<BroadcastJoinBuildHTJobStage> buildHashTable = unsafeCast<BroadcastJoinBuildHTJobStage, AbstractJobStage>(queryPlan[1]);

          QUNIT_IS_EQUAL(buildHashTable->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(buildHashTable->getStageId(), 2);
          QUNIT_IS_EQUAL(buildHashTable->getHashSetName(), "TestSelectionJob:JoinedFor_equals2JoinComp4_broadcastData");

          // remove the stages we don't need them anymore
          queryPlan.clear();

          break;
        }

        // second set of operators
        case 3 : {

          // get the tuple set job stage
          Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

          // get the pipline computations
          std::vector<std::string> buildMe;
          tupleStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getStageId(), 4);
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test93_db");
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test93_set1");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "test93_db");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "output_set1");
          QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "pdb::String");
          QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_0");
          QUNIT_IS_EQUAL(tupleStage->getTargetTupleSetSpecifier(), "deref_2OutForSelectionComp5");
          QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "WriteUserSet_6");
          QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);
          QUNIT_IS_EQUAL(tupleStage->isProbing(), true);

          // check the atomic computations in the pipeline
          QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_0");
          QUNIT_IS_EQUAL(buildMe[1], "methodCall_0ExtractedFor_JoinComp2");
          QUNIT_IS_EQUAL(buildMe[2], "methodCall_0ExtractedFor_JoinComp2_hashed");
          QUNIT_IS_EQUAL(buildMe[3], "JoinedFor_equals2JoinComp2");
          QUNIT_IS_EQUAL(buildMe[4], "JoinedFor_equals2JoinComp2_WithLHSExtracted");
          QUNIT_IS_EQUAL(buildMe[5], "JoinedFor_equals2JoinComp2_WithBOTHExtracted");
          QUNIT_IS_EQUAL(buildMe[6], "JoinedFor_equals2JoinComp2_BOOL");
          QUNIT_IS_EQUAL(buildMe[7], "JoinedFor_equals2JoinComp2_FILTERED");
          QUNIT_IS_EQUAL(buildMe[8], "nativ_3OutForJoinComp2");
          QUNIT_IS_EQUAL(buildMe[9], "methodCall_0ExtractedFor_JoinComp4");
          QUNIT_IS_EQUAL(buildMe[10], "methodCall_0ExtractedFor_JoinComp4_hashed");
          QUNIT_IS_EQUAL(buildMe[11], "JoinedFor_equals2JoinComp4");
          QUNIT_IS_EQUAL(buildMe[12], "JoinedFor_equals2JoinComp4_WithLHSExtracted");
          QUNIT_IS_EQUAL(buildMe[13], "JoinedFor_equals2JoinComp4_WithBOTHExtracted");
          QUNIT_IS_EQUAL(buildMe[14], "JoinedFor_equals2JoinComp4_BOOL");
          QUNIT_IS_EQUAL(buildMe[15], "JoinedFor_equals2JoinComp4_FILTERED");
          QUNIT_IS_EQUAL(buildMe[16], "nativ_3OutForJoinComp4");
          QUNIT_IS_EQUAL(buildMe[17], "nativ_0OutForSelectionComp5");
          QUNIT_IS_EQUAL(buildMe[18], "filteredInputForSelectionComp5");
          QUNIT_IS_EQUAL(buildMe[19], "attAccess_1OutForSelectionComp5");
          QUNIT_IS_EQUAL(buildMe[20], "deref_2OutForSelectionComp5");

          auto hashSetsToProbe = tupleStage->getHashSets();

          // there should be two hash sets we need to probe
          auto it = hashSetsToProbe->begin();

          QUNIT_IS_EQUAL("JoinedFor_equals2JoinComp2", (*it).key);
          QUNIT_IS_EQUAL("TestSelectionJob:JoinedFor_equals2JoinComp2_broadcastData", (*it).value);

          // go to the next one
          it.operator++();

          QUNIT_IS_EQUAL("JoinedFor_equals2JoinComp4", (*it).key);
          QUNIT_IS_EQUAL("TestSelectionJob:JoinedFor_equals2JoinComp4_broadcastData", (*it).value);

          break;
        }

        default: {

          // this situation should never happen
          QUNIT_IS_TRUE(false);
        }
      }
    }
  }


  /**
   * This tests a join after join using shuffeling (both sets are shuffled)
   */
  void test5() {

    const pdb::UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

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

    // the query graph has only the aggregation
    std::vector<Handle<Computation>> queryGraph = { myWriter };

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
    std::vector<Handle<AbstractJobStage>> queryPlan;
    std::vector<Handle<SetIdentifier>> interGlobalSets;

    DataStatistics ds;
    ds.numBytes = 100000000000;

    // create the data statistics
    StatisticsPtr stats = make_shared<Statistics>();
    stats->addSet("test93_db", "test93_set1", ds);
    stats->addSet("test93_db", "test93_set2", ds);
    stats->addSet("test93_db", "test93_set3", ds);

    int step = 0;

    while (physicalOptimizer.hasSources()) {

      // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
      bool success = physicalOptimizer.getNextStagesOptimized(queryPlan,
                                                              interGlobalSets,
                                                              stats,
                                                              jobStageId);
      // go to the next step
      step += success;

      switch(step) {

        // do not do anything
        case 0 : break;

        case 1 : {

          // get the tuple set job stage
          Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

          // get the pipline computations
          std::vector<std::string> buildMe;
          tupleStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getStageId(), 0);
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test93_db");
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test93_set1");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "JoinedFor_equals2JoinComp2_repartitionData");
          QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "IntermediateData");
          QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_0");
          QUNIT_IS_EQUAL(tupleStage->getTargetTupleSetSpecifier(), "methodCall_0ExtractedFor_JoinComp2_hashed");
          QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "JoinComp_2");
          QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);

          QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_0");
          QUNIT_IS_EQUAL(buildMe[1], "methodCall_0ExtractedFor_JoinComp2");
          QUNIT_IS_EQUAL(buildMe[2], "methodCall_0ExtractedFor_JoinComp2_hashed");

          // get the build hash table
          Handle<HashPartitionedJoinBuildHTJobStage> buildHashTable = unsafeCast<HashPartitionedJoinBuildHTJobStage, AbstractJobStage>(queryPlan[1]);

          QUNIT_IS_EQUAL(buildHashTable->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(buildHashTable->getStageId(), 1);
          QUNIT_IS_EQUAL(buildHashTable->getSourceContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(buildHashTable->getSourceContext()->getSetName(), "JoinedFor_equals2JoinComp2_repartitionData");
          QUNIT_IS_EQUAL(buildHashTable->getHashSetName(), "TestSelectionJob:JoinedFor_equals2JoinComp2_repartitionData");

          // remove the stages we don't need them anymore
          queryPlan.clear();

          break;
        }

        case 2 : {

          // get the tuple set job stage
          Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

          // get the pipline computations
          std::vector<std::string> buildMe;
          tupleStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getStageId(), 3);
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test93_db");
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test93_set2");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "JoinedFor_equals2JoinComp2_repartitionData");
          QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "IntermediateData");
          QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_1");
          QUNIT_IS_EQUAL(tupleStage->getTargetTupleSetSpecifier(), "methodCall_1ExtractedFor_JoinComp2_hashed");
          QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "JoinComp_2");
          QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);

          QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_1");
          QUNIT_IS_EQUAL(buildMe[1], "methodCall_1ExtractedFor_JoinComp2");
          QUNIT_IS_EQUAL(buildMe[2], "methodCall_1ExtractedFor_JoinComp2_hashed");

          // get the build hash table
          Handle<TupleSetJobStage> probeStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[1]);
          probeStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(probeStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(probeStage->getStageId(), 4);
          QUNIT_IS_EQUAL(probeStage->getSourceContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(probeStage->getSourceContext()->getSetName(), "JoinedFor_equals2JoinComp2_repartitionData");
          QUNIT_IS_EQUAL(probeStage->getSinkContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(probeStage->getSinkContext()->getSetName(), "JoinedFor_equals2JoinComp4_repartitionData");
          QUNIT_IS_EQUAL(probeStage->getOutputTypeName(), "IntermediateData");
          QUNIT_IS_EQUAL(probeStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_1");
          QUNIT_IS_EQUAL(probeStage->getTargetTupleSetSpecifier(), "methodCall_0ExtractedFor_JoinComp4_hashed");
          QUNIT_IS_EQUAL(probeStage->getTargetComputationSpecifier(), "JoinComp_4");
          QUNIT_IS_EQUAL(probeStage->getAllocatorPolicy(), defaultAllocator);

          QUNIT_IS_EQUAL(buildMe[0], "methodCall_1ExtractedFor_JoinComp2_hashed");
          QUNIT_IS_EQUAL(buildMe[1], "JoinedFor_equals2JoinComp2");
          QUNIT_IS_EQUAL(buildMe[2], "JoinedFor_equals2JoinComp2_WithLHSExtracted");
          QUNIT_IS_EQUAL(buildMe[3], "JoinedFor_equals2JoinComp2_WithBOTHExtracted");
          QUNIT_IS_EQUAL(buildMe[4], "JoinedFor_equals2JoinComp2_BOOL");
          QUNIT_IS_EQUAL(buildMe[5], "JoinedFor_equals2JoinComp2_FILTERED");
          QUNIT_IS_EQUAL(buildMe[6], "nativ_3OutForJoinComp2");
          QUNIT_IS_EQUAL(buildMe[7], "methodCall_0ExtractedFor_JoinComp4");
          QUNIT_IS_EQUAL(buildMe[8], "methodCall_0ExtractedFor_JoinComp4_hashed");

          // grab the hash sets to probe
          auto hashSetsToProbe = probeStage->getHashSets();

          // there should be two hash sets we need to probe
          auto it = hashSetsToProbe->begin();

          QUNIT_IS_EQUAL("JoinedFor_equals2JoinComp2", (*it).key);
          QUNIT_IS_EQUAL("TestSelectionJob:JoinedFor_equals2JoinComp2_repartitionData", (*it).value);

          // get the build hash table
          Handle<HashPartitionedJoinBuildHTJobStage> buildHashTable = unsafeCast<HashPartitionedJoinBuildHTJobStage, AbstractJobStage>(queryPlan[2]);

          QUNIT_IS_EQUAL(buildHashTable->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(buildHashTable->getStageId(), 5);
          QUNIT_IS_EQUAL(buildHashTable->getSourceContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(buildHashTable->getSourceContext()->getSetName(), "JoinedFor_equals2JoinComp4_repartitionData");
          QUNIT_IS_EQUAL(buildHashTable->getHashSetName(), "TestSelectionJob:JoinedFor_equals2JoinComp4_repartitionData");

          // remove the stages we don't need them anymore
          queryPlan.clear();

          break;
        }

          // second set of operators
        case 3 : {

          // get the tuple set job stage
          Handle<TupleSetJobStage> tupleStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[0]);

          // get the pipline computations
          std::vector<std::string> buildMe;
          tupleStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(tupleStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getStageId(), 6);
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getDatabase(), "test93_db");
          QUNIT_IS_EQUAL(tupleStage->getSourceContext()->getSetName(), "test93_set3");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(tupleStage->getSinkContext()->getSetName(), "JoinedFor_equals2JoinComp4_repartitionData");
          QUNIT_IS_EQUAL(tupleStage->getOutputTypeName(), "IntermediateData");
          QUNIT_IS_EQUAL(tupleStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_3");
          QUNIT_IS_EQUAL(tupleStage->getTargetTupleSetSpecifier(), "methodCall_1ExtractedFor_JoinComp4_hashed");
          QUNIT_IS_EQUAL(tupleStage->getTargetComputationSpecifier(), "JoinComp_4");
          QUNIT_IS_EQUAL(tupleStage->getAllocatorPolicy(), defaultAllocator);

          QUNIT_IS_EQUAL(buildMe[0], "inputDataForScanUserSet_3");
          QUNIT_IS_EQUAL(buildMe[1], "methodCall_1ExtractedFor_JoinComp4");
          QUNIT_IS_EQUAL(buildMe[2], "methodCall_1ExtractedFor_JoinComp4_hashed");

          // get the build hash table
          Handle<TupleSetJobStage> probeStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(queryPlan[1]);
          probeStage->getTupleSetsToBuildPipeline(buildMe);

          QUNIT_IS_EQUAL(probeStage->getJobId(), "TestSelectionJob");
          QUNIT_IS_EQUAL(probeStage->getStageId(), 7);
          QUNIT_IS_EQUAL(probeStage->getSourceContext()->getDatabase(), "TestSelectionJob");
          QUNIT_IS_EQUAL(probeStage->getSourceContext()->getSetName(), "JoinedFor_equals2JoinComp4_repartitionData");
          QUNIT_IS_EQUAL(probeStage->getSinkContext()->getDatabase(), "test93_db");
          QUNIT_IS_EQUAL(probeStage->getSinkContext()->getSetName(), "output_set1");
          QUNIT_IS_EQUAL(probeStage->getOutputTypeName(), "pdb::String");
          QUNIT_IS_EQUAL(probeStage->getSourceTupleSetSpecifier(), "inputDataForScanUserSet_3");
          QUNIT_IS_EQUAL(probeStage->getTargetTupleSetSpecifier(), "deref_2OutForSelectionComp5");
          QUNIT_IS_EQUAL(probeStage->getTargetComputationSpecifier(), "WriteUserSet_6");
          QUNIT_IS_EQUAL(probeStage->getAllocatorPolicy(), defaultAllocator);

          QUNIT_IS_EQUAL(buildMe[0], "methodCall_1ExtractedFor_JoinComp4_hashed");
          QUNIT_IS_EQUAL(buildMe[1], "JoinedFor_equals2JoinComp4");
          QUNIT_IS_EQUAL(buildMe[2], "JoinedFor_equals2JoinComp4_WithLHSExtracted");
          QUNIT_IS_EQUAL(buildMe[3], "JoinedFor_equals2JoinComp4_WithBOTHExtracted");
          QUNIT_IS_EQUAL(buildMe[4], "JoinedFor_equals2JoinComp4_BOOL");
          QUNIT_IS_EQUAL(buildMe[5], "JoinedFor_equals2JoinComp4_FILTERED");
          QUNIT_IS_EQUAL(buildMe[6], "nativ_3OutForJoinComp4");
          QUNIT_IS_EQUAL(buildMe[7], "nativ_0OutForSelectionComp5");
          QUNIT_IS_EQUAL(buildMe[8], "filteredInputForSelectionComp5");
          QUNIT_IS_EQUAL(buildMe[8], "attAccess_1OutForSelectionComp5");
          QUNIT_IS_EQUAL(buildMe[8], "deref_2OutForSelectionComp5");

          auto hashSetsToProbe = tupleStage->getHashSets();

          // there should be two hash sets we need to probe
          auto it = hashSetsToProbe->begin();

          QUNIT_IS_EQUAL("JoinedFor_equals2JoinComp4", (*it).key);
          QUNIT_IS_EQUAL("TestSelectionJob:JoinedFor_equals2JoinComp4_repartitionData", (*it).value);

          // remove the stages we don't need them anymore
          queryPlan.clear();

          break;
        }
        default: {

          // this situation should never happen
          QUNIT_IS_TRUE(false);
        }
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
    test3();
    test4();
    test5();

    // return the errors
    return qunit.errors();
  }

};


int main() {
  return Tests(std::cerr).run();
}
