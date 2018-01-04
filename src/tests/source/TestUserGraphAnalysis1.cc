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
#include <StringIntPair.h>
#include <StringSelectionOfStringIntPair.h>
#include <IntSimpleJoin.h>
#include <IntAggregation.h>

#include "SimpleAggregation.h"
#include "SimpleSelection.h"
#include "SimpleSelection.h"
#include "WriteUserSet.h"


using namespace pdb;

int main(int argc, char* argv[]) {

    // create all of the computation objects
    const UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};


    Handle<Computation> myScanSet1 = makeObject<ScanUserSet<int>>("test78_db", "test78_set1");
    Handle<Computation> myScanSet2 = makeObject<ScanUserSet<StringIntPair>>("test78_db", "test78_set2");
    Handle<Computation> mySelection = makeObject<StringSelectionOfStringIntPair>();
    mySelection->setInput(myScanSet2);
    Handle<Computation> myJoin = makeObject<IntSimpleJoin>();
    myJoin->setInput(0, myScanSet1);
    myJoin->setInput(1, myScanSet2);
    myJoin->setInput(2, mySelection);
    Handle<Computation> myAggregation = makeObject<IntAggregation>();
    myAggregation->setInput(myJoin);
    Handle<Computation> myWriter = makeObject<WriteUserSet<SumResult>>("test78_db", "output_set1");
    myWriter->setInput(myAggregation);

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
    PDBLoggerPtr logger = make_shared<PDBLogger>("testAggregationAnalysis.log");
    ConfigurationPtr conf = make_shared<Configuration>();
    std::string jobId = std::string("TestAggregationJob");
    TCAPAnalyzer tcapAnalyzer(jobId, computationsToSend, tcapString, logger, conf);

    std::vector<Handle<AbstractJobStage>> queryPlan;
    std::vector<Handle<SetIdentifier>> interGlobalSets;

    int jobStageId;

    std::vector<Handle<AbstractJobStage>> jobStages;
    std::vector<Handle<SetIdentifier>> intermediateSets;

    StatisticsPtr stats = nullptr;

    while (tcapAnalyzer.hasSources()) {
        bool success = false;
        while (tcapAnalyzer.hasSources() && !success) {

            success = tcapAnalyzer.getNextStagesOptimized(jobStages,
                                                          intermediateSets,
                                                          stats,
                                                          jobStageId);
        }


        jobStages.clear();
        intermediateSets.clear();
    }
}


#endif
