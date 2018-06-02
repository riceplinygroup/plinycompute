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
#include "TCAPAnalyzer.h"
#include <ctime>
#include <chrono>
#include <SumResult.h>
#include <IntAggregation.h>
#include <IntSimpleJoin.h>
#include <StringSelectionOfStringIntPair.h>

using namespace pdb;
int main(int argc, char* argv[]) {
    const UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};

    // create all of the computation objects
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

    PDBLoggerPtr logger = make_shared<PDBLogger>("testSelectionAnalysis.log");
    ConfigurationPtr conf = make_shared<Configuration>();
    std::string jobId = "TestSelectionJob";

    TCAPAnalyzer tcapAnalyzer(jobId, logger, conf, tcapString, computationsToSend);
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

        if(success) {
            std::cout << "PRINT PHYSICAL PLAN..." << std::endl;
            for (int i = 0; i < queryPlan.size(); i++) {
                std::cout << "to print the " << i << "-th plan" << std::endl;
                queryPlan[i]->print();
            }
        }
    }

    int code = system("scripts/cleanupSoFiles.sh force");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }

    getAllocator().cleanInactiveBlocks(36 * 1024 * 1024);
}

#endif
