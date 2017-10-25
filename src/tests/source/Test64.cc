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


// by Jia, Mar 2017
// to test user graph analysis

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "ScanEmployeeSet.h"
#include "WriteStringSet.h"
#include "EmployeeSelection.h"
#include "SharedEmployee.h"
#include "Set.h"
#include "DataTypes.h"
#include "QueryGraphAnalyzer.h"
#include "PDBLogger.h"
#include "AbstractJobStage.h"
#include "SetIdentifier.h"
#include "TCAPAnalyzer.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <vector>
#include <chrono>
#include <fcntl.h>

using namespace pdb;
int main(int argc, char* argv[]) {
    const UseTemporaryAllocationBlock myBlock{36 * 1024 * 1024};
    Handle<Computation> myScanSet = makeObject<ScanEmployeeSet>("chris_db", "chris_set");
    Handle<Computation> myQuery = makeObject<EmployeeSelection>();
    myQuery->setInput(myScanSet);
    Handle<Computation> myWriteSet = makeObject<WriteStringSet>("chris_db", "output_set1");
    myWriteSet->setInput(myQuery);
    std::vector<Handle<Computation>> queryGraph;
    queryGraph.push_back(myWriteSet);
    QueryGraphAnalyzer queryAnalyzer(queryGraph);
    std::string tcapString = queryAnalyzer.parseTCAPString();
    std::cout << "TCAP OUTPUT:" << std::endl;
    std::cout << tcapString << std::endl;
    std::vector<Handle<Computation>> computations;
    std::cout << "PARSE COMPUTATIONS..." << std::endl;
    queryAnalyzer.parseComputations(computations);
    Handle<Vector<Handle<Computation>>> computationsToSend =
        makeObject<Vector<Handle<Computation>>>();
    for (int i = 0; i < computations.size(); i++) {
        computationsToSend->push_back(computations[i]);
    }
    PDBLoggerPtr logger = make_shared<PDBLogger>("testSelectionAnalysis.log");
    ConfigurationPtr conf = make_shared<Configuration>();
    TCAPAnalyzer tcapAnalyzer("TestSelectionJob", computationsToSend, tcapString, logger, conf);

    std::vector<Handle<AbstractJobStage>> queryPlan;
    std::vector<Handle<SetIdentifier>> interGlobalSets;
    std::cout << "PARSE TCAP STRING..." << std::endl;
    tcapAnalyzer.analyze(queryPlan, interGlobalSets);
    std::cout << "PRINT PHYSICAL PLAN..." << std::endl;
    for (int i = 0; i < queryPlan.size(); i++) {
        std::cout << "to print the " << i << "-th plan" << std::endl;
        queryPlan[i]->print();
    }
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }
}

#endif
