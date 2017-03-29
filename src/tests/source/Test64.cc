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


//by Jia, Mar 2017
//to test ChrisSelection using new pipeline stuff

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "ScanEmployeeSet.h"
#include "WriteStringSet.h"
#include "EmployeeSelection.h"
#include "SharedEmployee.h"
#include "DispatcherClient.h"
#include "Set.h"
#include "DataTypes.h"
#include "QueryGraphAnalyzer.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <vector>
#include <chrono>
#include <fcntl.h>

/* This test uses data and selection of builtInType to demonstrate a distributed query with distributed storage */

/*  Note that data size must be larger than #numTotalThreadsInCluster*#PageSize */
/*  Below test case is tested using 8GB data in 4-node cluster, each node run 12 threads */
using namespace pdb;
int main (int argc, char * argv[]) {

        Handle<Computation> myScanSet = makeObject<ScanEmployeeSet>("chris_db", "chris_set");
        Handle<Computation> myQuery = makeObject<EmployeeSelection>();
        myQuery->setInput(myScanSet);
        Handle<Computation> myWriteSet = makeObject<WriteStringSet>("chris_db", "output_set1");
        myWriteSet->setInput(myQuery);
        std :: vector <Handle<Computation>> queryGraph;
        queryGraph.push_back(myWriteSet);
        QueryGraphAnalyzer queryAnalyzer(queryGraph);
        std :: string tcapString = queryAnalyzer.parseTCAPString();
        std :: cout << tcapString << std :: endl;
        system ("scripts/cleanupSoFiles.sh");
}

#endif
