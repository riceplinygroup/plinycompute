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

#ifndef TEST_606_H
#define TEST_606_H

#include "PDBString.h"
#include "BaseQuery.h"
#include "Lambda.h"
#include "QueryClient.h"

#include "QueryOutput.h"
#include "StorageClient.h"
#include "LeoQuery.h"
#include "QueriesAndPlan.h"
#include "QuerySchedulerServer.h"

#include "Set.h"

#include "Supervisor.h"
#include "DataTypes.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

using namespace pdb;

int main (int argc, char * argv[]) {

    const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};

    string errMsg;
    PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> ("clientLog");
    StorageClient temp (8108, "localhost", myLogger);

    if (!temp.registerType ("libraries/libLeoQuery.so", errMsg)) {
        cout << "Not able to register type: " + errMsg;
    } else {
        cout << "Registered type.\n";
    }

    std :: cout << "Done " << std :: endl;

    String program = "A(a) = load \"myDB mySet\"\n"
        "@exec \"attAccess_2\"\n"
        "B(a,b) = hoist \"someAtt\" from A[a] retain all\n"
        "@exec \"methodCall_1\"\n"
        "C(a,b,c) = apply method \"someMethod\" to B[a] retain all\n"
        "@exec \"==_0\"\n"
        "D(a,b) = apply func \"someFunc\" to C[c,b] retain a\n"
        "E(a) = filter D by b retain a\n"
        "@exec \"methodCall_3\"\n"
        "F(a,b) = apply method \"someMethod\" to E[a] retain a\n"
        "store F[b] \"myDB myOutput\"";


    PDBLoggerPtr logger = make_shared<PDBLogger> ("client606.log");
    QuerySchedulerServer server (logger);

    // Set the query and plans to be scheduled by the scheduler server
    Handle<QueriesAndPlan> queriesAndPlan = makeObject<QueriesAndPlan>();
    queriesAndPlan->setPlan(program);
    Handle<LeoQuery> qry = makeObject<LeoQuery>();
    Handle<BaseQuery> queryToExecute = unsafeCast <BaseQuery>(qry);
    std :: cout << "Done " << std :: endl;

    queriesAndPlan->addQuery(queryToExecute);
    std :: cout << "Setting query " << std :: endl;
    server.setQueryAndPlan(queriesAndPlan);
    std :: cout << "Scheduling " << std :: endl;
    server.scheduleNew("localhost", 8108, myLogger, Direct);

}

#endif
