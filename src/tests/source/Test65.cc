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

//by Jia, Mar 2017

#include "Handle.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "Supervisor.h"
#include "Employee.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SillySelection.h"
#include "SelectionComp.h"
#include "AggregateComp.h"
#include "ScanSupervisorSet.h"
#include "SillyAggregation.h"
#include "SillySelection.h"
#include "ZA_DepartmentTotal.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "InputTupleSetSpecifier.h"
#include "QueryGraphAnalyzer.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


// to run the aggregate, the system first passes each through the hash operation...
// then the system 
using namespace pdb;

int main (int argc, char * argv[]) {

	// create all of the computation objects
	Handle <Computation> myScanSet = makeObject <ScanSupervisorSet> ("chris_db", "chris_set");
	Handle <Computation> myFilter = makeObject <SillySelection> ();
        myFilter->setInput(myScanSet);
	Handle <Computation> myAgg = makeObject <SillyAggregation> ("chris_db", "output_set1");
        myAgg->setInput(myFilter);
        std :: vector <Handle<Computation>> queryGraph;
        queryGraph.push_back(myAgg);
        QueryGraphAnalyzer queryAnalyzer(queryGraph);
        std :: string tcapString = queryAnalyzer.parseTCAPString();
        std :: cout << tcapString << std :: endl;
        system ("scripts/cleanupSoFiles.sh");




}


#endif
