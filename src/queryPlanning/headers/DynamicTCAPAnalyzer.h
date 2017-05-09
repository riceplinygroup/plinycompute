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
#ifndef DYNAMIC_TCAP_ANALYZER
#define DYNAMIC_TCAP_ANALYZER

#include "ComputePlan.h"
#include "Computation.h"
#include "LogicalPlan.h"
#include "AtomicComputationList.h"
#include "PDBLogger.h"
#include "TupleSetJobStage.h"
#include "AggregationJobStage.h"



//by Jia, May 2017

namespace pdb {

/*
 * This class is to dynamically analyze TCAP, and transform TCAP into executable
 * Job stages.
 */

class DynmaicTCAPAnalyzer {

public:

    //constructor
    DynamicTCAPAnalyzer (std :: string jobId, Handle<Vector<Handle<Computation>>> myComputations, std :: string myTCAPString, PDBLoggerPtr logger);

    //destructor
    ~DynmaicTCAPAnalyzer ();

    //to get next stage and in the sametime remove computations from the graph
    Handle<AbstractJobStage> getNextJobStage( std :: vector<Handle<SetIdentifier>> & interGlobalSets );

    

private:

    //input computations
    Handle<Vector<Handle<Computation>>> computations;

    //input tcap string
    std :: string tcapString;

    //compute plan generated from input computations and the input tcap string
    Handle<ComputePlan> computePlan;

    //logical plan generated from the compute plan
    LogicalPlanPtr logicalPlan;

    //the computation graph generated from the logical plan
    AtomicComputationList computationGraph;

    //the current source nodes
    std :: vector <AtomicComputationPtr> curSources;

    //the logger
    PDBLoggerPtr logger;

    //the jobId for this query
    std :: string jobId;




};





}





#endif
