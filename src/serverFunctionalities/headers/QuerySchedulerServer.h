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
#ifndef QUERY_SCHEDULER_SERVER_H
#define QUERY_SCHEDULER_SERVER_H


#include "ServerFunctionality.h"
#include "ResourceInfo.h"
#include "StandardResourceInfo.h"
#include "Handle.h"
#include "PDBVector.h"
#include "QueryBase.h"
#include "ResourceInfo.h"
#include "JobStage.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "PDBLogger.h"
#include "QueryGraphIr.h"
#include "QueriesAndPlan.h"
#include <vector>

namespace pdb {

class QuerySchedulerServer : public ServerFunctionality {

public:

       //destructor
       ~QuerySchedulerServer ();

       //constructor for the case when query scheduler is co-located with resource manager       
       QuerySchedulerServer(PDBLoggerPtr logger, bool pseudoClusterMode = false);

       //constructor for the case when query scheduler and resource manager are in two different nodes
       QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger, bool usePipelineNetwork = false);

       //initialization
       void initialize(bool isRMRunAsServer);

       //to transform optimized client query into a physical plan
       //each pipeline can have more than one output
       void parseOptimizedQuery(pdb_detail::QueryGraphIrPtr queryGraph);

       //to print parsed physical execution plan
       void printCurrentPlan();

       //to schedule the current job plan
       bool schedule(std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode);

       //to schedule a job stage
       bool schedule(Handle<JobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode);

       //to schedule the current job plan on all available resources
       void schedule();

       //from the serverFunctionality interface... register the resource manager handlers
       void registerHandlers (PDBServer &forMe) override;

       void cleanup () override;       

       // For the new query execution stuff
       void setQueryAndPlan(Handle<QueriesAndPlan> setToMe) {newQueriesAndPlan = setToMe;}

       void scheduleNew();

       bool scheduleNew(std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode);

protected:

       //current resources (deprecated, and we should use standardResources in our code)
       //Handle<Vector<Handle<ResourceInfo>>> resources;

       //current resources
       std :: vector<StandardResourceInfoPtr> * standardResources;

       // resource manager IP address
       std :: string resourceManagerIp;

       // port number
       int port;


       // physical plan that is temporary, however each query scheduler can schedule one JobStage at each time, similar with Spark/Hadoop
       std :: vector<Handle<JobStage>> currentPlan;

       // logger
       PDBLoggerPtr logger;

       bool usePipelineNetwork;

       Handle<QueriesAndPlan> newQueriesAndPlan;

       bool pseudoClusterMode; 

};


}



#endif
