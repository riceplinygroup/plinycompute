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
#include "Handle.h"
#include "PDBVector.h"
#include "QueryBase.h"
#include "ResourceInfo.h"
#include "JobStage.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "PDBLogger.h"
#include "QueryGraphIr.h"
#include <vector>

namespace pdb {

class QuerySchedulerServer : public ServerFunctionality {

public:

       //destructor
       ~QuerySchedulerServer ();

       //constructor, initialize from catalog
       QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger, bool usePipelineNetwork = false);

       //to transform optimized client query into a physical plan
       //each pipeline can have more than one output
       void parseOptimizedQuery(pdb_detail::QueryGraphIr queryGraph);

       //from the serverFunctionality interface... register the resource manager handlers
       void registerHandlers (PDBServer &forMe) override;

       void cleanup () override;       


protected:

       //current resources
       Handle<Vector<Handle<ResourceInfo>>> resources;

       // resource manager IP address
       std :: string resourceManagerIp;

       // port number
       int port;


       // physical plan
       std :: vector<Handle<JobStage>> currentPlan;

       // logger
       PDBLoggerPtr logger;


       bool usePipelineNetwork;

};


}



#endif
